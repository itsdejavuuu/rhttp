use bytes::Bytes;
use gmod::lua::{LuaReference, State};
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub const MAX_IN_FLIGHT_REQUESTS: u64 = 1024;
pub const MAX_BUFFERED_BODY_BYTES: u32 = 64 * 1024 * 1024;

pub enum CallbackTask {
    Success(LuaReference, u16, Bytes, HeaderMap, BodyBudget),
    Failed(LuaReference, String),
    DropRef(LuaReference),
}

pub struct BodyBudget {
    _permits: Vec<OwnedSemaphorePermit>,
}

impl BodyBudget {
    pub fn new() -> Self {
        Self {
            _permits: Vec::new(),
        }
    }

    pub fn reserve(&mut self, permit: OwnedSemaphorePermit) {
        self._permits.push(permit);
    }
}

pub struct RequestCallbacks {
    pub success: Option<LuaReference>,
    pub failed: Option<LuaReference>,
}

pub struct HttpStats {
    submitted: AtomicU64,
    in_flight: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    cancelled: AtomicU64,
}

#[derive(Default)]
pub struct StatsSnapshot {
    pub submitted: u64,
    pub in_flight: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub retried: u64,
    pub cancelled: u64,
}

impl HttpStats {
    fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            retried: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
        }
    }

    pub fn try_request_started(self: &Arc<Self>) -> Option<InFlightRequest> {
        let mut in_flight = self.in_flight.load(Ordering::Relaxed);
        loop {
            if in_flight >= MAX_IN_FLIGHT_REQUESTS {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                in_flight,
                in_flight + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => in_flight = current,
            }
        }
        self.submitted.fetch_add(1, Ordering::Relaxed);
        Some(InFlightRequest {
            stats: self.clone(),
        })
    }

    pub fn succeeded(&self) {
        self.succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cancelled(&self) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
        }
    }
}

pub struct InFlightRequest {
    stats: Arc<HttpStats>,
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct RequestRegistry {
    next_id: AtomicU64,
    requests: Mutex<HashMap<u64, RequestEntry>>,
}

struct RequestEntry {
    token: CancellationToken,
    callbacks: Option<RequestCallbacks>,
}

impl RequestRegistry {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            requests: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(
        self: &Arc<Self>,
        success: Option<LuaReference>,
        failed: Option<LuaReference>,
    ) -> RegisteredRequest {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = CancellationToken::new();
        if let Ok(mut requests) = self.requests.lock() {
            requests.insert(
                id,
                RequestEntry {
                    token: token.clone(),
                    callbacks: Some(RequestCallbacks { success, failed }),
                },
            );
        }
        RegisteredRequest {
            id,
            token,
            registry: self.clone(),
        }
    }

    pub fn finish(&self, id: u64) {
        if let Ok(mut requests) = self.requests.lock() {
            requests.remove(&id);
        }
    }

    pub fn cancel(&self, id: u64) -> bool {
        let token = self
            .requests
            .lock()
            .ok()
            .and_then(|requests| requests.get(&id).map(|entry| entry.token.clone()));
        if let Some(token) = token {
            token.cancel();
            true
        } else {
            false
        }
    }

    pub fn take_callbacks(&self, id: u64) -> Option<RequestCallbacks> {
        self.requests
            .lock()
            .ok()
            .and_then(|mut requests| requests.get_mut(&id)?.callbacks.take())
    }

    fn cancel_all(&self) -> Vec<RequestCallbacks> {
        let entries = self
            .requests
            .lock()
            .map(|mut requests| requests.drain().map(|(_, entry)| entry).collect::<Vec<_>>())
            .unwrap_or_default();
        let mut callbacks = Vec::with_capacity(entries.len());
        for mut entry in entries {
            entry.token.cancel();
            if let Some(callbacks_for_request) = entry.callbacks.take() {
                callbacks.push(callbacks_for_request);
            }
        }
        callbacks
    }
}

pub struct RegisteredRequest {
    pub id: u64,
    pub token: CancellationToken,
    registry: Arc<RequestRegistry>,
}

impl Drop for RegisteredRequest {
    fn drop(&mut self) {
        self.registry.finish(self.id);
    }
}

impl RegisteredRequest {
    pub fn take_callbacks(&self) -> Option<RequestCallbacks> {
        self.registry.take_callbacks(self.id)
    }
}

struct WorkerState {
    callback_tx: Option<tokio::sync::mpsc::Sender<CallbackTask>>,
    callback_rx: Option<tokio::sync::mpsc::Receiver<CallbackTask>>,
    runtime: Option<tokio::runtime::Runtime>,
    client: Option<reqwest::Client>,
    concurrency_limit: Option<Arc<Semaphore>>,
    body_budget: Option<Arc<Semaphore>>,
    stats: Option<Arc<HttpStats>>,
    requests: Option<Arc<RequestRegistry>>,
}

impl WorkerState {
    const fn new() -> Self {
        Self {
            callback_tx: None,
            callback_rx: None,
            runtime: None,
            client: None,
            concurrency_limit: None,
            body_budget: None,
            stats: None,
            requests: None,
        }
    }
}

static WORKER: OnceLock<Mutex<WorkerState>> = OnceLock::new();

pub struct WorkerResources {
    pub callback_tx: tokio::sync::mpsc::Sender<CallbackTask>,
    pub client: reqwest::Client,
    pub concurrency_limit: Arc<Semaphore>,
    pub body_budget: Arc<Semaphore>,
    pub stats: Arc<HttpStats>,
    pub requests: Arc<RequestRegistry>,
    handle: tokio::runtime::Handle,
}

impl WorkerResources {
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

pub fn init(lua: State) {
    let worker = WORKER.get_or_init(|| Mutex::new(WorkerState::new()));
    let mut state = worker.lock().unwrap();

    if state.runtime.is_none() {
        let (callback_tx, callback_rx) = tokio::sync::mpsc::channel(4096);
        let client = reqwest::Client::builder()
            .user_agent("gmsv-rhttp/1.1")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::limited(10))
            .tcp_keepalive(Duration::from_secs(60))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .expect("Failed to build reqwest client");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime");

        state.callback_tx = Some(callback_tx);
        state.callback_rx = Some(callback_rx);
        state.client = Some(client);
        state.concurrency_limit = Some(Arc::new(Semaphore::new(256)));
        state.body_budget = Some(Arc::new(Semaphore::new(MAX_BUFFERED_BODY_BYTES as usize)));
        state.stats = Some(Arc::new(HttpStats::new()));
        state.requests = Some(Arc::new(RequestRegistry::new()));
        state.runtime = Some(runtime);
    }
    drop(state);

    unsafe {
        lua.get_global(lua_string!("hook"));
        lua.get_field(-1, lua_string!("Add"));
        lua.push_string("Think");
        lua.push_string("FetchRsW");
        lua.push_function(think);
        lua.call(3, 0);
        lua.pop();
    }
}

pub fn resources() -> Option<WorkerResources> {
    let worker = WORKER.get()?;
    let state = worker.lock().ok()?;

    Some(WorkerResources {
        callback_tx: state.callback_tx.clone()?,
        client: state.client.clone()?,
        concurrency_limit: state.concurrency_limit.clone()?,
        body_budget: state.body_budget.clone()?,
        stats: state.stats.clone()?,
        requests: state.requests.clone()?,
        handle: state.runtime.as_ref()?.handle().clone(),
    })
}

pub fn stats() -> StatsSnapshot {
    WORKER
        .get()
        .and_then(|worker| {
            worker
                .lock()
                .ok()
                .and_then(|state| state.stats.as_ref().map(|stats| stats.snapshot()))
        })
        .unwrap_or_default()
}

pub fn cancel_request(id: u64) -> bool {
    WORKER
        .get()
        .and_then(|worker| worker.lock().ok().and_then(|state| state.requests.clone()))
        .is_some_and(|requests| requests.cancel(id))
}

pub fn spawn_task<F: std::future::Future<Output = ()> + Send + 'static>(
    handle: tokio::runtime::Handle,
    future: F,
) {
    handle.spawn(future);
}

unsafe extern "C-unwind" fn think(lua: State) -> i32 {
    let start = Instant::now();
    let time_budget = Duration::from_millis(2);

    loop {
        let task = WORKER.get().and_then(|worker| {
            let mut state = worker.try_lock().ok()?;
            state.callback_rx.as_mut()?.try_recv().ok()
        });

        let Some(task) = task else {
            break;
        };

        match task {
            CallbackTask::Success(cb, status, body, headers, _body_budget) => {
                lua.from_reference(cb);
                lua.push_integer(status as _);
                lua.push_binary_string(&body);
                lua.create_table(0, headers.len() as _);
                for (name, value) in headers.iter() {
                    lua.push_string(name.as_str());
                    lua.push_binary_string(value.as_bytes());
                    lua.set_table(-3);
                }
                lua.pcall_ignore(3, 0);
                lua.dereference(cb);
            }
            CallbackTask::Failed(cb, err) => {
                lua.from_reference(cb);
                lua.push_string(&err);
                lua.pcall_ignore(1, 0);
                lua.dereference(cb);
            }
            CallbackTask::DropRef(cb) => {
                lua.dereference(cb);
            }
        }

        if start.elapsed() >= time_budget {
            break;
        }
    }
    0
}

pub fn shutdown(lua: State) {
    unsafe {
        lua.get_global(lua_string!("hook"));
        lua.get_field(-1, lua_string!("Remove"));
        lua.push_string("Think");
        lua.push_string("FetchRsW");
        lua.call(2, 0);
        lua.pop();
    }

    let Some((runtime, callback_rx, callbacks)) = WORKER.get().and_then(|worker| {
        let mut state = worker.lock().ok()?;
        let callbacks = state
            .requests
            .take()
            .map(|requests| requests.cancel_all())
            .unwrap_or_default();
        state.callback_tx = None;
        let callback_rx = state.callback_rx.take();
        state.client = None;
        state.concurrency_limit = None;
        state.body_budget = None;
        state.stats = None;
        Some((state.runtime.take(), callback_rx, callbacks))
    }) else {
        return;
    };

    if let Some(rt) = runtime {
        rt.shutdown_timeout(Duration::from_millis(250));
    }

    if let Some(mut rx) = callback_rx {
        while let Ok(task) = rx.try_recv() {
            unsafe { discard_callback_task(lua, task) };
        }
    }
    for callbacks_for_request in callbacks {
        unsafe { discard_callbacks(lua, callbacks_for_request) };
    }
}

unsafe fn discard_callback_task(lua: State, task: CallbackTask) {
    match task {
        CallbackTask::Success(cb, _, _, _, _)
        | CallbackTask::Failed(cb, _)
        | CallbackTask::DropRef(cb) => lua.dereference(cb),
    }
}

unsafe fn discard_callbacks(lua: State, callbacks: RequestCallbacks) {
    if let Some(cb) = callbacks.success {
        lua.dereference(cb);
    }
    if let Some(cb) = callbacks.failed {
        lua.dereference(cb);
    }
}

#[cfg(test)]
mod tests {
    use super::{BodyBudget, HttpStats, RequestRegistry};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn request_lifecycle_updates_stats() {
        let stats = Arc::new(HttpStats::new());
        let request = stats
            .try_request_started()
            .expect("request should be admitted");
        assert_eq!(stats.snapshot().submitted, 1);
        assert_eq!(stats.snapshot().in_flight, 1);
        drop(request);
        assert_eq!(stats.snapshot().in_flight, 0);
    }

    #[test]
    fn cancelled_request_is_removed_after_completion() {
        let registry = Arc::new(RequestRegistry::new());
        let request = registry.register(None, None);
        assert!(registry.cancel(request.id));
        assert!(request.token.is_cancelled());
        drop(request);
        assert!(!registry.cancel(1));
    }

    #[test]
    fn shutdown_collects_callback_references() {
        let registry = Arc::new(RequestRegistry::new());
        let request = registry.register(Some(10), Some(11));

        let callbacks = registry.cancel_all();
        assert_eq!(callbacks.len(), 1);
        assert_eq!(callbacks[0].success, Some(10));
        assert_eq!(callbacks[0].failed, Some(11));
        assert!(request.token.is_cancelled());
    }

    #[test]
    fn body_budget_keeps_bytes_reserved_until_callback_is_dropped() {
        let budget = Arc::new(Semaphore::new(8));
        let permit = budget
            .clone()
            .try_acquire_many_owned(8)
            .expect("budget should accept the body");
        let mut body_budget = BodyBudget::new();
        body_budget.reserve(permit);
        assert_eq!(budget.available_permits(), 0);
        drop(body_budget);
        assert_eq!(budget.available_permits(), 8);
    }
}
