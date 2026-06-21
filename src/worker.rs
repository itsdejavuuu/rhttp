use bytes::Bytes;
use gmod::lua::{LuaReference, State};
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub const MAX_IN_FLIGHT_REQUESTS: u64 = 1024;

pub enum CallbackTask {
    Success(LuaReference, u16, Bytes, HeaderMap),
    Failed(LuaReference, String),
    DropRef(LuaReference),
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
    requests: Mutex<HashMap<u64, CancellationToken>>,
}

impl RequestRegistry {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            requests: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(self: &Arc<Self>) -> RegisteredRequest {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = CancellationToken::new();
        if let Ok(mut requests) = self.requests.lock() {
            requests.insert(id, token.clone());
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
            .and_then(|requests| requests.get(&id).cloned());
        if let Some(token) = token {
            token.cancel();
            true
        } else {
            false
        }
    }

    fn cancel_all(&self) {
        let tokens = self
            .requests
            .lock()
            .map(|mut requests| requests.drain().map(|(_, token)| token).collect::<Vec<_>>())
            .unwrap_or_default();
        for token in tokens {
            token.cancel();
        }
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

struct WorkerState {
    callback_tx: Option<tokio::sync::mpsc::Sender<CallbackTask>>,
    callback_rx: Option<tokio::sync::mpsc::Receiver<CallbackTask>>,
    runtime: Option<tokio::runtime::Runtime>,
    client: Option<reqwest::Client>,
    concurrency_limit: Option<Arc<Semaphore>>,
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
            CallbackTask::Success(cb, status, body, headers) => {
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

    let runtime = WORKER.get().and_then(|worker| {
        let mut state = worker.lock().ok()?;
        if let Some(requests) = state.requests.take() {
            requests.cancel_all();
        }
        state.callback_tx = None;
        state.callback_rx = None;
        state.client = None;
        state.concurrency_limit = None;
        state.stats = None;
        state.runtime.take()
    });

    if let Some(rt) = runtime {
        rt.shutdown_timeout(Duration::from_millis(250));
    }
}

#[cfg(test)]
mod tests {
    use super::{HttpStats, RequestRegistry};
    use std::sync::Arc;

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
        let request = registry.register();
        assert!(registry.cancel(request.id));
        assert!(request.token.is_cancelled());
        drop(request);
        assert!(!registry.cancel(1));
    }
}
