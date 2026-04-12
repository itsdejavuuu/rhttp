use std::sync::{OnceLock, Mutex, mpsc::{self, SyncSender, Receiver}};
use std::time::{Duration, Instant};
use gmod::lua::{State, LuaReference};
use tokio::sync::Semaphore;
use bytes::Bytes;

pub enum CallbackTask {
    Success(LuaReference, u16, Bytes),
    Failed(LuaReference, String),
    DropRef(LuaReference),
}

static CALLBACK_RX: OnceLock<Mutex<Receiver<CallbackTask>>> = OnceLock::new();
static CALLBACK_TX: OnceLock<SyncSender<CallbackTask>> = OnceLock::new();

static TOKIO_RT: Mutex<Option<tokio::runtime::Runtime>> = Mutex::new(None);
static TOKIO_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

pub static CONCURRENCY_LIMIT: OnceLock<Semaphore> = OnceLock::new();

pub fn init(lua: State) {
    let (tx, rx) = mpsc::sync_channel(4096);
    let _ = CALLBACK_TX.set(tx);
    let _ = CALLBACK_RX.set(Mutex::new(rx));

    let _ = CONCURRENCY_LIMIT.set(Semaphore::new(256));

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Duration::from_secs(60))
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .expect("Failed to build reqwest client");

    let _ = HTTP_CLIENT.set(client);

    if let Ok(rt) = tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        let _ = TOKIO_HANDLE.set(rt.handle().clone());
        *TOKIO_RT.lock().unwrap() = Some(rt);
    }

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

pub fn get_tx() -> Option<SyncSender<CallbackTask>> {
    CALLBACK_TX.get().cloned()
}

pub fn get_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get().expect("HTTP_CLIENT is not initialized")
}

pub fn spawn_task<F: std::future::Future<Output = ()> + Send + 'static>(f: F) {
    if let Some(handle) = TOKIO_HANDLE.get() {
        handle.spawn(f);
    }
}

unsafe extern "C-unwind" fn think(lua: State) -> i32 {
    if let Some(rx_mutex) = CALLBACK_RX.get() {
        if let Ok(rx) = rx_mutex.try_lock() {
            let start = Instant::now();
            let time_budget = Duration::from_millis(2);

            while let Ok(task) = rx.try_recv() {
                match task {
                    CallbackTask::Success(cb, status, body) => {
                        lua.from_reference(cb);
                        lua.push_integer(status as _);
                        lua.push_binary_string(&body);
                        lua.call(2, 0);
                        lua.dereference(cb);
                    }
                    CallbackTask::Failed(cb, err) => {
                        lua.from_reference(cb);
                        lua.push_string(&err);
                        lua.call(1, 0);
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

    if let Some(rt) = TOKIO_RT.lock().unwrap().take() {
        rt.shutdown_timeout(Duration::from_millis(250));
    }
}
