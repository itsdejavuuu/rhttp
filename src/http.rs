use gmod::lua::State;
use std::collections::HashMap;
use std::time::Duration;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::Url;
use futures_util::StreamExt;
use crate::worker::{spawn_task, get_tx, get_client, CallbackTask, CONCURRENCY_LIMIT};

const MAX_BODY_SIZE: usize = 20 * 1024 * 1024;

unsafe fn parse_string_table(lua: State, index: i32) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let abs_index = if index < 0 { lua.get_top() + index + 1 } else { index };

    lua.push_nil();
    while lua.next(abs_index) != 0 {
        lua.push_value(-2);
        let k = if lua.lua_type(-1) == gmod::lua::LUA_TSTRING { lua.get_string(-1).map(|s| s.into_owned()) } else { None };
        lua.pop_n(1);

        lua.push_value(-1);
        let v = if lua.lua_type(-1) == gmod::lua::LUA_TSTRING || lua.lua_type(-1) == gmod::lua::LUA_TNUMBER {
            lua.get_string(-1).map(|s| s.into_owned())
        } else if lua.is_boolean(-1) {
            Some(if lua.get_boolean(-1) { "true".to_string() } else { "false".to_string() })
        } else {
            None
        };
        lua.pop_n(1);

        if let (Some(key), Some(val)) = (k, v) { map.insert(key, val); }
        lua.pop_n(1);
    }
    map
}

pub unsafe extern "C-unwind" fn request_lua(lua: State) -> i32 {
    if lua.lua_type(1) != gmod::lua::LUA_TTABLE {
        lua.error("rhttp: Expected table as first argument");
    }

    lua.get_field(1, lua_string!("url"));
    let url_str = lua.get_string(-1).map(|s| s.into_owned()).unwrap_or_default();
    lua.pop_n(1);

    lua.get_field(1, lua_string!("success"));
    let success_cb = if lua.is_function(-1) { Some(lua.reference()) } else { lua.pop_n(1); None };

    lua.get_field(1, lua_string!("failed"));
    let failed_cb = if lua.is_function(-1) { Some(lua.reference()) } else { lua.pop_n(1); None };

    let parsed_url = match Url::parse(&url_str) {
        Ok(u) => u,
        Err(e) => {
            if let Some(cb) = failed_cb {
                lua.from_reference(cb);
                lua.push_string(&format!("Invalid URL: {}", e));
                lua.call(1, 0);
                lua.dereference(cb);
            }
            if let Some(cb) = success_cb { lua.dereference(cb); }
            return 0;
        }
    };

    lua.get_field(1, lua_string!("method"));
    let method_str = lua.get_string(-1).map(|s| s.into_owned()).unwrap_or_else(|| "GET".to_string());
    lua.pop_n(1);
    let method = reqwest::Method::from_bytes(method_str.to_uppercase().as_bytes())
        .unwrap_or(reqwest::Method::GET);

    lua.get_field(1, lua_string!("headers"));
    let headers = if lua.lua_type(-1) == gmod::lua::LUA_TTABLE { Some(parse_string_table(lua, -1)) } else { None };
    lua.pop_n(1);

    lua.get_field(1, lua_string!("body"));
    let body = lua.get_binary_string(-1).map(|b| b.to_vec());
    lua.pop_n(1);

    lua.get_field(1, lua_string!("timeout"));
    let timeout = if lua.lua_type(-1) == gmod::lua::LUA_TNUMBER { Some(Duration::from_secs(lua.to_integer(-1).max(1) as u64)) } else { None };
    lua.pop_n(1);

    let has_callbacks = success_cb.is_some() || failed_cb.is_some();
    let tx = if has_callbacks { get_tx() } else { None };

    spawn_task(async move {
        let permit = if let Some(sem) = CONCURRENCY_LIMIT.get() {
            sem.acquire().await.ok()
        } else {
            None
        };

        let client = get_client();
        let mut request = client.request(method, parsed_url);

        if let Some(t) = timeout { request = request.timeout(t); }
        if let Some(h) = headers {
            for (k, v) in h {
                if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(&v)) {
                    request = request.header(name, value);
                }
            }
        }
        if let Some(b) = body { request = request.body(b); }

        if !has_callbacks {
            let _ = request.send().await;
            drop(permit);
            return;
        }

        let tx = tx.unwrap();

        match request.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();

                let mut body_bytes = Vec::new();
                let mut stream = resp.bytes_stream();
                let mut memory_limit_exceeded = false;

                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            if body_bytes.len() + chunk.len() > MAX_BODY_SIZE {
                                memory_limit_exceeded = true;
                                break;
                            }
                            body_bytes.extend_from_slice(&chunk);
                        }
                        Err(e) => {
                            if let Some(fcb) = failed_cb {
                                let _ = tx.try_send(CallbackTask::Failed(fcb, format!("Stream error: {}", e)));
                            }
                            if let Some(scb) = success_cb { let _ = tx.try_send(CallbackTask::DropRef(scb)); }
                            drop(permit);
                            return;
                        }
                    }
                }

                if memory_limit_exceeded {
                    if let Some(fcb) = failed_cb { let _ = tx.try_send(CallbackTask::Failed(fcb, "Response body exceeded memory limit".to_string())); }
                    if let Some(scb) = success_cb { let _ = tx.try_send(CallbackTask::DropRef(scb)); }
                    drop(permit);
                    return;
                }

                if let Some(cb) = success_cb {
                    let _ = tx.try_send(CallbackTask::Success(cb, status, body_bytes.into()));
                }
                if let Some(cb) = failed_cb { let _ = tx.try_send(CallbackTask::DropRef(cb)); }
            }
            Err(e) => {
                if let Some(cb) = failed_cb {
                    let _ = tx.try_send(CallbackTask::Failed(cb, e.to_string()));
                }
                if let Some(cb) = success_cb { let _ = tx.try_send(CallbackTask::DropRef(cb)); }
            }
        }

        drop(permit);
    });

    0
}
