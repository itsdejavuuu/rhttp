use crate::worker::{resources, spawn_task, CallbackTask, MAX_IN_FLIGHT_REQUESTS};
use futures_util::StreamExt;
use gmod::lua::State;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Url;
use std::collections::HashMap;
use std::time::Duration;

const MAX_BODY_SIZE: usize = 20 * 1024 * 1024;
const MAX_RETRIES: usize = 5;

fn default_retries(method: &reqwest::Method) -> usize {
    match *method {
        reqwest::Method::GET
        | reqwest::Method::HEAD
        | reqwest::Method::PUT
        | reqwest::Method::DELETE
        | reqwest::Method::OPTIONS => 2,
        _ => 0,
    }
}

fn retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

fn retry_delay(headers: Option<&HeaderMap>, attempt: usize, base: Duration) -> Duration {
    if let Some(seconds) = headers
        .and_then(|headers| headers.get(reqwest::header::RETRY_AFTER))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    {
        return Duration::from_secs(seconds.min(30));
    }

    base.checked_mul(1_u32 << attempt.min(6))
        .unwrap_or(Duration::from_secs(30))
        .min(Duration::from_secs(30))
}

async fn report_failure(
    tx: &tokio::sync::mpsc::Sender<CallbackTask>,
    success_cb: Option<gmod::lua::LuaReference>,
    failed_cb: Option<gmod::lua::LuaReference>,
    message: String,
) {
    if let Some(cb) = failed_cb {
        let _ = tx.send(CallbackTask::Failed(cb, message)).await;
    }
    if let Some(cb) = success_cb {
        let _ = tx.send(CallbackTask::DropRef(cb)).await;
    }
}

unsafe fn fail_request(
    lua: State,
    success_cb: Option<gmod::lua::LuaReference>,
    failed_cb: Option<gmod::lua::LuaReference>,
    message: &str,
) {
    if let Some(cb) = failed_cb {
        lua.from_reference(cb);
        lua.push_string(message);
        lua.pcall_ignore(1, 0);
        lua.dereference(cb);
    }
    if let Some(cb) = success_cb {
        lua.dereference(cb);
    }
    lua.push_boolean(false);
}

unsafe fn parse_string_table(lua: State, index: i32) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let abs_index = if index < 0 {
        lua.get_top() + index + 1
    } else {
        index
    };

    lua.push_nil();
    while lua.next(abs_index) != 0 {
        lua.push_value(-2);
        let k = if lua.lua_type(-1) == gmod::lua::LUA_TSTRING {
            lua.get_string(-1).map(|s| s.into_owned())
        } else {
            None
        };
        lua.pop_n(1);

        lua.push_value(-1);
        let v = if lua.lua_type(-1) == gmod::lua::LUA_TSTRING
            || lua.lua_type(-1) == gmod::lua::LUA_TNUMBER
        {
            lua.get_string(-1).map(|s| s.into_owned())
        } else if lua.is_boolean(-1) {
            Some(if lua.get_boolean(-1) {
                "true".to_string()
            } else {
                "false".to_string()
            })
        } else {
            None
        };
        lua.pop_n(1);

        if let (Some(key), Some(val)) = (k, v) {
            map.insert(key, val);
        }
        lua.pop_n(1);
    }
    map
}

pub unsafe extern "C-unwind" fn request_lua(lua: State) -> i32 {
    if lua.lua_type(1) != gmod::lua::LUA_TTABLE {
        lua.error("rhttp: Expected table as first argument");
    }

    lua.get_field(1, lua_string!("url"));
    let url_str = lua
        .get_string(-1)
        .map(|s| s.into_owned())
        .unwrap_or_default();
    lua.pop_n(1);

    lua.get_field(1, lua_string!("success"));
    let success_cb = if lua.is_function(-1) {
        Some(lua.reference())
    } else {
        lua.pop_n(1);
        None
    };

    lua.get_field(1, lua_string!("failed"));
    let failed_cb = if lua.is_function(-1) {
        Some(lua.reference())
    } else {
        lua.pop_n(1);
        None
    };

    let mut parsed_url = match Url::parse(&url_str) {
        Ok(u) => u,
        Err(e) => {
            fail_request(lua, success_cb, failed_cb, &format!("Invalid URL: {}", e));
            return 1;
        }
    };

    lua.get_field(1, lua_string!("method"));
    let method_str = lua
        .get_string(-1)
        .map(|s| s.into_owned())
        .unwrap_or_else(|| "GET".to_string());
    lua.pop_n(1);
    let method = match reqwest::Method::from_bytes(method_str.to_uppercase().as_bytes()) {
        Ok(method) => method,
        Err(e) => {
            fail_request(
                lua,
                success_cb,
                failed_cb,
                &format!("Invalid HTTP method: {}", e),
            );
            return 1;
        }
    };

    lua.get_field(1, lua_string!("headers"));
    let headers = if lua.lua_type(-1) == gmod::lua::LUA_TTABLE {
        Some(parse_string_table(lua, -1))
    } else {
        None
    };
    lua.pop_n(1);

    let mut request_headers = HeaderMap::new();
    if let Some(headers) = headers {
        for (key, value) in headers {
            let name = match HeaderName::from_bytes(key.as_bytes()) {
                Ok(name) => name,
                Err(e) => {
                    fail_request(
                        lua,
                        success_cb,
                        failed_cb,
                        &format!("Invalid header name: {}", e),
                    );
                    return 1;
                }
            };
            let value = match HeaderValue::from_str(&value) {
                Ok(value) => value,
                Err(e) => {
                    fail_request(
                        lua,
                        success_cb,
                        failed_cb,
                        &format!("Invalid header value: {}", e),
                    );
                    return 1;
                }
            };
            request_headers.insert(name, value);
        }
    }

    lua.get_field(1, lua_string!("parameters"));
    let parameters = if lua.lua_type(-1) == gmod::lua::LUA_TTABLE {
        Some(parse_string_table(lua, -1))
    } else {
        None
    };
    lua.pop_n(1);

    lua.get_field(1, lua_string!("body"));
    let mut body = lua.get_binary_string(-1).map(|b| b.to_vec());
    lua.pop_n(1);

    lua.get_field(1, lua_string!("type"));
    let body_type = if lua.lua_type(-1) == gmod::lua::LUA_TSTRING {
        lua.get_string(-1).map(|value| value.into_owned())
    } else {
        None
    };
    lua.pop_n(1);

    let mut generated_body_type = None;
    if let Some(parameters) = parameters {
        if method == reqwest::Method::POST {
            if body.is_none() {
                body = match serde_urlencoded::to_string(parameters) {
                    Ok(body) => Some(body.into_bytes()),
                    Err(e) => {
                        fail_request(
                            lua,
                            success_cb,
                            failed_cb,
                            &format!("Failed to encode parameters: {}", e),
                        );
                        return 1;
                    }
                };
                generated_body_type = Some("application/x-www-form-urlencoded".to_string());
            }
        } else {
            let mut query = parsed_url.query_pairs_mut();
            for (key, value) in parameters {
                query.append_pair(&key, &value);
            }
        }
    }

    if body.as_ref().is_some_and(|body| body.len() > MAX_BODY_SIZE) {
        fail_request(
            lua,
            success_cb,
            failed_cb,
            "Request body exceeded memory limit",
        );
        return 1;
    }

    if body.is_some() && !request_headers.contains_key(reqwest::header::CONTENT_TYPE) {
        let content_type = body_type
            .or(generated_body_type)
            .unwrap_or_else(|| "text/plain; charset=utf-8".to_string());
        let content_type = match HeaderValue::from_str(&content_type) {
            Ok(value) => value,
            Err(e) => {
                fail_request(
                    lua,
                    success_cb,
                    failed_cb,
                    &format!("Invalid content type: {}", e),
                );
                return 1;
            }
        };
        request_headers.insert(reqwest::header::CONTENT_TYPE, content_type);
    }

    lua.get_field(1, lua_string!("timeout"));
    let timeout = if lua.lua_type(-1) == gmod::lua::LUA_TNUMBER {
        Some(Duration::from_secs(lua.to_integer(-1).max(1) as u64))
    } else {
        None
    };
    lua.pop_n(1);

    lua.get_field(1, lua_string!("retries"));
    let retries = if lua.lua_type(-1) == gmod::lua::LUA_TNUMBER {
        lua.to_integer(-1).max(0) as usize
    } else {
        default_retries(&method)
    }
    .min(MAX_RETRIES);
    lua.pop_n(1);

    lua.get_field(1, lua_string!("retry_delay"));
    let retry_base_delay = if lua.lua_type(-1) == gmod::lua::LUA_TNUMBER {
        let seconds = lua.to_number(-1);
        if seconds.is_finite() {
            Duration::from_secs_f64(seconds.clamp(0.05, 30.0))
        } else {
            Duration::from_millis(250)
        }
    } else {
        Duration::from_millis(250)
    };
    lua.pop_n(1);

    let Some(worker) = resources() else {
        fail_request(lua, success_cb, failed_cb, "rhttp is not initialized");
        return 1;
    };
    let Some(in_flight) = worker.stats.try_request_started() else {
        fail_request(
            lua,
            success_cb,
            failed_cb,
            &format!(
                "rhttp request queue is full (limit: {})",
                MAX_IN_FLIGHT_REQUESTS
            ),
        );
        return 1;
    };
    let registered_request = worker.requests.register();
    let request_id = registered_request.id;

    let handle = worker.handle();
    spawn_task(handle, async move {
        let _registered_request = registered_request;
        let _in_flight = in_flight;
        let token = _registered_request.token.clone();
        let tx = worker.callback_tx;
        let mut attempt = 0;

        loop {
            if token.is_cancelled() {
                worker.stats.cancelled();
                report_failure(&tx, success_cb, failed_cb, "Request cancelled".to_string()).await;
                return;
            }

            let permit = tokio::select! {
                _ = token.cancelled() => {
                    worker.stats.cancelled();
                    report_failure(&tx, success_cb, failed_cb, "Request cancelled".to_string()).await;
                    return;
                }
                permit = worker.concurrency_limit.clone().acquire_owned() => permit.ok(),
            };

            let mut request = worker.client.request(method.clone(), parsed_url.clone());
            if let Some(t) = timeout {
                request = request.timeout(t);
            }
            request = request.headers(request_headers.clone());
            if let Some(body) = body.clone() {
                request = request.body(body);
            }

            let response = tokio::select! {
                _ = token.cancelled() => {
                    drop(permit);
                    worker.stats.cancelled();
                    report_failure(&tx, success_cb, failed_cb, "Request cancelled".to_string()).await;
                    return;
                }
                response = request.send() => response,
            };

            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    if attempt < retries && (error.is_connect() || error.is_timeout()) {
                        worker.stats.retried();
                        drop(permit);
                        let delay = retry_delay(None, attempt, retry_base_delay);
                        attempt += 1;
                        tokio::select! {
                            _ = token.cancelled() => {
                                worker.stats.cancelled();
                                report_failure(&tx, success_cb, failed_cb, "Request cancelled".to_string()).await;
                                return;
                            }
                            _ = tokio::time::sleep(delay) => continue,
                        }
                    }

                    worker.stats.failed();
                    drop(permit);
                    report_failure(&tx, success_cb, failed_cb, error.to_string()).await;
                    return;
                }
            };

            if attempt < retries && retryable_status(response.status()) {
                let delay = retry_delay(Some(response.headers()), attempt, retry_base_delay);
                worker.stats.retried();
                drop(response);
                drop(permit);
                attempt += 1;
                tokio::select! {
                    _ = token.cancelled() => {
                        worker.stats.cancelled();
                        report_failure(&tx, success_cb, failed_cb, "Request cancelled".to_string()).await;
                        return;
                    }
                    _ = tokio::time::sleep(delay) => continue,
                }
            }

            let status = response.status().as_u16();
            let response_headers = response.headers().clone();
            let mut body_bytes = Vec::new();
            let mut response_size = 0;
            let collect_body = success_cb.is_some();
            let mut stream = response.bytes_stream();

            while let Some(chunk_result) = tokio::select! {
                _ = token.cancelled() => {
                    drop(permit);
                    worker.stats.cancelled();
                    report_failure(&tx, success_cb, failed_cb, "Request cancelled".to_string()).await;
                    return;
                }
                chunk = stream.next() => chunk,
            } {
                match chunk_result {
                    Ok(chunk) => {
                        response_size += chunk.len();
                        if response_size > MAX_BODY_SIZE {
                            worker.stats.failed();
                            drop(permit);
                            report_failure(
                                &tx,
                                success_cb,
                                failed_cb,
                                "Response body exceeded memory limit".to_string(),
                            )
                            .await;
                            return;
                        }
                        if collect_body {
                            body_bytes.extend_from_slice(&chunk);
                        }
                    }
                    Err(error) => {
                        worker.stats.failed();
                        drop(permit);
                        report_failure(
                            &tx,
                            success_cb,
                            failed_cb,
                            format!("Stream error: {}", error),
                        )
                        .await;
                        return;
                    }
                }
            }

            worker.stats.succeeded();
            drop(permit);
            if let Some(cb) = success_cb {
                let _ = tx
                    .send(CallbackTask::Success(
                        cb,
                        status,
                        body_bytes.into(),
                        response_headers,
                    ))
                    .await;
            }
            if let Some(cb) = failed_cb {
                let _ = tx.send(CallbackTask::DropRef(cb)).await;
            }
            return;
        }
    });

    lua.push_boolean(true);
    lua.push_integer(request_id as _);
    2
}

pub unsafe extern "C-unwind" fn cancel_lua(lua: State) -> i32 {
    let cancelled = lua.lua_type(1) == gmod::lua::LUA_TNUMBER
        && crate::worker::cancel_request(lua.to_integer(1).max(0) as u64);
    lua.push_boolean(cancelled);
    1
}

pub unsafe extern "C-unwind" fn stats_lua(lua: State) -> i32 {
    let stats = crate::worker::stats();
    lua.create_table(0, 6);
    lua.push_integer(stats.submitted as _);
    lua.set_field(-2, lua_string!("submitted"));
    lua.push_integer(stats.in_flight as _);
    lua.set_field(-2, lua_string!("in_flight"));
    lua.push_integer(stats.succeeded as _);
    lua.set_field(-2, lua_string!("succeeded"));
    lua.push_integer(stats.failed as _);
    lua.set_field(-2, lua_string!("failed"));
    lua.push_integer(stats.retried as _);
    lua.set_field(-2, lua_string!("retried"));
    lua.push_integer(stats.cancelled as _);
    lua.set_field(-2, lua_string!("cancelled"));
    1
}

#[cfg(test)]
mod tests {
    use super::{default_retries, retryable_status};

    #[test]
    fn retries_only_idempotent_methods_by_default() {
        assert_eq!(default_retries(&reqwest::Method::GET), 2);
        assert_eq!(default_retries(&reqwest::Method::POST), 0);
    }

    #[test]
    fn retries_transient_statuses() {
        assert!(retryable_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(retryable_status(reqwest::StatusCode::SERVICE_UNAVAILABLE));
        assert!(!retryable_status(reqwest::StatusCode::BAD_REQUEST));
    }
}
