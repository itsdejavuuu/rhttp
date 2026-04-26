# gmsv_rhttp 

async HTTP module for Garry’s Mod written in rust

## features

- fully asynchronous HTTP requests (non-blocking)
- connection pooling
- configurable concurrency limit (default: 256)
- request body size limit: 20 MB
- TLS via `rustls` (no OpenSSL dependency)

## build

requires nightly rusttoolchain

```bash
cargo +nightly build --release
```

drop target/release/gmsv_rhttp.dll into garrysmod/lua/bin/

## usage

```lua
rhttp({
    url = "https://example.com/api",
    method = "POST",
    headers = {
        ["Content-Type"] = "application/json"
    },
    body = "{\"key\":\"value\"}",
    timeout = 30,
    success = function(status, body)
        print("W", status)
    end,
    failed = function(err)
        print("L", err)
    end
})
```

## notes

- requires nightly rust
