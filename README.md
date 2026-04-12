# gmsv_rhttp

> "another http module bc gmod's built-in one is mid"

async http client for gmod using rust (lfg)

## why?

- built-in http blocks the game tick
- no connection pooling
- this one uses tokio + reqwest

## features

- async requests that don't freeze the server
- connection pooling
- 256 concurrent request cap
- 20MB body limit
- rustls - no openssl nonsense

## build

```bash
cargo build --release
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
- tested on windows
