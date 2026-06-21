# gmsv_rhttp

Async HTTP for Garry's Mod servers. Rust + `reqwest` + `rustls`.

Use it for APIs, webhooks, auth services, telemetry, or whatever external service your server needs. It does not block Lua.

## Included

- HTTPS, connection pooling, redirects (max 10), gzip/brotli/deflate.
- Any valid HTTP method.
- Query params, form posts, JSON/binary bodies, request/response headers.
- 20 MB per request/response body; 64 MB total buffered-body budget.
- 256 concurrent network requests; 1024 requests in flight total.
- Retry/backoff, cancellation, and lightweight counters.
- Linux x86-64 and Windows x86-64.

## Usage

`rhttp(options)` returns `true, requestId` when queued, otherwise `false`.

### GET JSON

```lua
local ok, requestId = rhttp({
    url = "https://api.example.com/v1/users/42",
    headers = {
        ["Accept"] = "application/json",
        ["Authorization"] = "Bearer your-token",
    },
    timeout = 10,

    success = function(status, body, headers)
        if status ~= 200 then
            print("API returned", status)
            return
        end

        local data = util.JSONToTable(body)
        PrintTable(data)
    end,

    failed = function(reason)
        ErrorNoHalt("API request failed: " .. reason .. "\n")
    end,
})
```

Fetches JSON from any API. Check `status` yourself: HTTP `4xx` and `5xx` are valid HTTP responses, so they go to `success`.

### POST JSON

```lua
local payload = util.TableToJSON({
    event = "round_started",
    map = game.GetMap(),
})

rhttp({
    url = "https://api.example.com/v1/events",
    method = "POST",
    headers = {
        ["Content-Type"] = "application/json",
        ["Authorization"] = "Bearer your-token",
    },
    body = payload,
    timeout = 15,

    success = function(status)
        print("Event accepted, status:", status)
    end,

    failed = function(reason)
        ErrorNoHalt("Event upload failed: " .. reason .. "\n")
    end,
})
```

Sends JSON to any endpoint. Nothing webhook-specific here.

### Query params and form POST

```lua
-- GET: parameters are appended to the URL.
rhttp({
    url = "https://api.example.com/v1/search",
    parameters = { query = "gmod", page = 1 },
})

-- POST without body: parameters become application/x-www-form-urlencoded.
rhttp({
    url = "https://api.example.com/v1/login",
    method = "POST",
    parameters = { username = "player", password = "secret" },
})
```

`body` wins for POST. If you need JSON, send `body` and set `Content-Type` yourself.

## Options

| Field | Type | Default / behavior |
| --- | --- | --- |
| `url` | string | Required. |
| `method` | string | `GET`. |
| `parameters` | table | Query params, or form body for POST without `body`. |
| `headers` | table | Request headers. |
| `body` | string | Binary-safe body, max 20 MB. |
| `type` | string | Auto-generated content type when no `Content-Type` header exists. Default: `text/plain; charset=utf-8`. |
| `timeout` | number | Full request timeout in seconds. Default: 30; minimum: 1. |
| `retries` | number | 0–5 retries. |
| `retry_delay` | number | Base delay in seconds. Default: `0.25`. |
| `success` | function | `function(status, body, headers)`. |
| `failed` | function | `function(reason)`. |

Response header names are lowercase. Duplicate header values are not represented; same deal as the stock GMod HTTP API.

## Retry, cancel, stats

Safe methods (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`) retry twice by default. `POST` and `PATCH` retry zero times by default. That is deliberate: retries can duplicate writes.

Retries cover connection errors, timeouts, and `408`, `429`, `500`, `502`, `503`, `504`. Backoff is exponential, capped at 30 seconds. Numeric `Retry-After` wins.

```lua
local ok, requestId = rhttp({
    url = "https://api.example.com/v1/jobs",
    timeout = 30,
    retries = 3,
    retry_delay = 1,
})

if ok then
    rhttp_cancel(requestId) -- best effort; true when the request still exists
end

PrintTable(rhttp_stats())
-- submitted, in_flight, succeeded, failed, retried, cancelled
```

Do not enable POST retries without an idempotency key or equivalent server-side deduplication. That is not a module bug; that is distributed systems being annoying.

## Defaults

- User-Agent: `gmsv-rhttp/1.1` (override with `headers`).
- Connect timeout: 5 seconds.
- Full request timeout: 30 seconds.
- Redirects: 10.
- Network concurrency: 256.
- In-flight limit: 1024.
- Total Rust-side body buffering: 64 MB. A request fails with `rhttp buffered body limit reached` when this budget is exhausted.

## Build and install

Rust nightly via `rustup` is required.

### Linux x86-64

```bash
./scripts/package-linux.sh
```

Copy `dist/gmsv_rhttp_linux64.dll` to `garrysmod/lua/bin/`.

### Windows x86-64

```powershell
.\scripts\package-windows.ps1
```

Copy `dist/gmsv_rhttp_win64.dll` to `garrysmod\lua\bin\`.

GitHub Actions builds both artifacts.

## Server notes

- This is server-side: `gmsv_`, not `gmcl_`.
- Never pass untrusted URLs or headers through to this module. That is an SSRF footgun.
- Keep payloads small. The 20 MB cap is there to keep your server alive.
