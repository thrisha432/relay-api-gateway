# Relay API and Configuration Reference

## Endpoints

| Method & path | Purpose |
|---|---|
| `ANY /<configured prefix>/...` | Proxied to the matched route's upstreams. Methods: GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS. |
| `GET /health/live` | Liveness: `{"status": "alive"}` whenever the process is serving. |
| `GET /health/ready` | Readiness: 200 `{"status":"ready","checks":{...}}` or 503 `not_ready`. See ARCHITECTURE.md for the policy. |
| `GET /metrics` | Prometheus text exposition. |
| `GET /__relay/routes` | Route table for local debugging: ids, prefixes, upstream ids with health and breaker state. **No upstream URLs.** Disable with `server.enable_route_summary: false`. |

`/health`, `/metrics` and `/__relay` are reserved: config validation rejects
any route prefix that overlaps them. There are no endpoints that change
runtime configuration.

### Response headers added by Relay

| Header | When |
|---|---|
| `X-Request-ID` | Always. An incoming value matching `[A-Za-z0-9._:-]{1,128}` is reused; otherwise Relay generates one. It is also forwarded upstream. |
| `X-Relay-Cache: HIT \| MISS \| BYPASS` | Routes with caching enabled. An upstream cannot spoof this header (it is stripped from upstream responses). |
| `Age` | Cache hits. |
| `X-RateLimit-Limit`, `X-RateLimit-Remaining` | Rate-limited routes, on allowed and on 429 responses. |
| `Retry-After` | 429 responses (whole seconds until a token is available). |

### Headers sent upstream

Client headers are forwarded except hop-by-hop headers (`Connection` and
everything it names, `Keep-Alive`, `Proxy-Authorization`,
`Proxy-Authenticate`, `Proxy-Connection`, `TE`, `Trailer`,
`Transfer-Encoding`, `Upgrade`). Relay sets:

* `Host`: the upstream's host from config.
* `X-Forwarded-For`: the peer address. A prior chain is kept only if the peer is a trusted proxy.
* `X-Forwarded-Host`: the client's original `Host`.
* `X-Forwarded-Proto`: `http` or `https`, as received.
* `X-Request-ID`.

`Authorization` and `Cookie` are forwarded to the route's own upstreams (the
route owner), never anywhere else.

## Error contract

Every error Relay generates itself looks like this (upstream error responses
pass through untouched):

```json
{"error": {"code": "rate_limited", "message": "Rate limit exceeded. Retry later.", "request_id": "6f1c..."}}
```

| Status | `code` | Meaning |
|---|---|---|
| 400 | `invalid_request` | Non-ASCII path, dot segments (including encoded ones), forbidden characters, or a non-numeric `Content-Length`. |
| 404 | `route_not_found` | No configured prefix matches. |
| 405 | `method_not_allowed` | TRACE, CONNECT, or a non-standard method. |
| 413 | `payload_too_large` | Body larger than `server.max_request_body_bytes` (checked against `Content-Length` and while streaming). |
| 429 | `rate_limited` | Token bucket empty; see `Retry-After`. |
| 500 | `internal_error` | Unexpected gateway bug. The details are only in the logs. |
| 502 | `upstream_connection_error` | Could not connect to the upstream (after allowed retries). |
| 502 | `upstream_bad_response` | The upstream sent a malformed response or dropped the connection. |
| 502 | `upstream_response_too_large` | Response larger than `server.max_response_body_bytes`. |
| 503 | `upstream_unavailable` | No healthy upstream for the route. |
| 503 | `circuit_open` | Every healthy upstream's circuit breaker is rejecting calls. |
| 503 | `rate_limiter_unavailable` | Redis unreachable and the policy is fail-closed. |
| 504 | `upstream_timeout` | The attempt or request deadline expired. |

Every error response carries `Cache-Control: no-store`.

## Configuration

Static configuration is YAML (`RELAY_CONFIG`, default `config/routes.yaml`).
Validation runs at startup; any error stops the process with the field path
and reason. You can also check a file without starting the server:

```console
$ relay --config config/routes.yaml --check
config OK: 5 route(s)
$ relay --config bad.yaml --check
invalid Relay configuration:
  - routes.0.retries.retry_methods: Value error, methods ['POST'] are not safe to retry automatically; ...
```

Unknown keys are errors, so a typo can never silently do nothing.

### Environment variables

| Variable | Effect |
|---|---|
| `RELAY_CONFIG` | Path to the YAML file. |
| `RELAY_REDIS_URL` | Overrides `redis.url`. Put credentials here, never in YAML. |
| `RELAY_LOG_LEVEL` | `DEBUG`, `INFO` (default), `WARNING`, ... |
| `RELAY_LOG_JSON` | `true` (default) for JSON lines; `false` for human-readable text. |

### `server`

| Key | Default | Notes |
|---|---|---|
| `host`, `port` | `127.0.0.1`, `8080` | The Compose config uses `0.0.0.0` inside the container, published only on `127.0.0.1`. |
| `max_request_body_bytes` | 1 MiB | 0–100 MiB. |
| `max_response_body_bytes` | 10 MiB | Responses are buffered up to this size. |
| `trusted_proxies` | `[]` | CIDRs whose `X-Forwarded-For` is honored. |
| `enable_route_summary` | `true` | Exposes `/__relay/routes`. |
| `shutdown_grace_seconds` | 5 | Uvicorn graceful shutdown time. |
| `upstream_max_connections` / `upstream_max_keepalive_connections` | 100 / 20 | Shared upstream connection pool. |

### `redis`

| Key | Default | Notes |
|---|---|---|
| `url` | none | Required if any route has `rate_limit`, or if `cache.backend` is `redis`. |
| `connect_timeout_seconds` / `operation_timeout_seconds` | 0.5 / 0.5 | Short, so an outage fails fast. |
| `fail_open_for_rate_limit` | `false` | See "Outage policy" in ARCHITECTURE.md. |

### `cache`

| Key | Default | Notes |
|---|---|---|
| `backend` | `local` | `local` = per-process LRU; `redis` = shared. |
| `max_entries` | 1024 | Local LRU capacity (0 disables storage). |

### `defaults` and per-route policy sections

`defaults.{timeouts, retries, health_check, circuit_breaker}` are merged into
every route **per field**: a route that sets only `retries.max_attempts`
still inherits every other retry setting from `defaults`.

| Section.key | Default | Bounds / notes |
|---|---|---|
| `timeouts.connect_seconds` | 1.0 | ≤ `request_seconds` |
| `timeouts.attempt_seconds` | 2.0 | ≤ `request_seconds` |
| `timeouts.request_seconds` | 5.0 | ≤ 120; covers every attempt and backoff |
| `retries.max_attempts` | 2 | 1–5, counting the first attempt |
| `retries.retry_status_codes` | `[502, 503, 504]` | 5xx only |
| `retries.retry_methods` | `[GET, HEAD, OPTIONS]` | Only these three are accepted |
| `retries.backoff_base_seconds` / `backoff_max_seconds` | 0.05 / 0.5 | base ≤ max |
| `health_check.enabled` | `true` | |
| `health_check.path` | `/health` | |
| `health_check.interval_seconds` / `timeout_seconds` | 5.0 / 1.0 | |
| `health_check.unhealthy_threshold` / `healthy_threshold` | 3 / 2 | Consecutive results |
| `circuit_breaker.enabled` | `true` | |
| `circuit_breaker.failure_threshold` | 5 | Consecutive failures while CLOSED |
| `circuit_breaker.recovery_timeout_seconds` | 10 | Time spent OPEN before probes |
| `circuit_breaker.half_open_max_probes` | 1 | Concurrent probes |
| `circuit_breaker.success_threshold` | 1 | Probe successes needed to close |
| `circuit_breaker.failure_status_codes` | `[502, 503, 504]` | 5xx only |

### `routes[]`

| Key | Required | Notes |
|---|---|---|
| `id` | yes | `[a-z0-9][a-z0-9_-]{0,62}`; unique |
| `path_prefix` | yes | Starts with `/`; a trailing slash is normalized away; unique; may not overlap a reserved path |
| `strip_prefix` | no (`false`) | Remove the prefix before forwarding |
| `upstreams[]` | yes (1–64) | `{id, url}`; ids unique per route; the URL must be http(s), with no credentials, query, or fragment |
| `rate_limit` | no | `{capacity ≥ 1, refill_rate_per_second > 0, key_ttl_seconds?}` |
| `cache` | no (disabled) | `{enabled, ttl_seconds, max_entry_bytes, allowed_status_codes, vary_headers}` (`authorization` and `cookie` cannot be vary headers) |
| `retries`, `timeouts`, `health_check`, `circuit_breaker` | no | Per-field overrides of `defaults` |

### Differences from the illustrative schema in PROJECT_SPEC.md §7

The spec's example is described there as illustrative. The implemented
schema differs as follows:

* `defaults` is grouped into sections (`timeouts`, `retries`,
  `health_check`, `circuit_breaker`) that routes can override per field,
  instead of the flat keys (`request_timeout_seconds`, `max_retries`, ...).
* `failure_threshold` / `recovery_threshold` became
  `health_check.unhealthy_threshold` / `healthy_threshold`, plus separate
  circuit-breaker thresholds.
* `cache.allowed_methods` was dropped: only GET is ever cached.

### Example

```yaml
redis:
  url: "redis://redis:6379/0"
routes:
  - id: catalog
    path_prefix: /api/catalog
    upstreams:
      - { id: catalog-a, url: "http://backend-a:8001" }
      - { id: catalog-b, url: "http://backend-b:8002" }
    rate_limit: { capacity: 60, refill_rate_per_second: 10 }
    cache: { enabled: true, ttl_seconds: 10 }
    retries: { max_attempts: 2 }
```
