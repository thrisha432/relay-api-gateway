# Relay Architecture

Relay is a single-process asyncio HTTP gateway. This document describes how a
request flows through it, how each component works, how state is shared, and
what happens when things fail. It describes the code as implemented.

> Portfolio project: designed and tested carefully, but not production-certified.

## Components at a glance

```text
                       ┌──────────────────────── Relay process (one asyncio event loop) ───────────────────────┐
 client ──HTTP──▶ Uvicorn ─▶ RequestContextMiddleware ─▶ FastAPI router ─┬─▶ /health/live, /health/ready,     │
                       │      (request id, in-flight,                     │   /metrics, /__relay/routes        │
                       │       access log, JSON 500)                      └─▶ Gateway.handle (catch-all)       │
                       │                                                       │                               │
                       │   Router ── RedisRateLimiter ── ResponseCache ── RoundRobinBalancer ── CircuitBreaker │
                       │   (longest     (Lua token        (LRU or Redis)   (+ HealthRegistry)   (per upstream) │
                       │    prefix)      bucket)                                                │              │
                       │                                         RetryPolicy + Deadline ── httpx.AsyncClient ──┼──▶ upstreams
                       │   HealthChecker: one background task per route ── dedicated httpx client ─────────────┼──▶ /health
                       └──────────────────────────────────────────────────────────────────────────────────────┘
                                   │ Redis (rate-limit buckets, optional shared cache)   ▲ Prometheus scrapes /metrics
```

| Module | Responsibility |
|---|---|
| `config.py` | Pydantic schema; YAML + env loading; defaults merging; startup validation |
| `app.py` | ASGI app factory, lifespan (start/stop), operational endpoints |
| `middleware.py` | Admission control (load shedding), header limits, request IDs, in-flight gauge, request metrics, access log, last-resort JSON 500 |
| `proxy.py` | `Gateway`: owns clients/state and runs the request pipeline |
| `routing.py` | Path validation and longest-prefix matching |
| `headers.py` | Hop-by-hop filtering on raw header bytes |
| `client_identity.py` | Client IP resolution with explicit trusted proxies |
| `rate_limiter.py` + `token_bucket.lua` | Distributed token bucket in Redis |
| `cache/lru.py` | From-scratch LRU (hash map + doubly linked list) |
| `cache/response_cache.py` | Cache eligibility policy, cache key, local backend |
| `cache/redis_cache.py` | Shared Redis cache backend |
| `cache/singleflight.py` | Coalesces concurrent misses for one key into one upstream request |
| `load_balancer.py` | Health-aware round-robin |
| `health.py` | Active health checks and per-upstream health state |
| `resilience/circuit_breaker.py` | Closed / open / half-open state machine |
| `resilience/retry.py`, `resilience/timeouts.py` | Retry eligibility, jittered backoff, deadlines |
| `resilience/retry_budget.py` | Caps aggregate retries at a fraction of traffic |
| `metrics.py` | Prometheus registry with bounded labels |
| `mock_backend/app.py` | Deterministic upstream used by tests, demo, and benchmarks |

## Request pipeline

The order below is implemented in the middleware stack and in
`Gateway._handle` / `_forward`. It follows the spec's recommended order with
deliberate changes, called out in bold.

0. **Admission control** (outermost middleware). If `max_in_flight` proxied
   requests are already in progress, reply at once with 503 `overloaded` and
   `Retry-After: 1`, before routing, logging, or any other per-request work.
   *Added after the open-loop benchmark exposed a metastable failure (see
   "Overload and admission control").* Header limits come next: more than
   `max_header_bytes`/`max_header_count` gives 431, and an HTTP/1.1 request
   without `Host` gives 400.
1. **Validate the path and method.** The path must be origin-form ASCII with
   no dot segments, even encoded ones like `%2e%2e` or `..%2F` (400).
   `TRACE`/`CONNECT` and unknown methods get 405.
2. **Match a route**: the longest `path_prefix` wins, on a segment boundary, so
   `/api/catalog` never matches `/api/catalogue` (404 otherwise).
3. **Rate limit** (routes with `rate_limit`). The client identity is the peer
   IP, or the right-most untrusted `X-Forwarded-For` hop when the peer is a
   configured trusted proxy. Exhausted: 429 with `Retry-After`. Redis down:
   503 under the default fail-closed policy.
4. **Read the request body**, capped at `max_request_body_bytes` (413).
   *Changed order: body reading moved after rate limiting, so a limited client
   cannot make the gateway buffer a large body first.*
5. **Cache lookup** for eligible GETs. A hit returns immediately. On a miss,
   concurrent requests for the same key are **coalesced**: one fetches, the
   rest wait for its result.
   *Consequence: a cache hit is served even when every upstream is unhealthy,
   and cache hits still consume a rate-limit token (the limiter runs first).*
6. **Select an upstream**: round-robin over healthy instances, skipping any
   whose circuit breaker rejects the call (503 `upstream_unavailable` /
   `circuit_open` if none is left).
7. **Forward** with a per-attempt budget of `min(attempt_seconds, time left
   in the request deadline)`. Safe methods are retried on transient failures,
   preferring an instance not yet tried, as long as the route's **retry
   budget** allows.
8. **Record** the breaker outcome, upstream latency, and outcome metrics.
9. **Fill the cache** if the response is eligible.
10. **Respond** with hop-by-hop headers removed, plus `X-Request-ID`,
    `X-Relay-Cache` (cache-enabled routes) and `X-RateLimit-*` (limited routes).

## Routing and proxy correctness

* **Fixed destinations.** Upstream URLs come only from validated config:
  http/https only, no embedded credentials, no query or fragment. Nothing in
  a client request can change the host Relay connects to, which rules out SSRF.
* **Raw path matching.** Matching uses the percent-encoded `raw_path` as the
  client sent it, and that exact string is forwarded, so encoded segments
  like `%2F` survive the hop. `strip_prefix: true` removes the route prefix
  before forwarding.
* **Headers** are processed as raw `(bytes, bytes)` pairs, so values pass
  through byte-for-byte and repeated headers (several `Set-Cookie`) keep
  their multiplicity. Relay removes the RFC 9110 hop-by-hop set plus every
  header named in `Connection`. It also drops `Host` (httpx sends the
  upstream's own host, while the original goes in `X-Forwarded-Host`) and
  `Content-Length` (recomputed), and replaces `X-Forwarded-For`/`-Proto` and
  `X-Request-ID` with its own values.
* **One pooled client.** A single `httpx.AsyncClient` is created at startup
  and closed at shutdown. `trust_env=False` (ignore `HTTP_PROXY` env vars),
  `follow_redirects=False` (redirects go back to the client).
* **Buffered responses.** Relay does *not* stream. Response bodies are read
  fully, capped at `max_response_body_bytes` (default 10 MiB; a declared
  `Content-Length` above the cap is rejected before reading). Exceeding it
  gives 502 `upstream_response_too_large`. Bodies are read with `aiter_raw`,
  so compressed bodies pass through untouched with their `Content-Encoding`.
* **Errors never leak internals.** Transport exceptions map to fixed error
  codes. Unexpected exceptions are logged with a traceback and answered with a
  generic JSON 500.

## Overload and admission control

**The failure it prevents (measured, not hypothetical).** In the v0.1
open-loop benchmark, offered load above capacity (1,200 req/s against about
1,100 req/s of capacity) caused this chain:

1. Requests queued in the event loop until they exceeded the 2 s attempt
   timeout, producing 504s.
2. The circuit breaker counted those self-inflicted timeouts against a
   *healthy* upstream and opened.
3. Half-open probes also timed out under the same overload, so the breaker
   stayed open.

Error rates were 93.5% at 1,200 req/s and 100% at 1,400 and 1,600 req/s, with
zero requests reaching the backend. This is a *metastable* failure: the system
stays down while the trigger persists, even though nothing downstream is
broken.

**The fix.** Bound the work admitted. `AdmissionMiddleware` caps proxied
requests in flight at `server.max_in_flight` and answers the rest with an
immediate 503 `overloaded` + `Retry-After: 1`.

* **Rejection must be cheap.** It runs in the outermost middleware with a
  pre-encoded body: no routing, no request context, no access-log line (a
  counter still records it). Shedding inside the gateway pipeline was measured
  first and was not good enough. At 1.6× overload, goodput fell to about 525
  req/s, because CPU spent *rejecting* starved the admitted requests.
* **Size by Little's law:** limit ≈ throughput × acceptable queueing delay.
  A limit of 32 held goodput at about 1,000–1,100 req/s with p99 about 110–130
  ms at 1.6× overload; a limit of 128 let p99 rise to about 530 ms. The
  default is **64** (about 1,100 req/s × about 50 ms). The full v1 open-loop
  sweep compares it against the limit disabled.
* **Health and metrics are never shed.** Probes and scrapes keep working under
  overload, so orchestrators see "alive but busy", not "dead".
* **Shedding is not an upstream failure.** It never touches breakers.
* **Visibility:** `relay_load_shed_total{route}` and
  `relay_event_loop_lag_seconds` (how late a 100 ms timer fires). Event-loop
  lag separates "the gateway is saturated" from "the upstream is slow", which
  is exactly the confusion that caused the metastable failure.

Not implemented: an *adaptive* limit (AIMD / gradient, as in Netflix's
concurrency-limits). A static limit is predictable and explainable but must be
re-sized when capacity changes.

## Upstream connection pool: a design constraint (audit A16)

The open-loop benchmark also exposed a **critical** bug. After an overload
spike, every route returned 504 *forever*, while the backend answered directly
in 1 ms. The cause was an httpcore race (present in 1.0.9, the latest
release):

* Request R waits in the pool queue. Another task creates connection C and
  assigns it to R.
* If R's wait then ends with an exception before R resumes (an external cancel
  such as `asyncio.timeout`, *or httpcore's own `PoolTimeout`*), R leaves the
  queue but C is never closed.
* C has never dialed, so it is never closed, expired, idle, or available: one
  pool slot lost permanently. Repeated overload loses all of them.

Relay's defenses:

1. **No external cancellation of httpx.** Each phase uses httpx's own timeout
   (pool, connect, read, write), and the whole-attempt budget is checked
   cooperatively after the response headers and between body chunks. The
   trade-off: one stalled read can overrun the attempt budget by up to the
   read timeout (which equals the budget), so a worst-case attempt is bounded
   by about 2× the budget instead of exactly 1×.
2. **Requests never wait in the pool queue.** Config validation enforces
   `max_in_flight ≤ upstream_max_connections`. Each admitted request uses at
   most one upstream connection at a time: retries are sequential, coalesced
   followers use none, and health checks have their own client. So a free or
   creatable connection always exists in the request's own synchronous call,
   and the racy queued-wait path is never entered. Disabling admission control
   (`max_in_flight: 0`) gives up this guarantee and logs a startup warning.
3. **Detection:** `relay_upstream_pool_unstarted_connections` (scrape-time,
   read-only introspection that falls back to −1 if httpcore's internals
   change). If it stays above 0, slots are leaking.

The regression test drives five overload spikes at 5× the limit and asserts
full recovery with zero orphaned connections.

## Load balancing

`RoundRobinBalancer.candidates()` filters to healthy instances, rotates a
counter over *that* list, and returns the rotation, which doubles as the
failover order. Rotating over the available list (instead of skipping
unhealthy slots in the full list) keeps the split even: with A, B, C and B
down, A and C each get 50%.

**Concurrency:** the method contains no `await`, so on the single-threaded
event loop each call runs to completion. That makes the counter's
read-modify-write atomic without a lock. A test runs 1,000 concurrent
selections and checks an exactly even split.

## Active health checks

* One background task per route with health checks enabled, so the task count
  is fixed by config. Each round probes all of the route's upstreams
  concurrently, each with its own timeout (`timeout_seconds`).
* 2xx = success. Any other status, timeout, or connection error = failure.
* `unhealthy_threshold` consecutive failures mark an instance unhealthy;
  `healthy_threshold` consecutive successes restore it. Instances start
  healthy (optimistic) and the first round runs at startup.
* Probes use a **dedicated small HTTP client**. If proxy traffic saturates
  the main connection pool, probes would otherwise time out waiting for a
  connection and wrongly mark healthy upstreams down.
* Tasks are created in the lifespan startup and cancelled-and-awaited at
  shutdown (a test asserts no `relay-health-*` task survives shutdown).

**Readiness vs liveness:** `/health/live` returns 200 whenever the process is
serving. `/health/ready` returns 200 only when (a) the first health-check
round has finished, (b) every route has at least one healthy upstream, and
(c) Redis answers `PING`, if the config uses Redis. The trade-off: one route
with no healthy upstreams makes the whole instance "not ready". That's the
right signal for this demo, but in production you might want per-route
readiness instead.

## Distributed rate limiting (token bucket)

Each `(route, client)` pair has a bucket in Redis: a hash with `tokens` and
`ts` fields.

* **Algorithm:** the bucket holds up to `capacity` tokens and refills
  continuously at `refill_rate_per_second`; each request costs one token.
  `capacity` is the allowed burst and the refill rate is the long-run rate.
  When the bucket is empty, `Retry-After = ceil((1 - tokens) / rate)` seconds.
* **Atomicity:** refill, check, and consume run inside one Lua script.
  Redis executes a script without interleaving other commands, so two
  gateways can never both spend the last token. That is the race a
  GET-then-SET implementation would have. Tests fire 60 concurrent requests
  at a capacity-10 bucket, and the same across two gateway instances; exactly
  the capacity is admitted.
* **One clock:** the script reads Redis's own `TIME`, so gateways with skewed
  clocks still agree on "now". A backwards jump adds no tokens, and the stored
  timestamp never moves backwards. (The timestamp is written with
  `string.format('%.0f')`, because Lua's `tostring` keeps only 14 significant
  digits, about 100 µs of error on microsecond timestamps.)
* **Expiry:** keys get `PEXPIRE` of `key_ttl_seconds` (default: twice the
  full-refill time), so idle clients cost no memory. An expired bucket is
  simply full.
* **Privacy:** client IPs are SHA-256 hashed into the key and never logged.
* **EVALSHA:** redis-py's `Script` uses `EVALSHA` and falls back to `EVAL` if
  Redis lost its script cache (tested).

**Outage policy** (`redis.fail_open_for_rate_limit`):

| Setting | Redis unreachable on a rate-limited route |
|---|---|
| `false` (default) | **Fail closed:** 503 `rate_limiter_unavailable`; `relay_rate_limit_decisions_total{decision="fail_closed"}` and `relay_redis_errors_total{operation="rate_limit"}` increase. |
| `true` | **Fail open:** request allowed; `decision="fail_open"` counted; a warning is logged at most once per 10 s. |

Routes without `rate_limit` are unaffected either way. The Redis client is
built to fail fast (short timeouts, at most one quick retry). That came from
a measurement: with redis-py's default retry policy, every request during an
outage waited about 0.5 s before failing.

## Caching

### The LRU (`cache/lru.py`)

A dictionary maps key → node. A doubly linked list with **sentinel head and
tail nodes** keeps recency order (head side = most recently used). `get`
moves the node to the front, `put` inserts at the front, and eviction unlinks
`tail.prev`. Each of these touches a constant number of pointers, so all
three are average O(1); "average" because dict operations are.

Defined edge cases: a miss returns the default; updating an existing key
replaces the value and refreshes recency without evicting; inserting at
capacity evicts exactly the LRU entry; capacity 0 stores nothing; `peek` and
`in` do not change recency.

Invariants checked by `check_invariants()`: map size equals list length;
every list node is in the map under its own key; `prev`/`next` pointers are
mutually consistent; size ≤ capacity. A model-based test runs 20 seeds × 2,000
random operations against an `OrderedDict` reference and checks the
invariants after every step.

### Response cache policy (supported subset)

Relay implements a conservative subset of HTTP caching, not RFC 9111:

* **Lookup is bypassed** for non-GET requests, requests with `Authorization`,
  `Cookie` or `Range`, and requests with `Cache-Control: no-cache`/`no-store`
  or `Pragma: no-cache`.
* **A response is stored only if** its status is in `allowed_status_codes`
  (default `[200]`); it has no `Set-Cookie`; `Cache-Control` has no `private`,
  `no-store` or `no-cache`; `Vary` is not `*` and names only headers already
  in the key; and the body is at most `max_entry_bytes`.
* **TTL** = route `ttl_seconds`, lowered by `s-maxage` (preferred) or
  `max-age`. `max-age=0` means do not store.
* **Key** = SHA-256 of route id, method, raw path, raw query string, and the
  configured `vary_headers` (default `accept`, `accept-encoding`). Query
  strings are used verbatim, so `?a=1&b=2` ≠ `?b=2&a=1`: always correct,
  occasionally less efficient.
* Stored headers exclude hop-by-hop headers, `Date`, and `Age`. A hit adds
  `Age` and `X-Relay-Cache: HIT`.
* **Not implemented:** `Expires`, revalidation (ETag / `If-None-Match`),
  `stale-while-revalidate`.

### Request coalescing (single-flight)

Without it, 50 concurrent misses for one hot key sent 50 identical requests
upstream (measured during the audit): a cache stampede exactly when the cache
is cold. `SingleFlight` keeps one in-flight fetch per cache key, and
concurrent misses await its result (`X-Relay-Cache: COALESCED`).

* The shared fetch runs in **its own task**, and waiters await it through
  `asyncio.shield`. A leader whose client disconnects does not cancel the
  fetch for everyone else, and the cache is still filled.
* Errors propagate to every waiter. The entry is removed when the fetch
  completes, so results are shared only between *concurrent* requests;
  reuse over time is the cache's job.
* The coalescing key is the cache key, so requests that the cache would
  treat as different (another query, `Accept`, or a credentialed bypass) are
  never merged. Can be switched off per route (`cache.coalesce: false`).

### Backends

| `cache.backend` | Scope | Bounds | When Redis fails |
|---|---|---|---|
| `local` | Per process | `max_entries` × `max_entry_bytes`; LRU eviction; lazy TTL expiry | n/a |
| `redis` | Shared by every gateway instance | Redis TTL (`PSETEX`) plus Redis `maxmemory` policy | Treated as a miss / skipped store; counted in `relay_redis_errors_total`; never fails the request |

## Circuit breaker

One breaker per `(route, upstream)`:

```text
CLOSED ──(failure_threshold consecutive failures)──▶ OPEN
OPEN ──(recovery_timeout elapsed, on the next request)──▶ HALF_OPEN
HALF_OPEN ──(success_threshold probe successes)──▶ CLOSED
HALF_OPEN ──(any probe failure)──▶ OPEN   (cooldown restarts)
```

* **Failures** are connect errors, timeouts, malformed responses, and the
  statuses in `failure_status_codes` (default 502/503/504). 4xx responses
  and other 5xx count as **successes**: the upstream is reachable, and a
  client bug must not trip the breaker. Oversized responses are neutral.
* **Probe limiting:** in HALF_OPEN at most `half_open_max_probes` requests
  are in flight; the rest are rejected instantly. A test sends 20 concurrent
  requests to a recovering upstream and exactly 1 reaches it.
* **Never stuck:** every permit from `try_acquire` ends in exactly one
  `record_success`, `record_failure`, or `release`, enforced with
  `try/finally` (which also runs on cancellation). A test cancels a client
  mid-probe and asserts the probe slot is returned. A property test runs 200
  random operation sequences and asserts the breaker always admits traffic
  again after the cooldown.
* **Stale results:** permits carry a generation number that increments on
  every transition. A slow request that started while CLOSED and finishes
  after the breaker opened cannot change the new state's counters.
* An OPEN breaker removes that instance from selection, so traffic fails over
  to other healthy instances. Transitions are logged and counted;
  `relay_circuit_breaker_state` is computed at scrape time from the live
  breaker objects, so it cannot drift.

## Timeouts and retries

Three layers of limits:

| Limit | Applies to |
|---|---|
| `connect_seconds` | TCP connect (httpx connect timeout) |
| `attempt_seconds` | One upstream attempt end to end (`asyncio.timeout`, plus httpx read/write/pool timeouts) |
| `request_seconds` | The whole client request across all attempts and backoff (`Deadline`) |

Retry rules:

* Only **GET, HEAD, OPTIONS** are retried. Config validation rejects any
  other method in `retry_methods`, because Relay has no idempotency-key
  contract and a "failed" POST may already have been applied. Even a POST
  whose connection was refused is not retried, which is deliberately
  conservative.
* Retried outcomes: connect error, timeout, malformed response, and
  `retry_status_codes` (default 502/503/504).
* Bounds: `max_attempts` ≤ 5 (validated); never start an attempt that would
  not fit in the remaining deadline; stop if the client disconnected; a
  breaker that opens takes the instance out of rotation. Backoff uses "full
  jitter": uniform in `[0, min(max, base·2^(n-1))]`, which de-synchronizes
  retries from many clients.
* **Retry budget (aggregate bound).** `max_attempts` bounds retries *per
  request*, not in total. The breaker needs *consecutive* failures, so an
  upstream that fails every other request never trips it, and every failure
  is retried: a brownout amplifies load exactly when the upstream is
  struggling. Each route has a token budget. Every request deposits
  `budget_ratio` (default 0.2) tokens, a reserve refills at
  `budget_min_retries_per_second`, the balance is capped at
  `budget_max_balance`, and each retry spends one token. Long run: retries ≤
  20% of requests. The trade-off: once the budget is spent, some requests
  that a retry would have rescued fail instead. The brownout benchmark
  measures both sides.
* When retries run out on an upstream status (say 503), the upstream's own
  response is returned. When they run out on a transport failure, Relay
  returns its JSON error (502/504).

## Observability

* `/metrics` uses a per-app `CollectorRegistry`. Labels are bounded: route and
  upstream ids from config, a fixed method set (`OTHER` otherwise), status
  classes, and small fixed enums. A test sends unique paths, query strings,
  and client IPs, then asserts none of them appear in any label value.
* The access log is one JSON line per request: request id, route, upstream,
  method, status, duration, attempts/retries, cache result, rate-limit
  decision, error code. Bodies, auth headers, cookies, and client IPs are
  never logged.
* The Grafana dashboard (`deploy/grafana/dashboards/relay.json`) is generated
  by `scripts/build_dashboard.py`.

## Concurrency model and its limits

Everything runs on one asyncio event loop in one process. Balancer cursors,
breaker state, health state, and the LRU are mutated only by synchronous code
with no `await` in the critical section, so no locks are needed. The
trade-offs:

* **One CPU core.** The gateway is CPU-bound on a single core under load (see
  the benchmark report). It scales out by running more instances: the
  horizontal-scaling benchmark runs 1, 2, and 4 instances behind Docker DNS. Running several Uvicorn workers would scale
  throughput, but each worker would have its own LRU, breakers, health
  checker, and round-robin cursor. Redis-backed rate limits stay globally
  correct, since that state lives in Redis.
* Breaker and health state are **per process**, so two gateway instances
  each detect failures independently. That is a standard and acceptable
  choice; sharing breaker state would add a Redis round-trip per request.

## Performance notes (measured, see `benchmarks/results/`)

* Profiling (cProfile under k6 load) showed httpcore running `import sniffio`
  each time it creates a lock. The current anyio no longer installs `sniffio`,
  and failed imports are never cached, so every call re-searched `sys.path`.
  Adding `sniffio` as a dependency raised uncached gateway throughput from a
  median of 1,068 to 1,386 req/s (10 VUs, 3 × 20 s runs each;
  `benchmarks/results/profiling/`).
* Shrinking the upstream pool (100/20 instead of 200/50) made no measurable
  difference (1,407 vs 1,386 req/s, within noise). The limits are
  configurable, with that measured result recorded here.
* The remaining per-request cost is dominated by httpcore's pool bookkeeping
  (it scans pooled connections and polls their sockets on every request) and
  by the ASGI stack. Next steps, if needed: multiple workers, or a leaner
  HTTP client.
