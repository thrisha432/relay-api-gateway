# Relay v0.1 Audit (2026-10-25)

An audit of the existing implementation before any v1.0 changes. Each finding
cites the probe or measurement that produced it. The **Resolution** column is
filled in as fixes land, and each fix links to the test that guards it.

## Method

1. Reread the request pipeline (`proxy.py`) and the resilience components.
2. Sent hostile raw HTTP/1.1 requests over a socket to the running Compose
   stack, bypassing client libraries that would normalize them.
3. Fault probes on the live stack: Redis stopped and restarted mid-traffic;
   every backend failing; 50 concurrent cache misses on one key.
4. Compared what the docs claim with what a test actually exercises.

## Findings

| ID | Finding | Evidence (v0.1) | Severity | Resolution |
|---|---|---|---|---|
| A1 | **No request-header size limit.** A 100 KB header was accepted and forwarded upstream. | raw-socket probe → `200 OK` | High (resource abuse; upstream may reject or crash) | **Fixed.** `RequestLimitsMiddleware`: 431 above `max_header_bytes` (32 KiB) or `max_header_count` (100). `tests/security/test_security.py::test_oversized_headers_rejected_with_431` |
| A2 | **HTTP/1.1 request without `Host` accepted** (RFC 9112 §3.2 requires 400). | raw-socket probe → `200 OK` | Low | **Fixed.** 400 for HTTP/1.1 without `Host`; HTTP/1.0 still allowed. `test_http11_without_host_rejected_but_http10_allowed` |
| A3 | **Cache stampede.** Concurrent misses for one key all go upstream. | 50 concurrent misses on one key → 50 upstream GETs | Medium (load spike on the backend exactly when the cache is cold) | **Fixed.** Single-flight coalescing (`cache/singleflight.py`): 50 concurrent misses → 1 upstream call, measured in the stampede benchmark. `tests/integration/test_coalescing_and_budget.py` |
| A4 | **Retry amplification during brownouts.** The breaker needs *consecutive* failures, so an upstream failing every other request never trips it, and every failed GET is retried (up to `max_attempts`×). | design review; total outage is already bounded (100 requests → 10 upstream calls once breakers opened) | Medium | **Fixed.** Retry budget (`resilience/retry_budget.py`, default 20% of requests). Trade-off measured in the brownout benchmark. `test_retry_budget_caps_amplification_during_brownout` |
| A5 | Benchmarks are **closed-loop only**. Under saturation, closed-loop load hides queueing delay ("coordinated omission"), so the latency of a fixed offered load was never measured. | `benchmarks/k6/*` use `vus`/`duration` | Medium (evidence quality) | _pending_ |
| A6 | Performance evaluated at **one configuration** (10 or 50 VUs, ~0 ms upstream). No saturation curve, no upstream-latency sweep, no comparison with an established proxy, no horizontal scaling. | benchmark report | Medium (evidence quality) | _pending_ |
| A7 | **Cached-path overhead unexplained:** cached responses reach about 36% of direct throughput even though they never touch the upstream. | benchmark report (6,960 vs 19,380 req/s) | Low (understanding) | _pending_ |
| A8 | Redis outage → recovery verified only manually; the existing tests use a dead port, never a Redis that comes *back*. | live probe: 200 → 503 503 → 200 200 200 | Medium (untested recovery path) | **Tested.** TCP chaos proxy: a real Redis is reset, then restored (503 → 200 with no restart); a hung Redis fails closed within the 0.3 s operation timeout. `tests/integration/test_chaos.py` |
| A9 | Graceful shutdown with in-flight requests is untested. | no test | Medium | **Tested.** An in-flight request finishes during shutdown; new connections are refused; tasks and clients are closed. `tests/integration/test_lifecycle.py` |
| A10 | Rate-limit concurrency tested with in-process gateways only, never with separate OS processes. | `test_two_gateway_instances_share_one_bucket` | Low–Medium | **Tested.** 3 real `python -m relay` processes, 300 concurrent requests, capacity 40 → exactly 40 admitted; all exit cleanly on SIGTERM. `test_three_gateway_processes_share_one_bucket_exactly` |
| A11 | No static type checking. | no mypy in CI | Low | **Fixed.** `mypy --strict` clean on `src/`; run in CI. |
| A12 | Test strength unmeasured: line/branch coverage is known (96%), but whether the tests *detect* behavior changes is not. | no mutation testing | Medium (evidence quality) | **Measured.** mutmut on 11 core modules: 86.9% → 89.6%, every remaining survivor classified equivalent. Real gaps found and fixed (incl. `max-age=60,no-store` without a space). `docs/QUALITY.md` |
| A13 | No dependency-vulnerability scan in CI. | CI config | Low | **Fixed.** `pip-audit -r requirements.lock` in CI (no known vulnerabilities, 2026-10-25). |
| A14 | Failure scenarios are not systematic: only "one backend returns 503" was load-tested. Hangs, refused connections, total outage, Redis loss and gateway restart under load were not. | `benchmarks/k6/failure.js` | Medium | _pending_ |
| A15 | CI has never run (repository not pushed). | — | Medium | _pending (requires the author to push)_ |
| A16 | **Critical: connection-pool leak under overload → permanent outage.** Cancelling httpx with `asyncio.timeout` orphaned pooled connections in `CONNECTING` state whenever cancellation landed between "pool assigned a new connection" and "request dialed". After an open-loop overload spike, every route returned 504 *forever* while the backend answered directly in 1 ms. | found by the open-loop benchmark (v0.1 matrix); reproduced deterministically with a 4-connection pool: 4/4 connections stuck `CONNECTING`, all later requests 504 | **Critical** | **Fixed.** No external cancellation of httpx: per-phase httpx timeouts (pool/connect/read/write) plus a cooperative attempt-budget check after headers and between chunks. `tests/integration/test_pool_leak.py` |
| A17 | **Metastable overload failure: no admission control.** Beyond capacity, requests queued in the event loop until they timed out; the breaker counted those self-inflicted timeouts against a *healthy* upstream and opened, and half-open probes kept failing while load persisted. | open-loop sweep: 1,200 req/s → 93.5% errors; 1,400 and 1,600 req/s → 100% errors with 0 upstream requests | **High** | **Fixed.** `server.max_in_flight` (default 128): excess requests get an immediate 503 `overloaded` + `Retry-After: 1`; operational endpoints are never shed; shedding never counts as a breaker failure. New `relay_event_loop_lag_seconds` gauge makes gateway saturation visible. `tests/integration/test_admission.py`; re-measured in the v1 open-loop sweep |
| A18 | Benchmark harness did not isolate cells: breaker state left by one cell contaminated the next. | breaker open from the 1,200 req/s cell persisted into later cells | Medium (evidence quality) | **Fixed.** `bench_matrix.py` waits until every breaker is closed before each run; the contaminated v0.1 matrix run was discarded |

## Verified OK during the audit (no change needed)

* **Absolute-form request target** (`GET http://evil.example/... HTTP/1.1`):
  Uvicorn reduces it to the path, and Relay still routes only to the
  configured upstream. **No SSRF.**
* `CONNECT` and conflicting `Content-Length` + `Transfer-Encoding` are
  rejected with 400 by the HTTP parser before Relay sees them.
* `//evil.example/...` (protocol-relative path) → 404, never a proxy target.
* Redis stopped mid-traffic → limited routes return 503 (fail-closed), then
  recover automatically when Redis returns, with no restart needed.
* Every backend failing → circuit breakers bound the upstream load
  (100 client requests → 10 upstream requests).
