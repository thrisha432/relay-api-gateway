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
| A1 | **No request-header size limit.** A 100 KB header was accepted and forwarded upstream. | raw-socket probe → `200 OK` | High (resource abuse; upstream may reject or crash) | _pending_ |
| A2 | **HTTP/1.1 request without `Host` accepted** (RFC 9112 §3.2 requires 400). | raw-socket probe → `200 OK` | Low | _pending_ |
| A3 | **Cache stampede.** Concurrent misses for one key all go upstream. | 50 concurrent misses on one key → 50 upstream GETs | Medium (load spike on the backend exactly when the cache is cold) | _pending_ |
| A4 | **Retry amplification during brownouts.** The breaker needs *consecutive* failures, so an upstream failing every other request never trips it, and every failed GET is retried (up to `max_attempts`×). | design review; total outage is already bounded (100 requests → 10 upstream calls once breakers opened) | Medium | _pending_ |
| A5 | Benchmarks are **closed-loop only**. Under saturation, closed-loop load hides queueing delay ("coordinated omission"), so the latency of a fixed offered load was never measured. | `benchmarks/k6/*` use `vus`/`duration` | Medium (evidence quality) | _pending_ |
| A6 | Performance evaluated at **one configuration** (10 or 50 VUs, ~0 ms upstream). No saturation curve, no upstream-latency sweep, no comparison with an established proxy, no horizontal scaling. | benchmark report | Medium (evidence quality) | _pending_ |
| A7 | **Cached-path overhead unexplained:** cached responses reach about 36% of direct throughput even though they never touch the upstream. | benchmark report (6,960 vs 19,380 req/s) | Low (understanding) | _pending_ |
| A8 | Redis outage → recovery verified only manually; the existing tests use a dead port, never a Redis that comes *back*. | live probe: 200 → 503 503 → 200 200 200 | Medium (untested recovery path) | _pending_ |
| A9 | Graceful shutdown with in-flight requests is untested. | no test | Medium | _pending_ |
| A10 | Rate-limit concurrency tested with in-process gateways only, never with separate OS processes. | `test_two_gateway_instances_share_one_bucket` | Low–Medium | _pending_ |
| A11 | No static type checking. | no mypy in CI | Low | _pending_ |
| A12 | Test strength unmeasured: line/branch coverage is known (96%), but whether the tests *detect* behavior changes is not. | no mutation testing | Medium (evidence quality) | _pending_ |
| A13 | No dependency-vulnerability scan in CI. | CI config | Low | _pending_ |
| A14 | Failure scenarios are not systematic: only "one backend returns 503" was load-tested. Hangs, refused connections, total outage, Redis loss and gateway restart under load were not. | `benchmarks/k6/failure.js` | Medium | _pending_ |
| A15 | CI has never run (repository not pushed). | — | Medium | _pending (requires the author to push)_ |

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
