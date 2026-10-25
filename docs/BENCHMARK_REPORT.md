# Benchmark Report: 2026-10-25

Every number here comes from
[`benchmarks/results/20261025T025934Z/`](../benchmarks/results/20261025T025934Z/SUMMARY.md):
raw k6 JSON, k6 stdout, and `docker stats` samples for each run. Values are
the **median of 3 runs (min–max)**. These are local laptop measurements, not
production capacity claims.

## Environment

* Apple M4 Pro (12 cores, 24 GiB), macOS 15.7.7; Docker 29.4.0 VM with 12 CPUs / ~7.75 GiB
* Relay image: Python 3.12.15, FastAPI 0.162.0, Starlette 1.10.0, Uvicorn 0.54.0 (uvloop, httptools), httpx 0.28.1, redis-py 7.4.1
* k6 v1.1.0; k6, Relay, mock backends and Redis all in Docker on one bridge network
* One Uvicorn worker per service; closed-loop GETs (no think time); ~0.5 KB JSON responses
* Git commit `cfc5035`

## Method

| Scenario | Path |
|---|---|
| direct | k6 → backend-a |
| gateway-uncached | k6 → Relay `/api/bench` → backend-a (same single upstream as direct; no cache, no rate limit) |
| gateway-cached | k6 → Relay `/api/bench-cached` with 50 repeating keys (cache-friendly) |
| failure | 10 VUs on `/api/bench-ha` (backend-a + backend-b) for 80 s; backend-b returns 503 and fails health checks from t=20 s to t=50 s |
| ratelimit | One client alternating between two gateway instances on `/api/orders` (capacity 5, refill 1/s) for 30 s |

Profiles: **baseline** (10 VUs, 60 s) and **load** (50 VUs, 120 s). The
scenarios are interleaved within each of the 3 runs.

## Results

| Scenario | Requests/s | p50 ms | p95 ms | p99 ms | Errors | Cache hit |
|---|---|---|---|---|---|---|
| direct, 10 VUs | 19,380 (19,340–19,619) | 0.43 | 0.87 | 1.04 | 0% | – |
| gateway uncached, 10 VUs | 1,402 (1,353–1,411) | 5.38 | 14.50 | 41.22 | 0% | – |
| gateway cached, 10 VUs | 6,960 (6,773–7,010) | 1.23 | 2.50 | 2.79 | 0% | 99.9% |
| direct, 50 VUs | 20,382 (20,371–20,454) | 2.32 | 2.90 | 4.73 | 0% | – |
| gateway uncached, 50 VUs | 1,146 (1,138–1,162) | 32.41 | 116.57 | 177.15 | 0% | – |
| gateway cached, 50 VUs | 7,002 (6,999–7,023) | 6.76 | 7.73 | 13.68 | 0% | 99.7% |

Container CPU (100% = one core): Relay sat at **100–102%** in every gateway
scenario, so it is CPU-bound on its single core. Backend-a was the bottleneck
on the direct path (101%). Relay's peak memory stayed between 52 and 66 MiB.

### Failure injection

| Metric | Median (min–max) |
|---|---|
| Throughput | 1,327 req/s (1,308–1,334) |
| p95 / p99 | 13.80 ms / 30.30 ms |
| Client-visible error rate | **0.000%** in all 3 runs |
| Retries | 7 (6–7) |
| Circuit-breaker transitions | 3 (closed → open → half-open → closed) |
| Requests failed fast by the open breaker (routed to backend-a) | 2,632 (2,590–2,721) |
| Exhausted retries / no healthy upstream | 0 / 0 |

### Distributed rate limit (two gateways, one Redis)

Allowed: **34, 34, 34** in the three runs, against a theoretical ceiling of
5 + 1 × 30 = 35. Around 248,000 requests per run got 429. The shared Lua
bucket never admitted more than the limit across both instances.

## Interpretation

* **The gateway adds real overhead to uncached requests.** At 10 VUs,
  throughput drops from 19.4k to 1.4k req/s and p50 latency rises from 0.43
  to 5.38 ms. Relay is pinned at one core: each request pays for Python ASGI
  handling, metrics, a JSON access log line, and an httpx/httpcore upstream
  call, whose connection-pool bookkeeping dominated the profile. At 50 VUs
  requests queue behind that core, so p95/p99 grow (117/177 ms) while
  throughput stays roughly flat. This is the expected trade-off of a
  single-process Python proxy hop. The mock backend does almost no work, so
  this is a worst-case ratio; with real backend latency the relative
  overhead would be much smaller.
* **Caching helps repeated GETs a lot**: about 5× the uncached gateway
  throughput (6,960 vs 1,402 req/s) and p95 of 2.50 ms vs 14.50 ms at
  10 VUs. Cached responses are still slower than hitting the backend
  directly, because they still pass through the gateway's request pipeline.
* **Failure handling worked as designed.** While one of two backends was
  failing, a handful of retries plus the circuit breaker (and then the
  health checks) kept client-visible errors at 0% across all three runs. The
  breaker recovered through a single half-open probe.
* **The distributed rate limit is correct** across two gateway instances.

## Earlier optimization (separate measurement)

Profiling showed a failed `import sniffio` on every httpcore lock creation.
Adding the dependency raised uncached throughput from 1,068 to 1,386 req/s
(median of 3 × 20 s runs at 10 VUs; `benchmarks/results/profiling/`). The
results above include that fix.

## Limitations

Single machine, with all containers sharing one Docker VM. Single Uvicorn
worker. A mock upstream with near-zero work. Short runs (60–120 s, no soak
test). Closed-loop load measures throughput at saturation, not latency at a
fixed arrival rate.
