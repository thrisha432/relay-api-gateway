# Relay: an asynchronous Python API gateway

[![CI](https://github.com/OWNER/relay-api-gateway/actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)

Relay is a configuration-driven HTTP API gateway built on asyncio. It sits
between clients and a fixed set of backend services and adds the
cross-cutting reliability features gateways normally provide: health-aware
load balancing, distributed rate limiting, response caching, circuit
breakers, safe retries, and Prometheus/Grafana observability. Every behavior
is tested, and every performance number is backed by saved benchmark output.

> **Portfolio project.** Relay is built and tested carefully, but it is a
> local learning project, **not production-certified infrastructure**. See
> [SECURITY.md](docs/SECURITY.md).

## Features

| Feature | How |
|---|---|
| Config-driven routing | Longest-prefix match on segment boundaries; upstreams only from validated YAML (no SSRF) |
| Load balancing | Round-robin over currently healthy instances, with failover order |
| Active health checks | One lifespan-managed task per route; consecutive-threshold hysteresis |
| Distributed rate limiting | Token bucket in an **atomic Redis Lua script** using Redis server time; 429 + `Retry-After`; explicit fail-closed outage policy |
| Custom LRU cache | Hash map + doubly linked list with sentinels, O(1) average; invariant-checked |
| Safe response caching | GET only; never caches credentialed, cookie-setting, `private`/`no-store` responses; local LRU or shared Redis backend |
| Circuit breakers | Per upstream; closed / open / half-open with bounded concurrent probes; cannot get stuck |
| Timeouts and retries | Connect / per-attempt / whole-request deadlines; retries only GET/HEAD/OPTIONS with full-jitter backoff |
| Observability | Prometheus metrics with bounded labels, structured JSON access logs, a provisioned Grafana dashboard |
| Proxy correctness | Hop-by-hop header stripping, byte-exact headers, encoded paths preserved, request/response size caps, stable JSON errors |

## Architecture

```text
 client / k6
     │
     ▼
 ┌──────────────────────── Relay (one asyncio event loop) ────────────────────────┐
 │ validate path ─▶ route match ─▶ rate limit ─▶ read body ─▶ cache lookup (GET)  │
 │       ─▶ healthy upstream (round-robin) ─▶ circuit breaker ─▶ forward          │
 │          (deadline + safe retries) ─▶ cache fill ─▶ response (hop-by-hop out)  │
 │ metrics + JSON access log on every request                                     │
 └───────────┬──────────────────────────────────────────────────┬─────────────────┘
             │ token buckets, shared cache                      │ HTTP (pooled httpx)
             ▼                                                  ▼
          Redis                                        backend-a   backend-b
             Prometheus scrapes /metrics ──▶ Grafana dashboard
```

Details: [ARCHITECTURE.md](docs/ARCHITECTURE.md) · API, errors and config:
[API_AND_CONFIG.md](docs/API_AND_CONFIG.md) · Decisions:
[ADR-001](docs/ADR-001-technology-choices.md)

## Tech stack

Python 3.12+ · FastAPI/Starlette · Uvicorn (uvloop, httptools) · HTTPX ·
redis-py (asyncio) + Lua · Pydantic v2 + PyYAML · prometheus-client ·
pytest, pytest-asyncio, fakeredis · Ruff · Docker Compose · Prometheus ·
Grafana · k6 · GitHub Actions

## Quick start (Docker, everything included)

Prerequisites: Docker Desktop (or Docker Engine with Compose v2).

```bash
git clone https://github.com/OWNER/relay-api-gateway.git && cd relay-api-gateway
docker compose up -d --build
scripts/smoke_test.sh            # waits for readiness, then runs end-to-end checks
```

| URL | What |
|---|---|
| <http://localhost:8080> | Relay gateway |
| <http://localhost:3000> | Grafana, dashboard "Relay API Gateway" (anonymous read-only) |
| <http://localhost:9090> | Prometheus |
| <http://localhost:8001>, <http://localhost:8002> | Mock backends (for direct comparison and failure injection) |

All ports bind to `127.0.0.1`. Stop with
`docker compose --profile multi --profile bench down`.

### Try it

```bash
# round-robin across two backends
for i in 1 2 3 4; do curl -s localhost:8080/api/bench-ha/items | jq -r .instance; done

# cache: MISS then HIT
curl -si 'localhost:8080/api/catalog/items?id=1' | grep -i x-relay-cache
curl -si 'localhost:8080/api/catalog/items?id=1' | grep -i -E 'x-relay-cache|^age'

# rate limit: /api/orders allows a burst of 5, then 1 request/s
for i in $(seq 1 8); do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/api/orders/x; done; echo

# failure injection: backend-b starts failing; GETs are retried on backend-a
curl -s -XPOST localhost:8002/__mock/config -d '{"fail_status":503,"healthy":false}'
curl -s localhost:8080/__relay/routes | jq '.routes[] | select(.id=="bench-ha")'
curl -s -XPOST localhost:8002/__mock/reset

# errors use one stable JSON contract
curl -s localhost:8080/nope
# {"error":{"code":"route_not_found","message":"No configured route matches this path.","request_id":"..."}}
```

A full walkthrough is in [DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md).

## Local development (no Docker needed for most tests)

Prerequisites: Python 3.12+.

```bash
scripts/dev.sh setup        # .venv + pinned dependencies (requirements-dev.lock)
scripts/dev.sh test         # full suite; Redis tests use fakeredis
scripts/dev.sh lint         # ruff check + format check
```

To also run every Redis test against a real Redis (as CI does):

```bash
scripts/dev.sh redis                          # throwaway redis:7.4 on 127.0.0.1:6390
REDIS_URL=redis://127.0.0.1:6390/15 .venv/bin/pytest --cov
```

Run the gateway on its own: `RELAY_CONFIG=path/to/routes.yaml .venv/bin/relay`.
Validate a config file: `.venv/bin/relay --config config/routes.yaml --check`.

### Configuration example

```yaml
redis:
  url: "redis://redis:6379/0"          # or RELAY_REDIS_URL; secrets stay out of YAML
defaults:
  retries: { max_attempts: 2, retry_methods: [GET, HEAD, OPTIONS] }
  circuit_breaker: { failure_threshold: 5, recovery_timeout_seconds: 10 }
routes:
  - id: catalog
    path_prefix: /api/catalog
    upstreams:
      - { id: catalog-a, url: "http://backend-a:8001" }
      - { id: catalog-b, url: "http://backend-b:8002" }
    rate_limit: { capacity: 60, refill_rate_per_second: 10 }
    cache: { enabled: true, ttl_seconds: 10 }
```

Invalid config fails at startup with the exact field and reason. For example,
putting `POST` in `retry_methods` is rejected, because Relay never retries
writes. Full reference: [API_AND_CONFIG.md](docs/API_AND_CONFIG.md).

## Testing

```bash
REDIS_URL=redis://127.0.0.1:6390/15 .venv/bin/pytest --cov
```

* **237 tests**, covering unit, integration over real sockets, concurrency,
  and failure injection. Every Redis test runs against both fakeredis and a
  real Redis 7.4.
* Measured branch coverage for `relay` + `mock_backend`: **96%**.
* Highlights: a model-based LRU test against `OrderedDict`; 60 concurrent
  requests admitted exactly at bucket capacity, including across two gateway
  instances; a half-open breaker admitting exactly 1 of 20 concurrent probes;
  a cancelled probe releasing its slot; POST/PUT/PATCH/DELETE never retried;
  metric labels never containing paths, queries or IPs.
* The requirement-to-test mapping is in [TEST_PLAN.md](docs/TEST_PLAN.md).

## Benchmarks (measured)

k6 in Docker on an Apple M4 Pro laptop. Median of 3 runs; one Uvicorn worker.
Full method, environment and ranges are in the
[benchmark report](docs/BENCHMARK_REPORT.md), with raw output in
[`benchmarks/results/20261025T025934Z/`](benchmarks/results/20261025T025934Z/SUMMARY.md).

| Scenario (10 VUs, 60 s) | Requests/s | p50 | p95 | p99 |
|---|---|---|---|---|
| Direct to backend | 19,380 | 0.43 ms | 0.87 ms | 1.04 ms |
| Through Relay, uncached | 1,402 | 5.38 ms | 14.50 ms | 41.22 ms |
| Through Relay, cached (99.9% hits) | 6,960 | 1.23 ms | 2.50 ms | 2.79 ms |

* **Overhead is real:** uncached requests pay for a Python proxy hop, and
  Relay is CPU-bound on one core. The mock backend does almost no work, so
  this is a worst-case ratio.
* **Caching** gives about 5× the uncached throughput for repeated GETs.
* **Failure injection** (one of two backends failing for 30 s): **0.000%**
  client-visible errors in all 3 runs, with 7 retries and the breaker going
  closed → open → half-open → closed.
* **Two gateways sharing Redis** admitted 34 requests per run against a
  ceiling of 35.
* **A profiling-driven fix** (a missing `sniffio` caused a failed import on
  every request) raised uncached throughput by about 30%.

Reproduce: `scripts/run_benchmarks.sh` (~35 min) or see
[benchmarks/README.md](benchmarks/README.md).

## Monitoring

`docker compose up -d` provisions Prometheus (scraping `/metrics` every 5 s)
and Grafana with the **Relay API Gateway** dashboard: request rate, 5xx
error rate, p50/p95/p99 latency, cache hit ratio and lookups, rate-limit
decisions, upstream health, circuit-breaker state, retries, Redis errors, and
in-flight requests. The dashboard JSON is generated by
`scripts/build_dashboard.py`.

## Limitations

* A single process on one event loop (one CPU core). Multiple workers would
  each have their own LRU, breakers and health state.
* Responses are **buffered** (capped at 10 MiB by default), not streamed. No
  WebSockets, HTTP/2 upstreams, or gRPC.
* Caching implements a documented, conservative subset of HTTP caching. No
  revalidation, `Expires`, or request coalescing.
* No TLS termination or client authentication; local demo only.

## Future improvements

Single-flight request coalescing for cache misses · streaming responses ·
a retry budget · per-route header allowlists · shared/outlier-based ejection
across instances · comparing aiohttp as the upstream client.

## Project docs

[Architecture](docs/ARCHITECTURE.md) · [API & config](docs/API_AND_CONFIG.md) ·
[Security](docs/SECURITY.md) · [Test plan](docs/TEST_PLAN.md) ·
[Benchmark report](docs/BENCHMARK_REPORT.md) · [Demo script](docs/DEMO_SCRIPT.md) ·
[Interview guide](docs/INTERVIEW_GUIDE.md) · [ADR-001](docs/ADR-001-technology-choices.md) ·
[Implementation status](docs/IMPLEMENTATION_STATUS.md) · [Original spec](PROJECT_SPEC.md)

## License

MIT. See [LICENSE](LICENSE).
