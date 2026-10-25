# Changelog

## v1.0.0: validated

Driven by an audit of v0.1 ([docs/AUDIT.md](docs/AUDIT.md)) and by what the
new benchmarks found.

### Fixed
- **Critical:** a connection-pool leak turned an overload spike into a
  permanent outage (httpcore race on queued waiters). Now structurally
  impossible: no external cancellation of httpx, plus
  `max_in_flight ≤ upstream_max_connections` enforced. (A16)
- Metastable collapse under overload: self-inflicted timeouts tripped
  breakers on healthy upstreams. (A17)
- Cache stampede: concurrent misses all went upstream. (A3)
- Request headers had no size limit; HTTP/1.1 without `Host` was accepted. (A1, A2)
- A malformed `Cache-Control: max-age` (no value) was cached for the full route TTL.

### Added
- Admission control: in-flight limit, bounded FIFO wait queue, cheap
  503 `overloaded`. Event-loop lag and pool-health metrics.
- Single-flight request coalescing for cache misses.
- Retry budget (aggregate cap on retries).
- `least_request` load balancing (routes around slow or hung instances). (A19)
- Benchmark matrix with NGINX baseline, open-loop sweep, scaling, ablation,
  stampede and brownout experiments; chaos timelines; A/B tool; run-validity guards.
- Tests: adversarial raw-socket security suite, Hypothesis stateful,
  differential and invariant tests, TCP chaos proxy, multi-process
  rate-limit and graceful-shutdown tests, regression tests for every
  benchmark-found bug.
- Quality gates: `mypy --strict`, `pip-audit`, mutation testing (mutmut), `scripts/verify.sh`.

## v0.1.0: initial implementation

Config-driven routing, round-robin with active health checks, Redis Lua
token bucket, custom LRU cache, circuit breakers, safe retries,
Prometheus/Grafana, Docker Compose, k6 benchmarks.
