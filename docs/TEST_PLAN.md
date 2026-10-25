# Test Plan and Requirement Traceability

Run everything:

```bash
.venv/bin/pytest                                         # fakeredis only
docker run -d --rm --name relay-test-redis -p 127.0.0.1:6390:6379 redis:7.4-alpine
REDIS_URL=redis://127.0.0.1:6390/15 .venv/bin/pytest --cov   # + real Redis
```

* Mock upstreams are real Uvicorn servers on random localhost ports
  (`tests/conftest.py`). The gateway runs in-process with its lifespan via
  `httpx.ASGITransport`. One test also serves the gateway itself with
  Uvicorn over real sockets.
* Redis-dependent tests are parametrized over `fakeredis` and `real-redis`.
  The real variant carries the `redis` marker and is skipped when `REDIS_URL`
  is unset or unreachable. CI runs it against a `redis:7.4-alpine` service
  container. DB 15 is flushed before and after.
* Timing-sensitive tests poll for a condition (`helpers.wait_for`) instead of
  sleeping fixed amounts. Concurrency tests were run repeatedly (5+
  consecutive full passes) during development.

## Traceability (spec §11 → tests)

| Requirement | Tests |
|---|---|
| LRU get/put/update/eviction/capacity edge cases, invariants | `unit/test_lru.py` (deterministic cases, 20-seed model-based test vs `OrderedDict`, concurrent interleaving) |
| Route validation, longest prefix, boundaries, trailing slash, query, encoded segments | `unit/test_routing.py`, `unit/test_config.py`, `integration/test_proxy.py::test_encoded_path_segments_are_preserved` |
| Round-robin sequence, unhealthy exclusion, concurrent selection | `unit/test_load_balancer.py`, `integration/test_health_checks.py` |
| Token bucket math and Lua contract | `unit/test_rate_limiter.py` (deterministic time via `now_us`: burst, refill, cap, retry-after, clock skew, TTL, hashed keys, EVALSHA fallback) |
| Circuit-breaker transitions, probe limits, stale results, never stuck | `unit/test_circuit_breaker.py` (incl. 200-sequence property test) |
| Retry eligibility, backoff, deadline bounds | `unit/test_retry.py` |
| Header filtering, cache eligibility | `unit/test_headers.py`, `unit/test_cache_policy.py` |
| Proxy to two mock upstreams; method/body/path/query forwarding | `integration/test_proxy.py`, `integration/test_health_checks.py::test_round_robin_distribution_across_two_upstreams` |
| 404 for unknown route | `integration/test_proxy.py::test_unknown_route_returns_stable_json_404` |
| 429 on limit, refill | `integration/test_redis_rate_limit.py` |
| Rate limit shared across two gateway instances | `integration/test_redis_rate_limit.py::test_two_gateway_instances_share_one_bucket` |
| Redis outage policy (fail-closed default, fail-open option) | `integration/test_redis_rate_limit.py::test_redis_outage_*` |
| Cache hit/miss/expiry, key separation, unsafe bypass, concurrent access | `integration/test_cache.py` |
| Shared Redis cache across instances; Redis cache outage | `integration/test_failure_recovery.py::test_shared_redis_cache_*`, `::test_redis_cache_outage_degrades_to_miss` |
| Circuit breaker opens and recovers; half-open probe limit; cancellation | `integration/test_failure_recovery.py` |
| No unsafe-method retries; bounded attempts; deadline bounds | `integration/test_failure_recovery.py` |
| Timeout and unavailable-upstream responses | `integration/test_proxy.py` (504, 502), `integration/test_health_checks.py` (503) |
| Health checks remove and restore an upstream | `integration/test_health_checks.py::test_unhealthy_upstream_excluded_then_restored` |
| Shutdown cleans up background tasks and connections | `integration/test_health_checks.py::test_shutdown_cancels_health_tasks`, `integration/test_redis_rate_limit.py::test_gateway_creates_and_closes_its_own_redis_client` |
| Concurrent rate-limit requests never exceed capacity | `integration/test_redis_rate_limit.py::test_concurrent_requests_never_exceed_capacity` |
| Metrics exist under normal/failure traffic; bounded labels | `integration/test_metrics.py` |
| JSON 500 without leaking internals; logging format; CLI | `unit/test_observability.py` |

## Beyond example-based tests (v1)

| Technique | What it adds | Where |
|---|---|---|
| **Property-based (Hypothesis)** | Thousands of generated cases, shrunk to minimal counterexamples | `unit/test_properties.py` |
| Stateful testing | The LRU is driven by random operation sequences and compared with an `OrderedDict` model after every step | `TestLRUStateful` |
| **Differential testing** | The Lua token bucket (in Redis) and an independent Python reference agree on every step of random timelines, including clocks that jump backwards | `test_lua_bucket_matches_reference_model` |
| Invariant testing | For *any* arrival pattern: admitted ≤ capacity + rate × elapsed | `test_limiter_never_admits_more_than_capacity_plus_refill` |
| Model-checked routing | The router matches a brute-force longest-prefix reference; dot segments are rejected however encoded | `test_router_matches_reference_longest_prefix` |
| **Mutation testing (mutmut)** | Measures whether tests *detect* wrong code, not just execute it | `docs/QUALITY.md` |
| **Chaos / fault injection** | A TCP proxy that resets, black-holes, or truncates connections to Redis and upstreams, then heals them | `integration/test_chaos.py`, `tests/chaos_proxy.py` |
| **Multi-process** | 3 real `python -m relay` processes share one Redis bucket; SIGTERM shutdown verified | `integration/test_lifecycle.py` |
| **Adversarial security** | Raw-socket attacks against the real HTTP parser | `security/test_security.py`, SECURITY.md |
| **Regression for benchmark-found bugs** | The pool leak (A16) and metastable overload (A17) are reproduced and guarded | `integration/test_pool_leak.py`, `integration/test_admission.py` |

## Outside pytest

* `scripts/smoke_test.sh`: end-to-end checks against the Docker Compose stack.
* `benchmarks/`: k6 load, failure-injection, and two-gateway rate-limit
  scenarios (see `benchmarks/README.md`).

## Not covered (known gaps)

* HTTP request smuggling / fuzzing of the HTTP parser (delegated to Uvicorn).
* Multi-worker (single-process, multiple Uvicorn workers) deployments. Multiple *instances* are tested.
* Long-duration soak tests (memory growth over hours).
