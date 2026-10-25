# Demo Script (3–5 minutes)

Prerequisites: Docker Desktop running; commands run from the repo root.

## 1. Start the stack (30 s)

```bash
docker compose up -d --build
scripts/smoke_test.sh          # waits for readiness, then runs end-to-end checks
```

Say: "Relay sits in front of two mock services. Redis holds the rate-limit
buckets, and Prometheus and Grafana watch the gateway. Every port is bound to
localhost."

## 2. Two upstreams behind one entry point (30 s)

```bash
for i in 1 2 3 4; do curl -s localhost:8080/api/bench-ha/items | jq -r .instance; done
# backend-a, backend-b, backend-a, backend-b
curl -s localhost:8080/__relay/routes | jq '.routes[] | {id, upstreams}'
```

Say: "Round-robin over healthy instances. The route table shows health and
breaker state, but never the upstream URLs."

## 3. Cache hit (30 s)

```bash
curl -si localhost:8080/api/catalog/items?id=7 | grep -i -E 'x-relay-cache|x-ratelimit'
curl -si localhost:8080/api/catalog/items?id=7 | grep -i -E 'x-relay-cache|age'
curl -si -H 'Authorization: Bearer x' localhost:8080/api/catalog/items?id=7 | grep -i x-relay-cache
```

Say: "MISS, then HIT with an Age header. A credentialed request bypasses the
cache, because a response for one user must never be served to another."

## 4. Rate limiting (30 s)

```bash
for i in $(seq 1 8); do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/api/orders/x; done; echo
curl -si localhost:8080/api/orders/x | grep -i -E '^HTTP|retry-after'
```

Say: "`/api/orders` allows a burst of 5, refilling at 1 per second. The check,
refill, and consume happen in one Lua script, so it stays atomic even with
several gateways."

## 5. Failure: health checks and the circuit breaker (60 s)

```bash
curl -s -XPOST localhost:8002/__mock/config -d '{"fail_status": 503, "healthy": false}'
for i in $(seq 1 10); do curl -s localhost:8080/api/bench-ha/items | jq -r .instance; done   # all backend-a
curl -s localhost:8080/__relay/routes | jq '.routes[] | select(.id=="bench-ha") | .upstreams'
curl -s -XPOST localhost:8002/__mock/reset      # recover
```

Say: "Backend-b starts failing. The first failing GETs are retried on
backend-a, so clients see no errors. The breaker opens and fails fast, and
health checks take the instance out of rotation. After recovery, a half-open
probe closes the breaker."

## 6. Grafana (45 s)

Open <http://localhost:3000> (anonymous viewer; dashboard "Relay API Gateway").
Optionally run load in a second terminal while you talk:

```bash
docker compose --profile bench run --rm -e VUS=10 -e DURATION=60s -e MODE=cached k6 run /scripts/gateway.js
```

Point at request rate, the p50/p95/p99 latency, cache hit ratio, rate-limit
decisions, upstream health, and circuit-breaker state.

## 7. Benchmarks, honestly (30 s)

Open `docs/BENCHMARK_REPORT.md`. Say: "Uncached requests through the gateway
are slower than going direct. That overhead is the cost of a Python proxy hop,
and the report shows exactly how much. Caching helps repeated GETs. During
failure injection, retries and the breaker kept client errors at the measured
rate shown. These are laptop numbers, not capacity claims."

## 8. Tests (30 s)

```bash
.venv/bin/pytest -q tests/unit/test_lru.py tests/integration/test_failure_recovery.py
```

Show `test_half_open_admits_only_limited_concurrent_probes` and
`test_two_gateway_instances_share_one_bucket`.

## Clean up

```bash
docker compose --profile multi --profile bench down
```
