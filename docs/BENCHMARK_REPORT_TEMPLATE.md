# Benchmark Report: <date>

> Fill this in only from files in `benchmarks/results/<timestamp>/`. Never
> type numbers that were not measured.

## Environment
Paste `environment.txt`: machine, OS, Docker VM resources, Python and
dependency versions, k6 version, topology.

## Method
* Scenarios: direct baseline, gateway uncached, gateway cached
  (cache-friendly key set), failure injection, two-gateway rate limit.
* Profiles: baseline (10 VUs, 60 s), load (50 VUs, 120 s); closed loop, no
  think time.
* Runs per scenario: N (≥ 3), interleaved; reported as median (min–max).

## Results
Paste the tables from `SUMMARY.md`.

## Interpretation
* Gateway overhead for uncached requests (and why).
* Effect of caching on repeated GETs.
* Behavior under failure: client-visible error rate, retries, breaker transitions.
* Distributed rate-limit correctness.

## Limitations
Single laptop; all containers share the CPU; single Uvicorn worker; mock
upstream does no real work; numbers are not production capacity claims.
