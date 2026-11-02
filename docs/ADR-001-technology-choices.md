# ADR-001: Technology choices

**Status:** accepted · **Context:** portfolio-grade async HTTP gateway that
runs locally at $0 and can be understood end to end.

## Decisions

| Area | Choice | Why | Alternatives considered |
|---|---|---|---|
| Language/runtime | Python 3.12+ asyncio | Spec requirement; async I/O suits a proxy that mostly waits on sockets. PEP 695 generics used for the LRU. | Go (faster, but outside scope) |
| ASGI framework | FastAPI on Starlette, with the proxy mounted as a raw ASGI endpoint | FastAPI gives the operational endpoints; the catch-all is a raw ASGI app so *every* HTTP method reaches the gateway, which answers 405 with its own JSON contract. (Starlette 1.x turns `methods=None` on a function endpoint into GET/HEAD only.) OpenAPI/docs routes disabled so they cannot shadow user routes. | Plain Starlette (slightly faster; FastAPI kept for spec alignment) |
| Server | Uvicorn with uvloop + httptools (`uvicorn[standard]`) | Standard, fast ASGI server; graceful shutdown. | Hypercorn, Granian |
| Upstream client | HTTPX `AsyncClient`, one pooled instance | Spec requirement; explicit timeouts; raw streaming (`aiter_raw`) for byte-exact passthrough. Measured cost: pool bookkeeping dominates CPU (see ARCHITECTURE.md). | aiohttp (typically faster; would be the next experiment) |
| Shared state | Redis + Lua | Atomic multi-step token bucket in one round trip; server-side clock. | `INCR`+`EXPIRE` fixed window (simpler, burstier); `WATCH`/`MULTI` (more round trips, retries under contention) |
| Rate-limit algorithm | Token bucket | Allows controlled bursts (`capacity`) with a smooth long-run rate; O(1) state per client. | Sliding-window log (memory per request), fixed window (boundary bursts) |
| Local cache | Custom LRU (dict + doubly linked list with sentinels) | Spec requirement; O(1) operations with explicit, testable invariants. | `OrderedDict` (used as the test oracle), `functools.lru_cache` (wrong abstraction: function memoization, no TTL) |
| Config | YAML + Pydantic v2 (`extra="forbid"`, frozen models) | Fail-fast validation with field-level messages; immutable config at runtime. | TOML, env-only |
| Metrics | `prometheus-client`, per-app registry | Standard; per-app registry lets tests run several gateways in one process. | OpenTelemetry (more moving parts) |
| Tests | pytest + pytest-asyncio; real Uvicorn mock upstreams on ephemeral ports; fakeredis[lua] **and** real Redis | Real sockets catch timeout and connection behavior; fakeredis keeps the suite runnable anywhere, and the same tests run against a real Redis when `REDIS_URL` is set (CI uses a service container). | Pure mocks (miss real I/O behavior) |
| Load testing | k6 in Docker on the same network as the services | Repeatable, scriptable, no local install needed. | wrk, Locust |
| Lint/format | Ruff | One fast tool for both. | flake8 + black + isort |

## Consequences

* Single-process, single-loop design: no locks needed, but one core caps
  throughput. Scaling out means multiple workers or instances, each with its
  own LRU, breakers and health state.
* Responses are buffered (capped), not streamed: simpler and testable, but
  not suitable for large downloads or server-sent events.
