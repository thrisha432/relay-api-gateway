"""Regression test for audit finding A16: timed-out attempts leaked pool connections.

Found by the open-loop benchmark: after an overload spike Relay returned 504
on every route forever, while the backend answered directly in 1 ms. Cause:
Relay cancelled httpx with asyncio.timeout. When cancellation landed after the
pool had assigned a freshly created connection to a waiting request, but before
that request dialed, httpcore left the connection in CONNECTING state. It was
never idle, available, or expired, so it was never reclaimed, and each one
permanently consumed a pool slot.
"""

from __future__ import annotations

import asyncio

from helpers import build_config, gateway_client

TIGHT = {
    "timeouts": {"connect_seconds": 0.2, "attempt_seconds": 0.2, "request_seconds": 0.5},
    "retries": {"max_attempts": 1},
    "circuit_breaker": {"enabled": False},
}


async def test_timeouts_under_pool_contention_do_not_leak_connections(upstream_a) -> None:
    config = build_config(
        [upstream_a.url],
        server={"upstream_max_connections": 4, "max_in_flight": 0},
        route=TIGHT,
    )
    async with gateway_client(config) as client:
        assert (await client.get("/api/catalog/x")).status_code == 200
        # 20 concurrent slow requests against a 4-connection pool: most wait
        # in the pool queue and every attempt hits its timeout.
        slow = await asyncio.gather(
            *(client.get("/api/catalog/x?delay_ms=1000") for _ in range(20))
        )
        assert {r.status_code for r in slow} == {504}
        await asyncio.sleep(1.2)  # let the upstream finish the abandoned work
        after = [(await client.get("/api/catalog/x")).status_code for _ in range(5)]
        pool = client.app.state.gateway.client._transport._pool
        stuck = [c for c in pool.connections if "CONNECTING" in repr(c)]
    assert after == [200] * 5  # before the fix: [504] * 5, forever
    assert stuck == []


async def test_attempt_budget_is_still_enforced_without_cancellation(upstream_a) -> None:
    config = build_config([upstream_a.url], route=TIGHT)
    async with gateway_client(config) as client:
        loop = asyncio.get_running_loop()
        start = loop.time()
        response = await client.get("/api/catalog/x?delay_ms=2000")
        elapsed = loop.time() - start
    assert response.status_code == 504
    assert elapsed < 0.6  # httpx read timeout = attempt budget (0.2 s)
