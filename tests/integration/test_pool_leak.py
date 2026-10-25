"""Regression tests for audit finding A16: leaked upstream pool connections.

Found by the open-loop benchmark: after an overload spike every route returned
504 forever while the backend answered directly in 1 ms.

Mechanism (httpcore 1.0.9, the latest release): while request R waits in the
pool queue, another task creates connection C and assigns it to R. If R's wait
then ends with an exception (its own PoolTimeout, or an external cancel such
as asyncio.timeout) before R resumes, R leaves the queue but C is never
dialed, so it is never closed, expired, idle or available: one pool slot is
lost permanently.

Relay's guarantees:
1. No external cancellation of httpx (per-phase httpx timeouts + a cooperative
   attempt budget instead).
2. ``max_in_flight <= upstream_max_connections`` (validated): an admitted
   request always gets a connection in its own synchronous call, so it never
   waits in the pool queue, which is where the race lives.
"""

from __future__ import annotations

import asyncio

import pytest

from helpers import build_config, gateway_client
from relay.config import ConfigError

TIGHT = {
    "timeouts": {"connect_seconds": 0.2, "attempt_seconds": 0.2, "request_seconds": 0.5},
    "retries": {"max_attempts": 1},
    "circuit_breaker": {"enabled": False},
}


async def test_repeated_overload_never_leaks_pool_connections(upstream_a) -> None:
    config = build_config(
        [upstream_a.url],
        server={"upstream_max_connections": 4, "max_in_flight": 4},
        route=TIGHT,
    )
    async with gateway_client(config) as client:
        for _ in range(5):  # 5 overload spikes of 5x the limit, all timing out
            spike = await asyncio.gather(
                *(client.get("/api/catalog/x?delay_ms=1000") for _ in range(20))
            )
            assert {r.status_code for r in spike} <= {503, 504}
        await asyncio.sleep(1.2)
        after = [(await client.get("/api/catalog/x")).status_code for _ in range(5)]
        metrics = (await client.get("/metrics")).text
    assert after == [200] * 5
    assert "relay_upstream_pool_unstarted_connections 0.0" in metrics


async def test_attempt_budget_is_enforced_without_cancelling_httpx(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url], route=TIGHT)) as client:
        loop = asyncio.get_running_loop()
        start = loop.time()
        response = await client.get("/api/catalog/x?delay_ms=2000")
        elapsed = loop.time() - start
    assert response.status_code == 504
    assert elapsed < 0.6  # httpx read timeout = attempt budget (0.2 s)


def test_admission_limit_cannot_exceed_pool_size() -> None:
    with pytest.raises(ConfigError, match=r"must not exceed server\.upstream_max_connections"):
        build_config(["http://a:1"], server={"upstream_max_connections": 10, "max_in_flight": 11})


async def test_disabling_admission_control_logs_a_warning(upstream_a, relay_logs) -> None:
    config = build_config([upstream_a.url], server={"max_in_flight": 0})
    async with gateway_client(config):
        pass
    assert "admission_control_disabled" in relay_logs.text
