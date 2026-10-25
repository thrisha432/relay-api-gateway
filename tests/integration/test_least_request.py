"""Least-request balancing vs round-robin with a slow or hung instance.

Found by the backend-hang chaos run: with round-robin, requests hung on one
instance held the gateway's admission slots for the whole attempt timeout,
so traffic that could have gone to the healthy instance was shed (503
overloaded) for ~1.5 s, until that instance's breaker opened.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from helpers import build_config, gateway_client


async def staggered(client, n: int, gap_s: float) -> list:
    tasks = []
    for _ in range(n):
        tasks.append(asyncio.create_task(client.get("/api/catalog/x")))
        await asyncio.sleep(gap_s)
    return await asyncio.gather(*tasks)


def config(upstreams, strategy: str, **server):
    return build_config(
        [u.url for u in upstreams],
        route={"load_balancing": strategy, "circuit_breaker": {"enabled": False}},
        server=server,
    )


async def test_least_request_avoids_a_slow_instance(upstream_a, upstream_b) -> None:
    await upstream_b.configure(delay_ms=300)
    results = {}
    for strategy in ("round_robin", "least_request"):
        async with gateway_client(config([upstream_a, upstream_b], strategy)) as client:
            responses = await staggered(client, 30, 0.01)
        results[strategy] = Counter(r.json()["instance"] for r in responses)
    assert results["round_robin"]["backend-b"] == 15  # blind rotation
    assert results["least_request"]["backend-b"] <= 3  # routed around the slow one


async def test_least_request_spreads_evenly_across_equal_instances(upstream_a, upstream_b) -> None:
    async with gateway_client(config([upstream_a, upstream_b], "least_request")) as client:
        responses = [await client.get("/api/catalog/x") for _ in range(20)]
    # Sequential requests: everyone has 0 outstanding, so round-robin breaks ties.
    assert Counter(r.json()["instance"] for r in responses) == {"backend-a": 10, "backend-b": 10}


async def test_hung_instance_cannot_exhaust_admission_slots(upstream_a, upstream_b) -> None:
    """The chaos finding, in miniature: limit 8, backend-b hangs past the timeout."""
    await upstream_b.configure(delay_ms=3000)
    shed = {}
    for strategy in ("round_robin", "least_request"):
        cfg = build_config(
            [upstream_a.url, upstream_b.url],
            route={
                "load_balancing": strategy,
                "circuit_breaker": {"enabled": False},
                "retries": {"max_attempts": 1},
                "timeouts": {
                    "connect_seconds": 0.5,
                    "attempt_seconds": 1.0,
                    "request_seconds": 1.0,
                },
            },
            server={"max_in_flight": 8, "admission_queue_timeout_seconds": 0.05},
        )
        async with gateway_client(cfg) as client:
            responses = await staggered(client, 60, 0.01)
        shed[strategy] = sum(r.status_code == 503 for r in responses)
    assert shed["round_robin"] > 0  # hung requests filled the slots
    assert shed["least_request"] == 0


async def test_in_flight_gauge_is_exported(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        slow = asyncio.create_task(client.get("/api/catalog/x?delay_ms=300"))
        await asyncio.sleep(0.1)
        during = (await client.get("/metrics")).text
        await slow
        after = (await client.get("/metrics")).text
    assert 'relay_upstream_in_flight{route="catalog",upstream="u0"} 1.0' in during
    assert 'relay_upstream_in_flight{route="catalog",upstream="u0"} 0.0' in after
