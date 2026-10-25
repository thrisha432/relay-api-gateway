"""Admission control: shed excess load fast instead of queueing into timeouts.

Regression test for the metastable failure found in the open-loop benchmark:
without a limit, overload made requests time out *inside* the gateway, the
breaker blamed the healthy upstream, and it stayed open while load persisted.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from helpers import build_config, gateway_client


async def test_requests_beyond_the_limit_are_shed_immediately(upstream_a) -> None:
    config = build_config([upstream_a.url], server={"max_in_flight": 5})
    async with gateway_client(config) as client:
        responses = await asyncio.gather(
            *(client.get("/api/catalog/x?delay_ms=300") for _ in range(20))
        )
        metrics = (await client.get("/metrics")).text
    codes = Counter(r.status_code for r in responses)
    assert codes == {200: 5, 503: 15}
    shed = [r for r in responses if r.status_code == 503]
    assert {r.json()["error"]["code"] for r in shed} == {"overloaded"}
    assert {r.headers["retry-after"] for r in shed} == {"1"}
    assert 'relay_load_shed_total{route="catalog"} 15.0' in metrics
    assert (await upstream_a.stats())["requests"] == {"GET": 5}


async def test_shedding_does_not_trip_the_breaker_or_block_health(upstream_a) -> None:
    config = build_config(
        [upstream_a.url],
        server={"max_in_flight": 2},
        route={"circuit_breaker": {"failure_threshold": 1}},
    )
    async with gateway_client(config) as client:
        slow = [asyncio.create_task(client.get("/api/catalog/x?delay_ms=300")) for _ in range(2)]
        await asyncio.sleep(0.05)
        shed = await client.get("/api/catalog/x")
        live = await client.get("/health/live")
        await asyncio.gather(*slow)
        breaker = client.app.state.gateway.breakers[("catalog", "u0")]
        after = await client.get("/api/catalog/x")
    assert shed.status_code == 503
    assert live.status_code == 200  # operational endpoints are never shed
    assert breaker.state == "closed"  # shedding is not an upstream failure
    assert after.status_code == 200


async def test_limit_zero_disables_admission_control(upstream_a) -> None:
    config = build_config([upstream_a.url], server={"max_in_flight": 0})
    async with gateway_client(config) as client:
        responses = await asyncio.gather(
            *(client.get("/api/catalog/x?delay_ms=100") for _ in range(30))
        )
    assert {r.status_code for r in responses} == {200}


async def test_event_loop_lag_is_exported(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        await asyncio.sleep(1.2)
        metrics = (await client.get("/metrics")).text
    line = next(ln for ln in metrics.splitlines() if ln.startswith("relay_event_loop_lag_seconds "))
    assert 0 <= float(line.split()[1]) < 0.5
