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
    # 300 ms upstream > 100 ms queue timeout: the 15 extra requests wait, then are shed.
    assert 'relay_load_shed_total{reason="queue_timeout",route="catalog"} 15.0' in metrics
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


async def test_queued_requests_are_served_when_a_slot_frees(upstream_a) -> None:
    """Short bursts above the limit wait briefly instead of failing."""
    config = build_config(
        [upstream_a.url],
        server={"max_in_flight": 2, "admission_queue_timeout_seconds": 1.0},
    )
    async with gateway_client(config) as client:
        responses = await asyncio.gather(
            *(client.get("/api/catalog/x?delay_ms=50") for _ in range(10))
        )
    assert {r.status_code for r in responses} == {200}
    assert (await upstream_a.stats())["requests"] == {"GET": 10}


async def test_full_queue_sheds_immediately(upstream_a) -> None:
    config = build_config(
        [upstream_a.url],
        server={
            "max_in_flight": 1,
            "admission_max_queued": 2,
            "admission_queue_timeout_seconds": 2.0,
        },
    )
    async with gateway_client(config) as client:
        loop = asyncio.get_running_loop()
        tasks = [asyncio.create_task(client.get("/api/catalog/x?delay_ms=300")) for _ in range(3)]
        await asyncio.sleep(0.05)  # 1 in flight + 2 queued: the queue is full
        start = loop.time()
        shed = await client.get("/api/catalog/x")
        shed_after = loop.time() - start
        served = await asyncio.gather(*tasks)
        metrics = (await client.get("/metrics")).text
    assert shed.status_code == 503
    assert shed_after < 0.1  # not parked: rejected at once
    assert [r.status_code for r in served] == [200, 200, 200]
    assert 'reason="queue_full"' in metrics


async def test_cancelled_waiter_does_not_leak_a_slot(upstream_a) -> None:
    config = build_config(
        [upstream_a.url],
        server={"max_in_flight": 1, "admission_queue_timeout_seconds": 5.0},
    )
    async with gateway_client(config) as client:
        holder = asyncio.create_task(client.get("/api/catalog/x?delay_ms=200"))
        await asyncio.sleep(0.05)
        waiter = asyncio.create_task(client.get("/api/catalog/x"))
        await asyncio.sleep(0.05)
        waiter.cancel()  # client gives up while queued
        await asyncio.gather(waiter, return_exceptions=True)
        assert (await holder).status_code == 200
        # The single slot must be free again: sequential requests all succeed.
        after = [(await client.get("/api/catalog/x")).status_code for _ in range(3)]
        admission = next(
            m for m in _middlewares(client.app) if type(m).__name__ == "AdmissionMiddleware"
        )
    assert after == [200, 200, 200]
    assert admission.in_flight == 0
    assert admission.queued == 0


def _middlewares(app):
    node = app.middleware_stack
    while node is not None:
        yield node
        node = getattr(node, "app", None)
