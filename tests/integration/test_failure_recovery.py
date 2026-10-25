"""Phase 5 exit gate: safe retries only, bounded retry loops, and circuit
breakers that open, fail fast, probe, and recover (never get stuck)."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from contextlib import AsyncExitStack

from helpers import build_config, free_port_url, gateway_client

NO_BACKOFF = {"backoff_base_seconds": 0.001, "backoff_max_seconds": 0.002}


def requests_seen(stats: dict) -> int:
    return sum(stats["requests"].values())


async def test_get_is_retried_on_another_upstream(upstream_a, upstream_b) -> None:
    await upstream_a.configure(fail_status=503)
    async with gateway_client(build_config([upstream_a.url, upstream_b.url])) as client:
        response = await client.get("/api/catalog/x")
        metrics = (await client.get("/metrics")).text
    assert response.status_code == 200
    assert response.json()["instance"] == "backend-b"
    assert (await upstream_a.stats())["requests"] == {"GET": 1}
    assert 'relay_retries_total{reason="status",route="catalog"} 1.0' in metrics


async def test_retry_after_connection_failure(upstream_a) -> None:
    async with gateway_client(build_config([free_port_url(), upstream_a.url])) as client:
        response = await client.get("/api/catalog/x")
        metrics = (await client.get("/metrics")).text
    assert response.status_code == 200
    assert 'relay_retries_total{reason="connect_error",route="catalog"} 1.0' in metrics


async def test_unsafe_methods_are_never_retried(upstream_a, upstream_b) -> None:
    await upstream_a.configure(fail_status=503)
    config = build_config([upstream_a.url, upstream_b.url], route={"retries": {"max_attempts": 5}})
    async with gateway_client(config) as client:
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            response = await client.request(method, "/api/catalog/x", content=b"{}")
            assert response.status_code == 503, method
            # Skip backend-b so the next method starts on backend-a again.
            await client.get("/api/catalog/skip")
    stats_a, stats_b = await upstream_a.stats(), await upstream_b.stats()
    assert stats_a["requests"] == {"POST": 1, "PUT": 1, "PATCH": 1, "DELETE": 1}
    assert set(stats_b["requests"]) == {"GET"}  # no write ever reached backend-b


async def test_post_connection_failure_is_not_retried(upstream_a) -> None:
    async with gateway_client(build_config([free_port_url(), upstream_a.url])) as client:
        response = await client.post("/api/catalog/x", content=b"{}")
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_connection_error"
    assert requests_seen(await upstream_a.stats()) == 0


async def test_attempts_are_capped_and_exhaustion_is_reported(upstream_a, upstream_b) -> None:
    for upstream in (upstream_a, upstream_b):
        await upstream.configure(fail_status=503)
    config = build_config(
        [upstream_a.url, upstream_b.url],
        route={"retries": {"max_attempts": 3, **NO_BACKOFF}, "circuit_breaker": {"enabled": False}},
    )
    async with gateway_client(config) as client:
        response = await client.get("/api/catalog/x")
        metrics = (await client.get("/metrics")).text
    # The upstream's own 503 body is passed through after the last attempt.
    assert response.status_code == 503
    assert response.json() == {"error": "injected", "instance": "backend-a"}
    total = requests_seen(await upstream_a.stats()) + requests_seen(await upstream_b.stats())
    assert total == 3
    assert 'relay_retries_exhausted_total{route="catalog"} 1.0' in metrics


async def test_request_deadline_bounds_total_retry_time(upstream_a) -> None:
    await upstream_a.configure(delay_ms=2000)
    config = build_config(
        [upstream_a.url],
        route={
            "timeouts": {"connect_seconds": 0.1, "attempt_seconds": 0.25, "request_seconds": 0.6},
            "retries": {"max_attempts": 5, **NO_BACKOFF},
            "circuit_breaker": {"enabled": False},
        },
    )
    async with gateway_client(config) as client:
        start = time.perf_counter()
        response = await client.get("/api/catalog/x")
        elapsed = time.perf_counter() - start
    assert response.status_code == 504
    assert response.json()["error"]["code"] == "upstream_timeout"
    assert elapsed < 0.9  # bounded by request_seconds, not 5 x attempt_seconds
    assert requests_seen(await upstream_a.stats()) <= 3


async def test_breaker_opens_fails_fast_and_recovers(upstream_a) -> None:
    await upstream_a.configure(fail_status=503)
    config = build_config(
        [upstream_a.url],
        route={
            "retries": {"max_attempts": 1},
            "circuit_breaker": {"failure_threshold": 3, "recovery_timeout_seconds": 0.3},
        },
    )
    async with gateway_client(config) as client:
        gateway = client.app.state.gateway
        breaker = gateway.breakers[("catalog", "u0")]
        for _ in range(3):
            assert (await client.get("/api/catalog/x")).status_code == 503
        assert breaker.state == "open"

        rejected = await client.get("/api/catalog/x")
        assert rejected.status_code == 503
        assert rejected.json()["error"]["code"] == "circuit_open"
        assert requests_seen(await upstream_a.stats()) == 3  # failed fast, upstream untouched

        await upstream_a.configure(fail_status=0)
        await asyncio.sleep(0.35)
        assert (await client.get("/api/catalog/x")).status_code == 200  # half-open probe
        assert breaker.state == "closed"
        assert (await client.get("/api/catalog/x")).status_code == 200
        metrics = (await client.get("/metrics")).text
    for transition in (
        'from_state="closed",route="catalog",to_state="open"',
        'from_state="open",route="catalog",to_state="half_open"',
        'from_state="half_open",route="catalog",to_state="closed"',
    ):
        assert transition in metrics
    assert 'relay_circuit_breaker_state{route="catalog",upstream="u0"} 0.0' in metrics


async def test_failed_probe_reopens_breaker(upstream_a) -> None:
    await upstream_a.configure(fail_status=502)
    config = build_config(
        [upstream_a.url],
        route={
            "retries": {"max_attempts": 1},
            "circuit_breaker": {"failure_threshold": 1, "recovery_timeout_seconds": 0.1},
        },
    )
    async with gateway_client(config) as client:
        breaker = client.app.state.gateway.breakers[("catalog", "u0")]
        await client.get("/api/catalog/x")
        await asyncio.sleep(0.15)
        assert (await client.get("/api/catalog/x")).status_code == 502  # probe fails
        assert breaker.state == "open"


async def test_half_open_admits_only_limited_concurrent_probes(upstream_a) -> None:
    await upstream_a.configure(fail_status=503)
    config = build_config(
        [upstream_a.url],
        route={
            "retries": {"max_attempts": 1},
            "circuit_breaker": {
                "failure_threshold": 1,
                "recovery_timeout_seconds": 0.1,
                "half_open_max_probes": 1,
            },
        },
    )
    async with gateway_client(config) as client:
        await client.get("/api/catalog/x")
        await upstream_a.configure(fail_status=0, delay_ms=200)  # recovered but slow
        await asyncio.sleep(0.15)
        before = requests_seen(await upstream_a.stats())
        responses = await asyncio.gather(*(client.get("/api/catalog/x") for _ in range(20)))
    codes = Counter(r.status_code for r in responses)
    assert codes == {200: 1, 503: 19}
    assert requests_seen(await upstream_a.stats()) - before == 1


async def test_client_errors_do_not_open_the_breaker(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"circuit_breaker": {"failure_threshold": 2}})
    async with gateway_client(config) as client:
        for status in (400, 404, 409, 429, 500):
            await client.get(f"/api/catalog/x?status={status}")
        breaker = client.app.state.gateway.breakers[("catalog", "u0")]
    assert breaker.state == "closed"


async def test_open_breaker_fails_over_to_other_upstream(upstream_a, upstream_b) -> None:
    await upstream_a.configure(fail_status=503)
    config = build_config(
        [upstream_a.url, upstream_b.url],
        route={
            "retries": {"max_attempts": 1},
            "circuit_breaker": {"failure_threshold": 1, "recovery_timeout_seconds": 60},
        },
    )
    async with gateway_client(config) as client:
        assert (await client.get("/api/catalog/x")).status_code == 503  # opens a's breaker
        instances = [(await client.get("/api/catalog/x")).json()["instance"] for _ in range(4)]
    assert instances == ["backend-b"] * 4


async def test_cancelled_probe_releases_its_slot(upstream_a) -> None:
    """A client that gives up mid-probe must not leave HALF_OPEN stuck."""
    await upstream_a.configure(fail_status=503)
    config = build_config(
        [upstream_a.url],
        route={
            "retries": {"max_attempts": 1},
            "circuit_breaker": {"failure_threshold": 1, "recovery_timeout_seconds": 0.05},
        },
    )
    async with gateway_client(config) as client:
        breaker = client.app.state.gateway.breakers[("catalog", "u0")]
        await client.get("/api/catalog/x")
        await upstream_a.configure(fail_status=0, delay_ms=1000)
        await asyncio.sleep(0.1)
        probe = asyncio.create_task(client.get("/api/catalog/x"))
        await asyncio.sleep(0.1)
        assert breaker.probes_in_flight == 1
        probe.cancel()
        await asyncio.gather(probe, return_exceptions=True)
        assert breaker.probes_in_flight == 0
        await upstream_a.configure(delay_ms=0)
        assert (await client.get("/api/catalog/x")).status_code == 200
        assert breaker.state == "closed"


async def test_shared_redis_cache_across_gateway_instances(upstream_a, redis_factory) -> None:
    config = build_config(
        [upstream_a.url],
        route={"cache": {"enabled": True, "ttl_seconds": 30}},
        cache={"backend": "redis"},
        redis={"url": "redis://injected:6379/0"},
    )
    async with AsyncExitStack() as stack:
        gw1 = await stack.enter_async_context(gateway_client(config, redis=redis_factory()))
        gw2 = await stack.enter_async_context(gateway_client(config, redis=redis_factory()))
        first = await gw1.get("/api/catalog/shared?x=1")
        second = await gw2.get("/api/catalog/shared?x=1")
    assert first.headers["x-relay-cache"] == "MISS"
    assert second.headers["x-relay-cache"] == "HIT"
    assert second.json() == first.json()
    assert requests_seen(await upstream_a.stats()) == 1


async def test_redis_cache_outage_degrades_to_miss(upstream_a, dead_redis) -> None:
    config = build_config(
        [upstream_a.url],
        route={"cache": {"enabled": True}},
        cache={"backend": "redis"},
        redis={"url": "redis://injected:6379/0"},
    )
    async with gateway_client(config, redis=dead_redis) as client:
        responses = [await client.get("/api/catalog/x") for _ in range(2)]
        metrics = (await client.get("/metrics")).text
    assert [r.status_code for r in responses] == [200, 200]
    assert [r.headers["x-relay-cache"] for r in responses] == ["MISS", "MISS"]
    assert 'relay_redis_errors_total{operation="cache_get"} 2.0' in metrics
    await dead_redis.aclose()
