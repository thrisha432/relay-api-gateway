"""Phase 4 exit gate: 429 + Retry-After, refill, concurrency, multiple
gateway instances sharing Redis, and the Redis outage policy."""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import AsyncExitStack

from helpers import build_config, gateway_client

REDIS = {"url": "redis://configured-but-injected:6379/0"}


def limited_config(upstream_url: str, capacity: int, rate: float, **top):
    return build_config(
        [upstream_url],
        route={"rate_limit": {"capacity": capacity, "refill_rate_per_second": rate}},
        redis={**REDIS, **top.pop("redis", {})},
        **top,
    )


async def test_exhausted_bucket_returns_429_with_retry_after(upstream_a, redis_factory) -> None:
    config = limited_config(upstream_a.url, capacity=3, rate=0.5)
    async with gateway_client(config, redis=redis_factory()) as client:
        responses = [await client.get("/api/catalog/x") for _ in range(4)]
        metrics = (await client.get("/metrics")).text
    assert [r.status_code for r in responses] == [200, 200, 200, 429]
    assert [r.headers["x-ratelimit-remaining"] for r in responses] == ["2", "1", "0", "0"]
    limited = responses[-1]
    assert limited.json()["error"]["code"] == "rate_limited"
    assert limited.headers["retry-after"] == "2"  # one token at 0.5/s
    assert limited.headers["x-ratelimit-limit"] == "3"
    assert (await upstream_a.stats())["requests"] == {"GET": 3}  # limited call never forwarded
    assert 'relay_rate_limit_decisions_total{decision="limited",route="catalog"} 1.0' in metrics


async def test_tokens_refill_over_time(upstream_a, redis_factory) -> None:
    config = limited_config(upstream_a.url, capacity=1, rate=10)
    async with gateway_client(config, redis=redis_factory()) as client:
        assert (await client.get("/api/catalog/x")).status_code == 200
        assert (await client.get("/api/catalog/x")).status_code == 429
        await asyncio.sleep(0.15)  # >= 1 token at 10/s
        assert (await client.get("/api/catalog/x")).status_code == 200


async def test_clients_are_limited_independently(upstream_a, redis_factory) -> None:
    # Trust the test transport's peer so X-Forwarded-For selects the client.
    config = limited_config(
        upstream_a.url, capacity=1, rate=0.01, server={"trusted_proxies": ["10.0.0.0/8"]}
    )
    async with gateway_client(config, redis=redis_factory()) as client:
        a1 = await client.get("/api/catalog/x", headers={"x-forwarded-for": "203.0.113.1"})
        a2 = await client.get("/api/catalog/x", headers={"x-forwarded-for": "203.0.113.1"})
        b1 = await client.get("/api/catalog/x", headers={"x-forwarded-for": "203.0.113.2"})
    assert (a1.status_code, a2.status_code, b1.status_code) == (200, 429, 200)


async def test_spoofed_forwarded_for_is_ignored_without_trusted_proxy(
    upstream_a, redis_factory
) -> None:
    config = limited_config(upstream_a.url, capacity=1, rate=0.01)
    async with gateway_client(config, redis=redis_factory()) as client:
        first = await client.get("/api/catalog/x", headers={"x-forwarded-for": "1.1.1.1"})
        second = await client.get("/api/catalog/x", headers={"x-forwarded-for": "2.2.2.2"})
    assert (first.status_code, second.status_code) == (200, 429)


async def test_concurrent_requests_never_exceed_capacity(upstream_a, redis_factory) -> None:
    config = limited_config(upstream_a.url, capacity=10, rate=0.001)
    async with gateway_client(config, redis=redis_factory()) as client:
        responses = await asyncio.gather(*(client.get("/api/catalog/x") for _ in range(60)))
    assert Counter(r.status_code for r in responses) == {200: 10, 429: 50}


async def test_two_gateway_instances_share_one_bucket(upstream_a, redis_factory) -> None:
    """Two independent gateway apps, each with its own Redis client, same server."""
    config = limited_config(upstream_a.url, capacity=6, rate=0.001)
    async with AsyncExitStack() as stack:
        gw1 = await stack.enter_async_context(gateway_client(config, redis=redis_factory()))
        gw2 = await stack.enter_async_context(gateway_client(config, redis=redis_factory()))
        sequential = [await (gw1 if i % 2 else gw2).get("/api/catalog/x") for i in range(8)]
        assert [r.status_code for r in sequential] == [200] * 6 + [429] * 2

    config = limited_config(upstream_a.url, capacity=15, rate=0.001)
    async with AsyncExitStack() as stack:
        shared = redis_factory()
        await shared.flushdb()
        gw1 = await stack.enter_async_context(gateway_client(config, redis=redis_factory()))
        gw2 = await stack.enter_async_context(gateway_client(config, redis=redis_factory()))
        concurrent = await asyncio.gather(
            *(gw.get("/api/catalog/x") for _ in range(30) for gw in (gw1, gw2))
        )
    assert Counter(r.status_code for r in concurrent) == {200: 15, 429: 45}


async def test_limit_is_checked_before_reading_the_body(upstream_a, redis_factory) -> None:
    config = limited_config(
        upstream_a.url, capacity=1, rate=0.01, server={"max_request_body_bytes": 10}
    )
    async with gateway_client(config, redis=redis_factory()) as client:
        await client.get("/api/catalog/x")
        response = await client.post("/api/catalog/x", content=b"x" * 100)
    assert response.status_code == 429  # not 413: limiter ran first


async def test_redis_outage_fails_closed_by_default(upstream_a, dead_redis) -> None:
    config = limited_config(
        upstream_a.url,
        capacity=10,
        rate=1,
        extra_routes=[
            {
                "id": "open",
                "path_prefix": "/api/open",
                "upstreams": [{"id": "a", "url": upstream_a.url}],
                "health_check": {"enabled": False},
            }
        ],
    )
    async with gateway_client(config, redis=dead_redis) as client:
        limited = await client.get("/api/catalog/x")
        unlimited = await client.get("/api/open/x")
        ready = await client.get("/health/ready")
        metrics = (await client.get("/metrics")).text
    assert limited.status_code == 503
    assert limited.json()["error"]["code"] == "rate_limiter_unavailable"
    assert unlimited.status_code == 200  # routes without a limit are unaffected
    assert ready.status_code == 503
    assert ready.json()["checks"]["redis"] is False
    assert 'relay_redis_errors_total{operation="rate_limit"} 1.0' in metrics
    assert 'relay_rate_limit_decisions_total{decision="fail_closed",route="catalog"} 1.0' in metrics
    assert (await upstream_a.stats())["requests"] == {"GET": 1}  # only /api/open
    await dead_redis.aclose()


async def test_redis_outage_fail_open_when_configured(upstream_a, dead_redis) -> None:
    config = limited_config(
        upstream_a.url, capacity=1, rate=0.01, redis={"fail_open_for_rate_limit": True}
    )
    async with gateway_client(config, redis=dead_redis) as client:
        responses = [await client.get("/api/catalog/x") for _ in range(3)]
        metrics = (await client.get("/metrics")).text
    assert [r.status_code for r in responses] == [200, 200, 200]
    assert "x-ratelimit-remaining" not in responses[0].headers
    assert 'relay_rate_limit_decisions_total{decision="fail_open",route="catalog"} 3.0' in metrics
    await dead_redis.aclose()


async def test_gateway_creates_and_closes_its_own_redis_client(upstream_a) -> None:
    config = limited_config(upstream_a.url, capacity=1, rate=1)
    async with gateway_client(config) as client:
        gateway = client.app.state.gateway
        assert gateway.redis is not None
    assert gateway.redis is None  # owned client was closed and released on shutdown
