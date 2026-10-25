"""Network-fault injection with a TCP chaos proxy (audit A8 and A14).

Unlike a dead port, the chaos proxy lets a dependency *come back*, hang
without refusing, or cut a response off mid-body, so recovery paths are
tested and not just failure paths.
"""

from __future__ import annotations

import asyncio
import os
import time

import fakeredis
import pytest

from chaos_proxy import chaos_proxy
from helpers import build_config, gateway_client, serve, wait_for
from mock_backend.app import create_mock_app
from relay.config import RedisConfig
from relay.redis_client import create_redis

LIMITED = {"rate_limit": {"capacity": 1000, "refill_rate_per_second": 100}}


def hostport(url: str) -> tuple[str, int]:
    host, port = url.split("://", 1)[1].split("/")[0].split(":")
    return host, int(port)


async def status_of(client, path: str = "/api/catalog/x") -> int:
    return (await client.get(path)).status_code


# ------------------------------------------------------------------ Redis


@pytest.mark.redis
async def test_real_redis_outage_and_recovery() -> None:
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("REDIS_URL not set")
    host, port = hostport(url)
    async with chaos_proxy(host, port) as proxy, _upstream() as upstream_url:
        redis = create_redis(
            RedisConfig(url=f"redis://127.0.0.1:{proxy.port}/15", operation_timeout_seconds=0.3)
        )
        config = build_config(
            [upstream_url], route=LIMITED, redis={"url": "redis://injected:6379/0"}
        )
        async with gateway_client(config, redis=redis) as client:
            assert await status_of(client) == 200

            proxy.set_mode("reset")  # Redis connection refused/reset
            assert [await status_of(client) for _ in range(3)] == [503, 503, 503]
            assert (await client.get("/health/ready")).status_code == 503

            proxy.set_mode("pass")  # Redis is back: no restart needed
            assert await status_of(client) == 200
            assert (await client.get("/health/ready")).status_code == 200
        await redis.aclose()


@pytest.mark.redis
async def test_hung_redis_fails_closed_within_the_operation_timeout() -> None:
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("REDIS_URL not set")
    host, port = hostport(url)
    async with chaos_proxy(host, port) as proxy, _upstream() as upstream_url:
        redis = create_redis(
            RedisConfig(url=f"redis://127.0.0.1:{proxy.port}/15", operation_timeout_seconds=0.3)
        )
        config = build_config(
            [upstream_url], route=LIMITED, redis={"url": "redis://injected:6379/0"}
        )
        async with gateway_client(config, redis=redis) as client:
            assert await status_of(client) == 200
            proxy.set_mode("blackhole")  # accepts, never answers
            start = time.perf_counter()
            assert await status_of(client) == 503
            assert time.perf_counter() - start < 1.0  # bounded, not hung
        await redis.aclose()


async def test_fakeredis_outage_and_recovery() -> None:
    server = fakeredis.FakeServer()
    redis = fakeredis.FakeAsyncRedis(server=server)
    async with _upstream() as upstream_url:
        config = build_config(
            [upstream_url], route=LIMITED, redis={"url": "redis://injected:6379/0"}
        )
        async with gateway_client(config, redis=redis) as client:
            assert await status_of(client) == 200
            server.connected = False
            assert await status_of(client) == 503
            server.connected = True
            assert await status_of(client) == 200
    await redis.aclose()


# -------------------------------------------------------------- upstreams


async def test_truncated_response_is_retried_for_get_but_not_post(upstream_a, upstream_b) -> None:
    async with chaos_proxy(*hostport(upstream_a.url)) as proxy:
        proxy.set_mode("truncate", truncate_after=60)  # cut inside the body
        flaky = f"http://127.0.0.1:{proxy.port}"
        config = build_config(
            [flaky, upstream_b.url], route={"circuit_breaker": {"enabled": False}}
        )
        async with gateway_client(config) as client:
            get = await client.get("/api/catalog/x")
            await client.get("/api/catalog/skip")  # rotate back to the flaky upstream
            post = await client.post("/api/catalog/x", content=b"{}")
    assert get.status_code == 200
    assert get.json()["instance"] == "backend-b"  # retried elsewhere
    assert post.status_code == 502
    assert post.json()["error"]["code"] == "upstream_bad_response"
    assert (await upstream_b.stats())["requests"].get("POST", 0) == 0


async def test_hung_upstream_times_out_then_breaker_fails_fast_and_recovers(upstream_a) -> None:
    async with chaos_proxy(*hostport(upstream_a.url)) as proxy:
        config = build_config(
            [f"http://127.0.0.1:{proxy.port}"],
            route={
                "timeouts": {"connect_seconds": 0.2, "attempt_seconds": 0.3, "request_seconds": 1},
                "retries": {"max_attempts": 1},
                "circuit_breaker": {"failure_threshold": 2, "recovery_timeout_seconds": 0.3},
            },
        )
        async with gateway_client(config) as client:
            breaker = client.app.state.gateway.breakers[("catalog", "u0")]
            assert await status_of(client) == 200
            proxy.set_mode("blackhole")
            timings = []
            for _ in range(3):
                start = time.perf_counter()
                timings.append((await status_of(client), time.perf_counter() - start))
            codes = [code for code, _ in timings]
            # First request: the pooled keep-alive connection was just severed,
            # so it fails as a bad response (502) or, on a fresh connection,
            # times out (504). Either way it counts as a breaker failure.
            assert codes[0] in (502, 504)
            assert codes[1:] == [504, 503]
            assert 0.25 < timings[1][1] < 0.6  # bounded by attempt_seconds
            assert timings[2][1] < 0.05  # breaker open: fail fast, no waiting
            proxy.set_mode("pass")
            await asyncio.sleep(0.35)
            assert await status_of(client) == 200  # half-open probe succeeds
            await wait_for(lambda: breaker.state == "closed")


async def test_connection_reset_is_retried_on_the_other_upstream(upstream_a, upstream_b) -> None:
    async with chaos_proxy(*hostport(upstream_a.url)) as proxy:
        proxy.set_mode("reset")
        config = build_config([f"http://127.0.0.1:{proxy.port}", upstream_b.url])
        async with gateway_client(config) as client:
            responses = [await client.get("/api/catalog/x") for _ in range(6)]
    assert {r.status_code for r in responses} == {200}
    assert {r.json()["instance"] for r in responses} == {"backend-b"}


async def test_upstream_partition_heals_and_traffic_returns(upstream_a, upstream_b) -> None:
    async with chaos_proxy(*hostport(upstream_a.url)) as proxy:
        config = build_config(
            [f"http://127.0.0.1:{proxy.port}", upstream_b.url],
            route={
                "health_check": {
                    "enabled": True,
                    "interval_seconds": 0.05,
                    "timeout_seconds": 0.2,
                    "unhealthy_threshold": 1,
                    "healthy_threshold": 1,
                },
            },
        )
        async with gateway_client(config) as client:
            health = client.app.state.gateway.health
            proxy.set_mode("reset")
            await wait_for(lambda: not health.is_healthy("catalog", "u0"))
            during = {(await client.get("/api/catalog/x")).json()["instance"] for _ in range(6)}
            proxy.set_mode("pass")
            await wait_for(lambda: health.is_healthy("catalog", "u0"))
            after = {(await client.get("/api/catalog/x")).json()["instance"] for _ in range(6)}
    assert during == {"backend-b"}
    assert after == {"backend-a", "backend-b"}


# ----------------------------------------------------------------- helper


def _upstream():
    return serve(create_mock_app("backend-a"))
