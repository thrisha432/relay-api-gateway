"""Audit fixes A3 (cache stampede) and A4 (retry amplification in brownouts)."""

from __future__ import annotations

import asyncio
from collections import Counter

from helpers import build_config, gateway_client


async def upstream_gets(upstream) -> int:
    return (await upstream.stats())["requests"].get("GET", 0)


async def test_concurrent_misses_are_coalesced_into_one_upstream_request(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"cache": {"enabled": True}})
    async with gateway_client(config) as client:
        responses = await asyncio.gather(
            *(client.get("/api/catalog/herd?delay_ms=200") for _ in range(50))
        )
        metrics = (await client.get("/metrics")).text
    assert {r.status_code for r in responses} == {200}
    assert len({r.content for r in responses}) == 1  # identical bodies
    assert Counter(r.headers["x-relay-cache"] for r in responses) == {"MISS": 1, "COALESCED": 49}
    assert await upstream_gets(upstream_a) == 1
    assert 'relay_cache_coalesced_total{route="catalog"} 49.0' in metrics


async def test_coalescing_can_be_disabled(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"cache": {"enabled": True, "coalesce": False}})
    async with gateway_client(config) as client:
        await asyncio.gather(*(client.get("/api/catalog/herd?delay_ms=200") for _ in range(20)))
    assert await upstream_gets(upstream_a) == 20


async def test_coalescing_respects_the_cache_key(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"cache": {"enabled": True}})
    async with gateway_client(config) as client:
        await asyncio.gather(
            *(client.get(f"/api/catalog/herd?delay_ms=100&n={i % 5}") for i in range(25)),
            client.get("/api/catalog/herd?delay_ms=100&n=0", headers={"accept": "application/xml"}),
            client.get("/api/catalog/herd?delay_ms=100&n=0", headers={"authorization": "Bearer x"}),
        )
    # 5 query variants + 1 distinct Accept + 1 credentialed bypass (never shared)
    assert await upstream_gets(upstream_a) == 7


async def test_shared_failure_is_returned_to_every_waiter(upstream_a) -> None:
    await upstream_a.configure(fail_status=503)
    config = build_config(
        [upstream_a.url],
        route={"cache": {"enabled": True}, "retries": {"max_attempts": 1}},
    )
    async with gateway_client(config) as client:
        responses = await asyncio.gather(
            *(client.get("/api/catalog/x?delay_ms=100") for _ in range(10))
        )
    assert {r.status_code for r in responses} == {503}
    assert await upstream_gets(upstream_a) == 1


async def test_leader_disconnect_does_not_fail_followers(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"cache": {"enabled": True}})
    async with gateway_client(config) as client:
        leader = asyncio.create_task(client.get("/api/catalog/x?delay_ms=300"))
        await asyncio.sleep(0.05)
        followers = [
            asyncio.create_task(client.get("/api/catalog/x?delay_ms=300")) for _ in range(3)
        ]
        await asyncio.sleep(0.05)
        leader.cancel()
        results = await asyncio.gather(*followers)
        cached = await client.get("/api/catalog/x?delay_ms=300")
    assert [r.status_code for r in results] == [200, 200, 200]
    assert cached.headers["x-relay-cache"] == "HIT"  # leader's fill still completed
    assert await upstream_gets(upstream_a) == 1


async def run_brownout(upstream, *, budget_enabled: bool, requests: int = 200) -> tuple[int, str]:
    """Upstream fails every 2nd request: the breaker (consecutive failures)
    never opens, so only the retry budget limits amplification."""
    await upstream.configure(fail_status=503, fail_every=2)
    config = build_config(
        [upstream.url],
        route={
            "retries": {
                "max_attempts": 3,
                "budget_enabled": budget_enabled,
                "budget_ratio": 0.1,
                "budget_min_retries_per_second": 0,
                "budget_max_balance": 1,
            },
            "circuit_breaker": {"failure_threshold": 5},
        },
    )
    async with gateway_client(config) as client:
        for _ in range(requests):
            await client.get("/api/catalog/x")
        metrics = (await client.get("/metrics")).text
    return await upstream_gets(upstream), metrics


async def test_retry_budget_caps_amplification_during_brownout(upstream_a) -> None:
    upstream_calls, metrics = await run_brownout(upstream_a, budget_enabled=True)
    # <= 200 first attempts + 10% budget (+1 initial token)
    assert upstream_calls <= 200 * 1.1 + 1
    assert "relay_retry_budget_exhausted_total" in metrics


async def test_without_budget_brownout_amplifies(upstream_a) -> None:
    upstream_calls, _ = await run_brownout(upstream_a, budget_enabled=False)
    assert upstream_calls >= 290  # ~1.5x: every failed first attempt is retried
