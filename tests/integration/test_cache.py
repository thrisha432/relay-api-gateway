"""Phase 3 exit gate: cache hit/miss/expiry, key separation, unsafe bypass."""

from __future__ import annotations

import asyncio

from helpers import build_config, gateway_client, wait_for

CACHED = {"cache": {"enabled": True, "ttl_seconds": 30}}


async def get_requests(upstream) -> int:
    return (await upstream.stats())["requests"].get("GET", 0)


async def test_second_identical_get_is_served_from_cache(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url], route=CACHED)) as client:
        first = await client.get("/api/catalog/items?id=1")
        second = await client.get("/api/catalog/items?id=1")
    assert first.headers["x-relay-cache"] == "MISS"
    assert second.headers["x-relay-cache"] == "HIT"
    assert second.json() == first.json()
    assert "age" in second.headers
    assert second.headers["x-request-id"] != first.headers["x-request-id"]
    assert await get_requests(upstream_a) == 1


async def test_entries_expire_after_ttl(upstream_a) -> None:
    route = {"cache": {"enabled": True, "ttl_seconds": 0.2}}
    async with gateway_client(build_config([upstream_a.url], route=route)) as client:
        assert (await client.get("/api/catalog/x")).headers["x-relay-cache"] == "MISS"
        assert (await client.get("/api/catalog/x")).headers["x-relay-cache"] == "HIT"
        await asyncio.sleep(0.3)
        assert (await client.get("/api/catalog/x")).headers["x-relay-cache"] == "MISS"
    assert await get_requests(upstream_a) == 2


async def test_query_string_and_accept_header_separate_keys(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url], route=CACHED)) as client:
        await client.get("/api/catalog/x?a=1")
        assert (await client.get("/api/catalog/x?a=2")).headers["x-relay-cache"] == "MISS"
        xml = await client.get("/api/catalog/x?a=1", headers={"accept": "application/xml"})
        assert xml.headers["x-relay-cache"] == "MISS"
        assert (await client.get("/api/catalog/x?a=1")).headers["x-relay-cache"] == "HIT"
    assert await get_requests(upstream_a) == 3


async def test_credentialed_and_unsafe_requests_bypass_cache(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url], route=CACHED)) as client:
        await client.get("/api/catalog/x")  # warm the cache
        auth = await client.get("/api/catalog/x", headers={"authorization": "Bearer t"})
        cookie = await client.get("/api/catalog/x", headers={"cookie": "s=1"})
        no_cache = await client.get("/api/catalog/x", headers={"cache-control": "no-cache"})
        post = await client.post("/api/catalog/x", content=b"{}")
    for response in (auth, cookie, no_cache, post):
        assert response.headers["x-relay-cache"] == "BYPASS"
    # The authorized request reached the upstream with its credentials intact.
    assert auth.json()["headers"]["authorization"] == "Bearer t"


async def test_unsafe_responses_are_not_stored(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url], route=CACHED)) as client:
        for query in (
            "set_cookie=1",
            "cache_control=private",
            "cache_control=no-store",
            "status=500",
        ):
            await client.get(f"/api/catalog/x?{query}")
            # httpx keeps Set-Cookie in its jar and would replay it as Cookie
            # (which correctly bypasses the cache); clear it to test storage.
            client.cookies.clear()
            again = await client.get(f"/api/catalog/x?{query}")
            client.cookies.clear()
            assert again.headers["x-relay-cache"] == "MISS", query
        metrics = (await client.get("/metrics")).text
    assert 'relay_cache_stores_total{outcome="not_cacheable",route="catalog"} 8.0' in metrics


async def test_oversized_responses_are_not_stored(upstream_a) -> None:
    route = {"cache": {"enabled": True, "max_entry_bytes": 100}}
    async with gateway_client(build_config([upstream_a.url], route=route)) as client:
        await client.get("/api/catalog/x?size=101")
        assert (await client.get("/api/catalog/x?size=101")).headers["x-relay-cache"] == "MISS"
        await client.get("/api/catalog/x?size=100")
        assert (await client.get("/api/catalog/x?size=100")).headers["x-relay-cache"] == "HIT"


async def test_cache_hit_served_while_upstream_is_unhealthy(upstream_a) -> None:
    route = {
        **CACHED,
        "health_check": {
            "enabled": True,
            "interval_seconds": 0.05,
            "unhealthy_threshold": 1,
            "healthy_threshold": 1,
        },
    }
    async with gateway_client(build_config([upstream_a.url], route=route)) as client:
        await client.get("/api/catalog/x")
        await upstream_a.configure(healthy=False)
        gateway = client.app.state.gateway
        await wait_for(lambda: not gateway.health.is_healthy("catalog", "u0"))
        assert (await client.get("/api/catalog/x")).headers["x-relay-cache"] == "HIT"
        assert (await client.get("/api/catalog/y")).status_code == 503


async def test_lru_capacity_bounds_entries_and_counts_evictions(upstream_a) -> None:
    config = build_config([upstream_a.url], route=CACHED, cache={"max_entries": 3})
    async with gateway_client(config) as client:
        await asyncio.gather(*(client.get(f"/api/catalog/x?n={n}") for n in range(20)))
        lru = client.app.state.gateway.cache.lru
        lru.check_invariants()
        assert len(lru) == 3
        metrics = (await client.get("/metrics")).text
    assert "relay_cache_evictions_total 17.0" in metrics
    assert 'relay_cache_requests_total{result="miss",route="catalog"} 20.0' in metrics


async def test_cache_disabled_route_has_no_cache_header(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        response = await client.get("/api/catalog/x")
    assert "x-relay-cache" not in response.headers
