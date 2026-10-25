"""Phase 2 exit gate: distribution, unhealthy exclusion, and recovery."""

from __future__ import annotations

import asyncio
from collections import Counter

from helpers import build_config, free_port_url, gateway_client, wait_for

FAST_HEALTH = {
    "enabled": True,
    "interval_seconds": 0.05,
    "timeout_seconds": 0.5,
    "unhealthy_threshold": 2,
    "healthy_threshold": 2,
}


async def instances(client, n: int) -> list[str]:
    return [(await client.get("/api/catalog/x")).json()["instance"] for _ in range(n)]


async def test_round_robin_distribution_across_two_upstreams(upstream_a, upstream_b) -> None:
    async with gateway_client(build_config([upstream_a.url, upstream_b.url])) as client:
        sequence = await instances(client, 10)
    assert sequence == ["backend-a", "backend-b"] * 5


async def test_unhealthy_upstream_excluded_then_restored(upstream_a, upstream_b) -> None:
    config = build_config([upstream_a.url, upstream_b.url], route={"health_check": FAST_HEALTH})
    async with gateway_client(config) as client:
        gateway = client.app.state.gateway
        await wait_for(gateway.health_checker.first_round_complete)
        assert (await client.get("/health/ready")).status_code == 200

        await upstream_b.configure(healthy=False)
        await wait_for(lambda: not gateway.health.is_healthy("catalog", "u1"))
        assert set(await instances(client, 6)) == {"backend-a"}
        summary = (await client.get("/__relay/routes")).json()["routes"][0]
        assert summary["upstreams"] == [
            {"id": "u0", "healthy": True},
            {"id": "u1", "healthy": False},
        ]
        assert "url" not in str(summary)  # no upstream URLs leaked

        await upstream_b.configure(healthy=True)
        await wait_for(lambda: gateway.health.is_healthy("catalog", "u1"))
        assert Counter(await instances(client, 6)) == {"backend-a": 3, "backend-b": 3}


async def test_all_unhealthy_returns_503_and_not_ready(upstream_a) -> None:
    config = build_config([upstream_a.url, free_port_url()], route={"health_check": FAST_HEALTH})
    async with gateway_client(config) as client:
        gateway = client.app.state.gateway
        await wait_for(lambda: not gateway.health.is_healthy("catalog", "u1"))
        # Refused-connection upstream is excluded; healthy one serves everything.
        assert set(await instances(client, 4)) == {"backend-a"}

        await upstream_a.configure(healthy=False)
        await wait_for(lambda: not gateway.health.is_healthy("catalog", "u0"))
        response = await client.get("/api/catalog/x")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "upstream_unavailable"
        ready = await client.get("/health/ready")
        assert ready.status_code == 503
        assert ready.json()["checks"]["routes_with_healthy_upstream"] == {"catalog": False}
        assert (await client.get("/health/live")).status_code == 200  # liveness unaffected


async def test_shutdown_cancels_health_tasks(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"health_check": FAST_HEALTH})
    async with gateway_client(config) as client:
        checker = client.app.state.gateway.health_checker
        assert checker.running_tasks == 1
    assert checker.running_tasks == 0
    names = {t.get_name() for t in asyncio.all_tasks()}
    assert not any(name.startswith("relay-health-") for name in names)
