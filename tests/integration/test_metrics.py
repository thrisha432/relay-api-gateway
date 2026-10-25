"""Phase 6: metrics exist under normal and failure traffic and labels stay
bounded (no raw paths, query strings, or client IPs)."""

from __future__ import annotations

import re

from prometheus_client.parser import text_string_to_metric_families

from helpers import build_config, free_port_url, gateway_client

REQUIRED_FAMILIES = {
    "relay_requests",
    "relay_request_duration_seconds",
    "relay_in_flight_requests",
    "relay_upstream_requests",
    "relay_upstream_request_duration_seconds",
    "relay_no_healthy_upstream",
    "relay_health_checks",
    "relay_upstream_healthy",
    "relay_rate_limit_decisions",
    "relay_cache_requests",
    "relay_cache_stores",
    "relay_cache_evictions",
    "relay_retries",
    "relay_retries_exhausted",
    "relay_circuit_breaker_transitions",
    "relay_circuit_breaker_rejections",
    "relay_circuit_breaker_state",
    "relay_redis_errors",
}

ALLOWED_LABELS = {
    "route",
    "method",
    "status_class",
    "upstream",
    "outcome",
    "result",
    "decision",
    "reason",
    "from_state",
    "to_state",
    "operation",
    "le",
}

IP_LIKE = re.compile(r"\d+\.\d+\.\d+\.\d+")


async def test_metrics_cover_normal_and_failure_paths(upstream_a, redis_factory) -> None:
    config = build_config(
        [upstream_a.url, free_port_url()],
        route={
            "cache": {"enabled": True},
            "rate_limit": {"capacity": 3, "refill_rate_per_second": 0.01},
            "circuit_breaker": {"failure_threshold": 1, "recovery_timeout_seconds": 60},
            "health_check": {
                "enabled": True,
                "interval_seconds": 0.05,
                "unhealthy_threshold": 50,
            },
        },
        redis={"url": "redis://injected:6379/0"},
        server={"trusted_proxies": ["10.0.0.0/8"]},
    )
    async with gateway_client(config, redis=redis_factory()) as client:
        for i in range(35):  # unique paths, queries and client IPs
            await client.get(
                f"/api/catalog/item-{i}?user={i}&token=secret{i}",
                headers={"x-forwarded-for": f"198.51.100.{i}"},
            )
        for _ in range(4):  # one client bursting past its own bucket (capacity 3)
            await client.get("/api/catalog/burst", headers={"x-forwarded-for": "198.51.100.250"})
        await client.get("/no/such/route?q=1")
        text = (await client.get("/metrics")).text

    families = {f.name: f for f in text_string_to_metric_families(text)}
    missing = REQUIRED_FAMILIES - families.keys()
    assert not missing, f"missing metric families: {missing}"

    for family in families.values():
        if not family.name.startswith("relay_"):
            continue
        for sample in family.samples:
            assert set(sample.labels) <= ALLOWED_LABELS, (family.name, sample.labels)
            for value in sample.labels.values():
                assert "/" not in value, (family.name, value)
                assert "?" not in value, (family.name, value)
                assert "secret" not in value, (family.name, value)
                assert not IP_LIKE.search(value), (family.name, value)

    routes = {s.labels["route"] for s in families["relay_requests"].samples if "route" in s.labels}
    assert routes <= {"catalog", "_unmatched", "_ops"}
    decisions = {
        s.labels["decision"]
        for s in families["relay_rate_limit_decisions"].samples
        if s.name.endswith("_total")
    }
    assert {"allowed", "limited"} <= decisions
    assert 'relay_circuit_breaker_state{route="catalog",upstream="u1"} 2.0' in text
