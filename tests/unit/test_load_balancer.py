import asyncio
from collections import Counter

import pytest

from relay.config import UpstreamConfig
from relay.load_balancer import RoundRobinBalancer


def ups(*ids: str) -> list[UpstreamConfig]:
    return [UpstreamConfig(id=i, url=f"http://{i}:1") for i in ids]


def picks(balancer: RoundRobinBalancer, n: int, available=lambda _: True) -> list[str]:
    return [balancer.candidates(available)[0].id for _ in range(n)]


def test_round_robin_sequence() -> None:
    assert picks(RoundRobinBalancer(ups("a", "b", "c")), 7) == ["a", "b", "c", "a", "b", "c", "a"]


def test_unhealthy_instances_are_skipped_and_load_spreads_evenly() -> None:
    balancer = RoundRobinBalancer(ups("a", "b", "c"))
    sequence = picks(balancer, 6, available=lambda u: u.id != "b")
    assert "b" not in sequence
    assert Counter(sequence) == {"a": 3, "c": 3}


def test_candidates_include_failover_order() -> None:
    balancer = RoundRobinBalancer(ups("a", "b", "c"))
    assert [u.id for u in balancer.candidates()] == ["a", "b", "c"]
    assert [u.id for u in balancer.candidates()] == ["b", "c", "a"]


def test_no_available_instances_returns_empty() -> None:
    assert RoundRobinBalancer(ups("a", "b")).candidates(lambda _: False) == []


def test_requires_upstreams() -> None:
    with pytest.raises(ValueError, match="at least one"):
        RoundRobinBalancer([])


async def test_concurrent_selection_is_exactly_even() -> None:
    """1,000 concurrent tasks: each pick is atomic (no await inside), so the
    split is exactly even. This holds because the event loop is single-threaded."""
    balancer = RoundRobinBalancer(ups("a", "b", "c", "d"))

    async def pick() -> str:
        await asyncio.sleep(0)
        return balancer.candidates()[0].id

    results = await asyncio.gather(*(pick() for _ in range(1000)))
    assert Counter(results) == {"a": 250, "b": 250, "c": 250, "d": 250}
