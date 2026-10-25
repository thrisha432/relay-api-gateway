import asyncio
import random
from collections import OrderedDict

import pytest

from relay.cache.lru import LRUCache


def test_get_miss_returns_default() -> None:
    cache: LRUCache[str, int] = LRUCache(2)
    assert cache.get("x") is None
    assert cache.get("x", -1) == -1


def test_evicts_least_recently_used() -> None:
    cache: LRUCache[str, int] = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.put("c", 3) == ("a", 1)
    assert list(cache.keys()) == ["c", "b"]
    assert "a" not in cache


def test_get_refreshes_recency() -> None:
    cache: LRUCache[str, int] = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == 1  # a becomes MRU
    assert cache.put("c", 3) == ("b", 2)
    assert list(cache.keys()) == ["c", "a"]


def test_update_existing_key_replaces_value_and_refreshes_without_eviction() -> None:
    cache: LRUCache[str, int] = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.put("a", 10) is None
    assert len(cache) == 2
    assert list(cache.keys()) == ["a", "b"]
    assert cache.peek("a") == 10


def test_peek_and_contains_do_not_change_recency() -> None:
    cache: LRUCache[str, int] = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.peek("a") == 1
    assert "a" in cache
    assert cache.put("c", 3) == ("a", 1)


def test_capacity_zero_stores_nothing() -> None:
    cache: LRUCache[str, int] = LRUCache(0)
    assert cache.put("a", 1) is None
    assert len(cache) == 0
    assert cache.get("a") is None
    cache.check_invariants()


def test_capacity_one() -> None:
    cache: LRUCache[str, int] = LRUCache(1)
    cache.put("a", 1)
    assert cache.put("b", 2) == ("a", 1)
    assert list(cache.keys()) == ["b"]


def test_delete_and_clear() -> None:
    cache: LRUCache[str, int] = LRUCache(3)
    for i, k in enumerate("abc"):
        cache.put(k, i)
    assert cache.delete("b") is True
    assert cache.delete("b") is False
    assert list(cache.keys()) == ["c", "a"]
    cache.check_invariants()
    cache.clear()
    assert len(cache) == 0
    assert list(cache.keys()) == []
    cache.check_invariants()


def test_on_evict_callback() -> None:
    evicted: list[tuple[str, int]] = []
    cache: LRUCache[str, int] = LRUCache(1, on_evict=lambda k, v: evicted.append((k, v)))
    cache.put("a", 1)
    cache.put("a", 2)  # update: no eviction
    cache.put("b", 3)
    assert evicted == [("a", 2)]


def test_negative_capacity_rejected() -> None:
    with pytest.raises(ValueError, match="capacity"):
        LRUCache(-1)


@pytest.mark.parametrize("seed", range(20))
def test_matches_reference_model_under_random_operations(seed: int) -> None:
    """Model-based test: compare against an OrderedDict reference LRU."""
    rng = random.Random(seed)
    capacity = rng.randint(0, 8)
    cache: LRUCache[int, int] = LRUCache(capacity)
    model: OrderedDict[int, int] = OrderedDict()

    for _ in range(2000):
        key = rng.randint(0, 12)
        op = rng.random()
        if op < 0.45:
            expected = model.get(key)
            if expected is not None:
                model.move_to_end(key)
            assert cache.get(key) == expected
        elif op < 0.9:
            value = rng.randint(0, 1000)
            expected_evicted = None
            if capacity > 0:
                if key in model:
                    model.move_to_end(key)
                elif len(model) >= capacity:
                    expected_evicted = model.popitem(last=False)
                model[key] = value
            assert cache.put(key, value) == expected_evicted
        else:
            assert cache.delete(key) == (model.pop(key, None) is not None)
        cache.check_invariants()
        assert list(cache.keys()) == list(reversed(model))


async def test_concurrent_access_keeps_invariants() -> None:
    """Many coroutines interleaving get/put/delete never corrupt the list."""
    cache: LRUCache[int, int] = LRUCache(16)
    rng = random.Random(7)

    async def worker(worker_id: int) -> None:
        for i in range(300):
            key = rng.randint(0, 40)
            match i % 3:
                case 0:
                    cache.put(key, worker_id)
                case 1:
                    cache.get(key)
                case _:
                    if rng.random() < 0.2:
                        cache.delete(key)
            await asyncio.sleep(0)  # force interleaving between operations

    await asyncio.gather(*(worker(n) for n in range(50)))
    cache.check_invariants()
    assert len(cache) <= 16
