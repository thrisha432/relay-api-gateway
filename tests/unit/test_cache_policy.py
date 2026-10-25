import pytest

from relay.cache.response_cache import (
    CachedResponse,
    LocalResponseCache,
    cache_key,
    cacheable_headers,
    request_bypass_reason,
    response_ttl,
)
from relay.config import RouteCacheConfig

CFG = RouteCacheConfig(enabled=True, ttl_seconds=30, max_entry_bytes=100)


@pytest.mark.parametrize(
    ("method", "headers", "reason"),
    [
        ("GET", [], None),
        ("GET", [(b"accept", b"application/json")], None),
        ("POST", [], "method"),
        ("HEAD", [], "method"),
        ("GET", [(b"Authorization", b"Bearer x")], "authorization"),
        ("GET", [(b"cookie", b"s=1")], "cookie"),
        ("GET", [(b"range", b"bytes=0-10")], "range"),
        ("GET", [(b"cache-control", b"no-cache")], "client_no_cache"),
        ("GET", [(b"cache-control", b"max-age=0, no-store")], "client_no_cache"),
        ("GET", [(b"pragma", b"no-cache")], "client_no_cache"),
    ],
)
def test_request_bypass_rules(method, headers, reason) -> None:
    assert request_bypass_reason(method, headers) == reason


@pytest.mark.parametrize(
    ("status", "headers", "size", "ttl"),
    [
        (200, [], 10, 30),
        (201, [], 10, None),  # status not allowed
        (500, [], 10, None),
        (200, [(b"set-cookie", b"a=1")], 10, None),
        (200, [(b"cache-control", b"private")], 10, None),
        (200, [(b"cache-control", b"no-store")], 10, None),
        (200, [(b"cache-control", b"no-cache")], 10, None),
        (200, [(b"cache-control", b"public, max-age=5")], 10, 5),
        (200, [(b"cache-control", b"max-age=999")], 10, 30),  # route TTL is a ceiling
        (200, [(b"cache-control", b"max-age=60, s-maxage=3")], 10, 3),  # s-maxage wins
        (200, [(b"cache-control", b"max-age=0")], 10, None),
        (200, [(b"cache-control", b"max-age=abc")], 10, None),
        (200, [], 101, None),  # too large
        (200, [(b"vary", b"Accept")], 10, 30),
        (200, [(b"vary", b"Accept, Accept-Encoding")], 10, 30),
        (200, [(b"vary", b"*")], 10, None),
        (200, [(b"vary", b"User-Agent")], 10, None),  # not part of our key
    ],
)
def test_response_ttl_rules(status, headers, size, ttl) -> None:
    assert response_ttl(status, headers, size, CFG) == ttl


def test_cache_key_is_deterministic_and_separates_inputs() -> None:
    vary = ("accept", "accept-encoding")
    base = cache_key("r", "/p", "a=1", [(b"accept", b"json")], vary)
    assert base == cache_key("r", "/p", "a=1", [(b"Accept", b"json")], vary)
    assert len(base) == 64
    different = {
        cache_key("other", "/p", "a=1", [(b"accept", b"json")], vary),
        cache_key("r", "/q", "a=1", [(b"accept", b"json")], vary),
        cache_key("r", "/p", "a=2", [(b"accept", b"json")], vary),
        cache_key("r", "/p", "", [(b"accept", b"json")], vary),
        cache_key("r", "/p", "a=1", [(b"accept", b"xml")], vary),
        cache_key("r", "/p", "a=1", [], vary),
    }
    assert base not in different
    assert len(different) == 6
    # Headers outside the vary list do not affect the key.
    assert base == cache_key("r", "/p", "a=1", [(b"accept", b"json"), (b"x-y", b"1")], vary)


def test_cacheable_headers_drop_per_response_and_hop_by_hop() -> None:
    headers = [
        (b"content-type", b"application/json"),
        (b"date", b"Mon"),
        (b"age", b"3"),
        (b"connection", b"close"),
        (b"x-relay-cache", b"MISS"),
        (b"etag", b'"v1"'),
    ]
    assert cacheable_headers(headers) == [
        (b"content-type", b"application/json"),
        (b"etag", b'"v1"'),
    ]


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


async def test_local_cache_ttl_expiry_and_eviction_counting() -> None:
    clock = FakeClock()
    evictions = []
    cache = LocalResponseCache(2, clock=clock, on_evict=lambda: evictions.append(1))
    entry = CachedResponse(200, [], b"x", stored_at=0)
    await cache.set("a", entry, ttl_seconds=10)
    clock.now += 9.9
    assert await cache.get("a") == entry
    clock.now += 0.2
    assert await cache.get("a") is None  # expired, lazily removed
    assert len(cache.lru) == 0
    for key in "bcd":
        await cache.set(key, entry, 10)
    assert len(evictions) == 1
    cache.lru.check_invariants()


def test_age_header_value() -> None:
    assert CachedResponse(200, [], b"", stored_at=1000.0).age_seconds(now=1003.7) == 3
