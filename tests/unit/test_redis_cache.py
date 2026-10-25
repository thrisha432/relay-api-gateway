import fakeredis

from relay.cache.redis_cache import RedisResponseCache, deserialize, serialize
from relay.cache.response_cache import CachedResponse

ENTRY = CachedResponse(
    status=200,
    headers=[(b"set-x", b"1"), (b"set-x", b"2"), (b"x-bin", bytes([0xE9, 0x41]))],
    body=bytes(range(256)),
    stored_at=1234.5,
)


def test_serialization_round_trips_bytes_exactly() -> None:
    assert deserialize(serialize(ENTRY)) == ENTRY


def test_corrupt_values_are_misses() -> None:
    for raw in (b"not json", b"{}", b'{"s":200,"h":[],"b":"***","t":1}'):
        assert deserialize(raw) is None


async def test_get_set_with_ttl() -> None:
    redis = fakeredis.FakeAsyncRedis()
    cache = RedisResponseCache(redis)
    assert await cache.get("k") is None
    await cache.set("k", ENTRY, ttl_seconds=5)
    assert await cache.get("k") == ENTRY
    assert 0 < await redis.pttl("relay:cache:k") <= 5000


async def test_redis_errors_degrade_to_miss(dead_redis) -> None:
    errors: list[str] = []
    cache = RedisResponseCache(dead_redis, on_error=errors.append)
    await cache.set("k", ENTRY, 5)
    assert await cache.get("k") is None
    assert errors == ["cache_set", "cache_get"]
    await dead_redis.aclose()
