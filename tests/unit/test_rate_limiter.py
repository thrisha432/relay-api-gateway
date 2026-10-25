"""Token-bucket math and the Lua contract, with deterministic time (now_us)."""

from __future__ import annotations

import fakeredis
import pytest

from relay.config import RateLimitConfig
from relay.rate_limiter import RateLimiterUnavailable, RedisRateLimiter, client_key

SECOND = 1_000_000
T0 = 1_700_000_000 * SECOND  # realistic epoch microseconds


@pytest.fixture
async def limiter(redis_factory):
    return RedisRateLimiter(redis_factory())


async def take(limiter, cfg, at_us, client="c1", route="r"):
    return await limiter.acquire(route, client, cfg, now_us=at_us)


async def test_burst_up_to_capacity_then_reject(limiter) -> None:
    cfg = RateLimitConfig(capacity=5, refill_rate_per_second=1)
    results = [await take(limiter, cfg, T0) for _ in range(6)]
    assert [r.allowed for r in results] == [True] * 5 + [False]
    assert [r.remaining for r in results[:5]] == [4, 3, 2, 1, 0]
    assert results[-1].retry_after_seconds == 1
    assert results[-1].limit == 5


async def test_steady_refill(limiter) -> None:
    cfg = RateLimitConfig(capacity=2, refill_rate_per_second=2)
    assert (await take(limiter, cfg, T0)).allowed
    assert (await take(limiter, cfg, T0)).allowed
    assert not (await take(limiter, cfg, T0)).allowed
    # 0.5 s at 2 tokens/s = exactly one token.
    assert (await take(limiter, cfg, T0 + SECOND // 2)).allowed
    assert not (await take(limiter, cfg, T0 + SECOND // 2)).allowed


async def test_refill_never_exceeds_capacity(limiter) -> None:
    cfg = RateLimitConfig(capacity=3, refill_rate_per_second=100)
    await take(limiter, cfg, T0)
    later = [await take(limiter, cfg, T0 + 3600 * SECOND) for _ in range(4)]
    assert [r.allowed for r in later] == [True, True, True, False]


async def test_retry_after_reflects_slow_refill(limiter) -> None:
    cfg = RateLimitConfig(capacity=1, refill_rate_per_second=0.1)
    await take(limiter, cfg, T0)
    assert (await take(limiter, cfg, T0)).retry_after_seconds == 10
    # 4 s later: 0.4 tokens -> 6 s still to wait.
    assert (await take(limiter, cfg, T0 + 4 * SECOND)).retry_after_seconds == 6


async def test_clock_going_backwards_adds_no_tokens(limiter) -> None:
    cfg = RateLimitConfig(capacity=1, refill_rate_per_second=1)
    assert (await take(limiter, cfg, T0)).allowed
    assert not (await take(limiter, cfg, T0 - 10 * SECOND)).allowed
    # Stored timestamp did not move backwards: refill resumes from T0.
    assert (await take(limiter, cfg, T0 + SECOND)).allowed


async def test_clients_and_routes_are_independent(limiter) -> None:
    cfg = RateLimitConfig(capacity=1, refill_rate_per_second=1)
    assert (await take(limiter, cfg, T0, client="a")).allowed
    assert not (await take(limiter, cfg, T0, client="a")).allowed
    assert (await take(limiter, cfg, T0, client="b")).allowed
    assert (await take(limiter, cfg, T0, client="a", route="other")).allowed


async def test_server_time_is_used_when_now_is_omitted(limiter) -> None:
    cfg = RateLimitConfig(capacity=2, refill_rate_per_second=0.001)
    assert (await limiter.acquire("r", "srv", cfg)).allowed
    assert (await limiter.acquire("r", "srv", cfg)).allowed
    assert not (await limiter.acquire("r", "srv", cfg)).allowed


async def test_keys_expire_and_hide_client_identity(redis_factory) -> None:
    redis = redis_factory()
    limiter = RedisRateLimiter(redis)
    cfg = RateLimitConfig(capacity=10, refill_rate_per_second=1, key_ttl_seconds=30)
    await limiter.acquire("r", "203.0.113.9", cfg)
    key = client_key("r", "203.0.113.9")
    assert "203.0.113.9" not in key
    assert 0 < await redis.pttl(key) <= 30_000


async def test_unreachable_redis_raises_unavailable(dead_redis) -> None:
    limiter = RedisRateLimiter(dead_redis, operation_timeout_seconds=0.5)
    cfg = RateLimitConfig(capacity=1, refill_rate_per_second=1)
    with pytest.raises(RateLimiterUnavailable):
        await limiter.acquire("r", "c", cfg)
    await dead_redis.aclose()


async def test_script_reloads_after_redis_script_flush() -> None:
    """EVALSHA falls back to EVAL if Redis lost the cached script (e.g. restart)."""
    redis = fakeredis.FakeAsyncRedis()
    limiter = RedisRateLimiter(redis)
    cfg = RateLimitConfig(capacity=5, refill_rate_per_second=1)
    await limiter.acquire("r", "c", cfg)
    await redis.script_flush()
    assert (await limiter.acquire("r", "c", cfg)).allowed
