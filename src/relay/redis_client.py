"""Redis connection lifecycle.

One client (with its own connection pool) per gateway process, created at
startup and closed at shutdown. Short socket timeouts make an unreachable
Redis fail fast so the configured outage policy can kick in.
"""

from __future__ import annotations

import asyncio

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import RedisError

from relay.config import RedisConfig

# Exceptions that mean "Redis is unavailable right now".
REDIS_ERRORS: tuple[type[BaseException], ...] = (RedisError, OSError, TimeoutError)


def create_redis(cfg: RedisConfig) -> Redis:
    if cfg.url is None:
        raise ValueError("redis.url is not configured")
    return Redis.from_url(
        cfg.url,
        socket_connect_timeout=cfg.connect_timeout_seconds,
        socket_timeout=cfg.operation_timeout_seconds,
        retry_on_timeout=False,
        # At most one quick retry: during an outage, failing fast matters more
        # than riding out blips (redis-py's default retries several times).
        retry=Retry(ExponentialBackoff(cap=0.05, base=0.01), retries=1),
        health_check_interval=30,
        decode_responses=False,
    )


async def ping(redis: Redis, timeout_seconds: float) -> bool:
    try:
        async with asyncio.timeout(timeout_seconds):
            return bool(await redis.ping())
    except REDIS_ERRORS:
        return False
