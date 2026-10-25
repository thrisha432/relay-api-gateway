"""Distributed token-bucket rate limiting backed by Redis.

Algorithm (per route + client): a bucket holds up to ``capacity`` tokens and
refills continuously at ``refill_rate_per_second``. Each request takes one
token; with none left the request gets 429 and a ``Retry-After`` computed from
the refill rate. ``capacity`` is therefore the allowed burst, and the
long-run rate is the refill rate.

Atomicity: refill, check, and consume happen inside one Lua script
(``token_bucket.lua``). Redis executes scripts one at a time, so concurrent
requests from many gateway instances can never both spend the last token.

Clock: the script reads Redis's own ``TIME``, so gateway instances with
skewed clocks still share one consistent notion of "now".

Privacy: client identifiers are hashed before becoming part of a Redis key,
and are never logged.

Outage policy (``redis.fail_open_for_rate_limit``):
* false (default): fail closed. Rate-limited routes return 503
  ``rate_limiter_unavailable`` rather than silently serving unlimited traffic.
* true: fail open. Requests are allowed, a metric is incremented, and a
  warning is logged (at most once per ``_LOG_INTERVAL`` seconds).
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import dataclass
from importlib.resources import files

from redis.asyncio import Redis

from relay.config import RateLimitConfig
from relay.redis_client import REDIS_ERRORS

TOKEN_BUCKET_LUA = files("relay").joinpath("token_bucket.lua").read_text(encoding="utf-8")


class RateLimiterUnavailable(Exception):
    """Redis could not be reached or the script failed."""


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_seconds: int  # whole seconds for the Retry-After header; 0 if allowed
    limit: int


def client_key(route_id: str, client_id: str, prefix: str = "relay:rl") -> str:
    digest = hashlib.sha256(client_id.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}:{route_id}:{digest}"


class RedisRateLimiter:
    def __init__(
        self, redis: Redis, *, operation_timeout_seconds: float = 0.5, key_prefix: str = "relay:rl"
    ) -> None:
        self._script = redis.register_script(TOKEN_BUCKET_LUA)
        self._timeout = operation_timeout_seconds
        self._prefix = key_prefix

    async def acquire(
        self,
        route_id: str,
        client_id: str,
        cfg: RateLimitConfig,
        *,
        cost: int = 1,
        now_us: int | None = None,
    ) -> RateLimitDecision:
        """Try to take ``cost`` tokens. Raises RateLimiterUnavailable on Redis failure.

        ``now_us`` overrides Redis server time; only tests should pass it.
        """
        args: list[int | float] = [
            cfg.capacity,
            cfg.refill_rate_per_second,
            cost,
            cfg.effective_key_ttl_seconds * 1000,
        ]
        if now_us is not None:
            args.append(now_us)
        try:
            async with asyncio.timeout(self._timeout):
                allowed, tokens, retry_after_us = await self._script(
                    keys=[client_key(route_id, client_id, self._prefix)], args=args
                )
        except REDIS_ERRORS as exc:
            raise RateLimiterUnavailable(type(exc).__name__) from exc
        remaining = math.floor(float(tokens))
        retry_after = 0 if allowed else max(1, math.ceil(int(retry_after_us) / 1_000_000))
        return RateLimitDecision(
            allowed=bool(allowed),
            remaining=remaining,
            retry_after_seconds=retry_after,
            limit=cfg.capacity,
        )
