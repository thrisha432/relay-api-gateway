"""Shared response cache in Redis (``cache.backend: redis``).

Unlike the per-process LRU, every gateway instance sees the same entries, so
a response cached by one instance is a hit on all of them. Redis enforces the
TTL (``PSETEX``) and its own memory policy; Relay applies the same
eligibility rules and ``max_entry_bytes`` cap as for the local cache.

Failure policy: the cache is an optimisation, so Redis errors are treated as a
miss (on read) or a skipped store (on write). They are counted in
``relay_redis_errors_total`` but never fail the client request.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable

from redis.asyncio import Redis

from relay.cache.response_cache import CachedResponse
from relay.redis_client import REDIS_ERRORS


def serialize(entry: CachedResponse) -> bytes:
    return json.dumps(
        {
            "s": entry.status,
            # latin-1 maps bytes 0-255 one-to-one, so header bytes round-trip exactly.
            "h": [[k.decode("latin-1"), v.decode("latin-1")] for k, v in entry.headers],
            "b": base64.b64encode(entry.body).decode("ascii"),
            "t": entry.stored_at,
        },
        separators=(",", ":"),
    ).encode("utf-8")


def deserialize(raw: bytes) -> CachedResponse | None:
    try:
        data = json.loads(raw)
        return CachedResponse(
            status=int(data["s"]),
            headers=[(k.encode("latin-1"), v.encode("latin-1")) for k, v in data["h"]],
            body=base64.b64decode(data["b"], validate=True),
            stored_at=float(data["t"]),
        )
    except (ValueError, KeyError, TypeError):
        return None  # corrupt/foreign value: treat as a miss


class RedisResponseCache:
    backend_name = "redis"

    def __init__(
        self,
        redis: Redis,
        *,
        operation_timeout_seconds: float = 0.5,
        key_prefix: str = "relay:cache:",
        on_error: Callable[[str], None] | None = None,
    ) -> None:
        self._redis = redis
        self._timeout = operation_timeout_seconds
        self._prefix = key_prefix
        self._on_error = on_error

    async def get(self, key: str) -> CachedResponse | None:
        try:
            async with asyncio.timeout(self._timeout):
                raw = await self._redis.get(self._prefix + key)
        except REDIS_ERRORS:
            self._error("cache_get")
            return None
        return deserialize(raw) if raw is not None else None

    async def set(self, key: str, value: CachedResponse, ttl_seconds: float) -> None:
        ttl_ms = max(1, int(ttl_seconds * 1000))
        try:
            async with asyncio.timeout(self._timeout):
                await self._redis.psetex(self._prefix + key, ttl_ms, serialize(value))
        except REDIS_ERRORS:
            self._error("cache_set")

    async def close(self) -> None:
        """The Redis client is owned by the gateway, which closes it."""

    def _error(self, operation: str) -> None:
        if self._on_error is not None:
            self._on_error(operation)
