"""Response-cache policy and the local (per-process) cache backend.

Supported subset of HTTP caching (deliberately conservative; NOT a full
RFC 9111 implementation):

Request side - the cache is bypassed when the request
* is not GET,
* carries ``Authorization`` or ``Cookie`` (credentialed responses may differ
  per user),
* sends ``Cache-Control: no-cache``/``no-store`` or ``Pragma: no-cache``,
* sends ``Range`` (partial content is never cached).

Response side - a response is stored only when
* its status is in the route's ``allowed_status_codes`` (default: 200),
* it has no ``Set-Cookie``,
* ``Cache-Control`` does not contain ``private``, ``no-store``, or ``no-cache``,
* ``Vary`` is not ``*`` and names only headers already in the cache key,
* the body fits in ``max_entry_bytes``.

TTL is the route's ``ttl_seconds``, lowered to ``s-maxage``/``max-age`` when
the upstream sends a smaller value (``max-age=0`` means do not store).
``Expires``, revalidation (ETag/If-None-Match), and ``stale-*`` are not
implemented. Query strings are part of the key verbatim, so ``?a=1&b=2`` and
``?b=2&a=1`` are distinct entries (safe, possibly less efficient).
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from relay.cache.lru import LRUCache
from relay.config import RouteCacheConfig
from relay.headers import RawHeaders, filter_response_headers, get_all, get_header

CACHE_HEADER = b"x-relay-cache"


@dataclass(frozen=True, slots=True)
class CachedResponse:
    status: int
    headers: RawHeaders
    body: bytes
    stored_at: float  # wall-clock seconds, used for the Age header

    def age_seconds(self, now: float | None = None) -> int:
        return max(0, int((now or time.time()) - self.stored_at))


class ResponseCache(Protocol):
    """Backend interface shared by the local LRU and the Redis cache."""

    backend_name: str

    async def get(self, key: str) -> CachedResponse | None: ...

    async def set(self, key: str, value: CachedResponse, ttl_seconds: float) -> None: ...

    async def close(self) -> None: ...


# ---------------------------------------------------------------- policy


def _cache_control(headers: RawHeaders) -> dict[str, str | None]:
    directives: dict[str, str | None] = {}
    for value in get_all(headers, b"cache-control"):
        for part in value.decode("latin-1").split(","):
            name, _, arg = part.strip().partition("=")
            if name:
                directives[name.lower()] = arg.strip('"') if arg else None
    return directives


def request_bypass_reason(method: str, headers: RawHeaders) -> str | None:
    """Why this request must not use the cache, or None if it may."""
    if method != "GET":
        return "method"
    if get_header(headers, b"authorization") is not None:
        return "authorization"
    if get_header(headers, b"cookie") is not None:
        return "cookie"
    if get_header(headers, b"range") is not None:
        return "range"
    cc = _cache_control(headers)
    if "no-cache" in cc or "no-store" in cc:
        return "client_no_cache"
    pragma = get_header(headers, b"pragma")
    if pragma is not None and b"no-cache" in pragma.lower():
        return "client_no_cache"
    return None


def response_ttl(
    status: int, headers: RawHeaders, body_size: int, cfg: RouteCacheConfig
) -> float | None:
    """TTL to store this response for, or None if it must not be cached."""
    if status not in cfg.allowed_status_codes:
        return None
    if get_header(headers, b"set-cookie") is not None:
        return None
    if body_size > cfg.max_entry_bytes:
        return None
    cc = _cache_control(headers)
    if {"private", "no-store", "no-cache"} & cc.keys():
        return None
    for vary in get_all(headers, b"vary"):
        names = {n.strip().lower() for n in vary.decode("latin-1").split(",") if n.strip()}
        if "*" in names or not names <= set(cfg.vary_headers):
            return None
    ttl = cfg.ttl_seconds
    for directive in ("s-maxage", "max-age"):
        if directive not in cc:
            continue
        raw = cc[directive]
        # Malformed (missing or non-integer value): do not guess, do not cache.
        if raw is None or not raw.isdigit():
            return None
        ttl = min(ttl, float(int(raw)))
        break  # s-maxage takes precedence for shared caches
    return ttl if ttl > 0 else None


def cache_key(
    route_id: str, raw_path: str, query: str, headers: RawHeaders, vary_headers: tuple[str, ...]
) -> str:
    """Deterministic key: route, method, path, query, and selected request headers.

    Hashed so keys have a fixed size whatever the URL length (and so Redis keys
    never contain raw paths).
    """
    parts = [route_id, "GET", raw_path, query]
    for name in vary_headers:
        value = get_header(headers, name.encode("latin-1"))
        parts.append(f"{name}={value.decode('latin-1') if value is not None else ''}")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def cacheable_headers(headers: RawHeaders) -> RawHeaders:
    """Headers worth storing: end-to-end only, minus per-response values."""
    drop = {b"date", b"age", CACHE_HEADER}
    return [(k, v) for k, v in filter_response_headers(headers) if k.lower() not in drop]


# --------------------------------------------------------------- backend


@dataclass(slots=True)
class _Entry:
    response: CachedResponse
    expires_at: float  # monotonic


class LocalResponseCache:
    """Bounded in-process cache: the custom LRU plus per-entry TTL.

    Memory is bounded by ``max_entries * max_entry_bytes`` (plus headers).
    Each gateway process has its own copy; use the Redis backend to share.
    """

    backend_name = "local"

    def __init__(
        self,
        max_entries: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        on_evict: Callable[[], None] | None = None,
    ) -> None:
        self._clock = clock
        self._on_evict = on_evict
        self._lru: LRUCache[str, _Entry] = LRUCache(max_entries, on_evict=self._evicted)

    def _evicted(self, _key: str, _entry: _Entry) -> None:
        if self._on_evict is not None:
            self._on_evict()

    @property
    def lru(self) -> LRUCache[str, _Entry]:
        return self._lru

    async def get(self, key: str) -> CachedResponse | None:
        entry = self._lru.get(key)
        if entry is None:
            return None
        if self._clock() >= entry.expires_at:
            self._lru.delete(key)  # lazy expiry; not counted as an eviction
            return None
        return entry.response

    async def set(self, key: str, value: CachedResponse, ttl_seconds: float) -> None:
        self._lru.put(key, _Entry(value, self._clock() + ttl_seconds))

    async def close(self) -> None:
        self._lru.clear()
