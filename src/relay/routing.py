"""Route matching.

Contract:
* A request path matches a route when it equals the route's ``path_prefix`` or
  continues it on a segment boundary. ``/api/catalog`` matches ``/api/catalog``,
  ``/api/catalog/`` and ``/api/catalog/items`` but NOT ``/api/catalogue``.
* When several prefixes match, the longest one wins (most specific route).
* Matching is done on the raw (still percent-encoded) path, exactly as the
  client sent it. ``/api/%63atalog`` therefore does not match ``/api/catalog``;
  this is deterministic and avoids decode/re-encode ambiguity.
* Dot segments (``.``/``..``, including encoded forms like ``%2e%2e``) are
  rejected before matching so a request cannot climb out of its route prefix
  on an upstream that normalizes paths.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import unquote

from relay.config import RouteConfig


class InvalidPathError(ValueError):
    """The request path is malformed or unsafe to forward."""


@dataclass(frozen=True, slots=True)
class RouteMatch:
    route: RouteConfig
    # Path to send upstream (raw/encoded form, always starts with "/").
    upstream_path: str


def validate_raw_path(raw_path: str) -> None:
    """Reject paths that are not origin-form or that contain dot segments."""
    if not raw_path.startswith("/"):
        raise InvalidPathError("path must start with '/'")
    if any(ch in raw_path for ch in ("\\", "\x00", "#")) or any(
        ord(ch) < 0x21 or ord(ch) > 0x7E for ch in raw_path
    ):
        raise InvalidPathError("path contains forbidden characters")
    for segment in raw_path.split("/"):
        decoded = unquote(segment)
        # Split again: an encoded slash (%2F) can hide a dot segment ("..%2F").
        if any(part in (".", "..") for part in decoded.split("/")):
            raise InvalidPathError("path contains dot segments")
        if "\x00" in decoded or "\\" in decoded:
            raise InvalidPathError("path contains forbidden encoded characters")


class Router:
    """Longest-prefix router over a fixed, validated route table."""

    def __init__(self, routes: Iterable[RouteConfig]) -> None:
        # Sorted longest-first so the first hit is the most specific route.
        self._routes = sorted(routes, key=lambda r: len(r.path_prefix), reverse=True)

    def match(self, raw_path: str) -> RouteMatch | None:
        for route in self._routes:
            prefix = route.path_prefix
            if raw_path == prefix or raw_path.startswith(prefix + "/"):
                upstream_path = raw_path
                if route.strip_prefix:
                    upstream_path = raw_path[len(prefix) :] or "/"
                return RouteMatch(route=route, upstream_path=upstream_path)
        return None
