"""Header filtering for the proxy hop.

Headers are handled as raw ``(bytes, bytes)`` pairs end to end so values are
forwarded byte-for-byte without re-encoding, and repeated headers such as
``Set-Cookie`` keep their multiplicity.

Hop-by-hop headers (RFC 9110 section 7.6.1) describe a single connection and
must not be forwarded. Any header named in ``Connection`` is hop-by-hop too.
"""

from __future__ import annotations

from collections.abc import Iterable

RawHeaders = list[tuple[bytes, bytes]]

HOP_BY_HOP_HEADERS: frozenset[bytes] = frozenset(
    {
        b"connection",
        b"keep-alive",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    }
)

# Request headers Relay sets itself, or that httpx computes for the new hop.
# Host is dropped so httpx sends the upstream's own host (see ARCHITECTURE.md).
_REQUEST_OVERRIDDEN = frozenset(
    {
        b"host",
        b"content-length",
        b"x-forwarded-for",
        b"x-forwarded-host",
        b"x-forwarded-proto",
        b"x-request-id",
    }
)
# Response headers recomputed for the buffered body Relay returns.
_RESPONSE_OVERRIDDEN = frozenset({b"content-length", b"x-request-id"})


def connection_tokens(headers: Iterable[tuple[bytes, bytes]]) -> set[bytes]:
    """Header names listed in any ``Connection`` header (lowercased)."""
    tokens: set[bytes] = set()
    for name, value in headers:
        if name.lower() == b"connection":
            tokens.update(t.strip().lower() for t in value.split(b",") if t.strip())
    return tokens


def _filter(headers: RawHeaders, extra_drop: frozenset[bytes]) -> RawHeaders:
    drop = HOP_BY_HOP_HEADERS | extra_drop | connection_tokens(headers)
    return [(name, value) for name, value in headers if name.lower() not in drop]


def filter_request_headers(headers: RawHeaders) -> RawHeaders:
    """Client headers that are safe to forward upstream."""
    return _filter(headers, _REQUEST_OVERRIDDEN)


def filter_response_headers(headers: RawHeaders) -> RawHeaders:
    """Upstream response headers that are safe to return to the client."""
    return _filter(headers, _RESPONSE_OVERRIDDEN)


def get_header(headers: Iterable[tuple[bytes, bytes]], name: bytes) -> bytes | None:
    """First value of ``name`` (case-insensitive), or None."""
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def get_all(headers: Iterable[tuple[bytes, bytes]], name: bytes) -> list[bytes]:
    return [value for key, value in headers if key.lower() == name]
