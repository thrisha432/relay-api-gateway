"""Small shared data types used across the request pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field

# Route label used in logs/metrics for requests that matched no route or hit
# an operational endpoint. Keeps the label set bounded.
UNMATCHED_ROUTE = "_unmatched"
OPERATIONAL_ROUTE = "_ops"


@dataclass(slots=True)
class RequestContext:
    """Per-request facts collected by the pipeline for logging and metrics.

    Stored in the ASGI scope state by the middleware; the proxy fills it in.
    """

    request_id: str
    route_id: str = UNMATCHED_ROUTE
    upstream_id: str | None = None
    attempts: int = 0
    cache: str = "none"  # none | hit | miss | bypass
    rate_limit: str = "none"  # none | allowed | limited | fail_open | fail_closed
    error_code: str | None = None


@dataclass(slots=True)
class UpstreamResponse:
    """A fully buffered upstream response (Relay caps the size; no streaming)."""

    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes
    upstream_id: str = ""
    extra: dict[str, str] = field(default_factory=dict)
