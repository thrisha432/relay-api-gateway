"""Stable JSON error contract.

Every error Relay itself generates has the shape::

    {"error": {"code": "...", "message": "...", "request_id": "..."}}

``code`` values are part of the public contract; messages are human-readable
and never contain exception text, stack traces, or upstream URLs.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from starlette.responses import JSONResponse


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    status: int
    code: str
    message: str


INVALID_REQUEST = ErrorSpec(400, "invalid_request", "The request path or headers are malformed.")
ROUTE_NOT_FOUND = ErrorSpec(404, "route_not_found", "No configured route matches this path.")
METHOD_NOT_ALLOWED = ErrorSpec(405, "method_not_allowed", "This HTTP method is not supported.")
PAYLOAD_TOO_LARGE = ErrorSpec(413, "payload_too_large", "The request body exceeds the limit.")
RATE_LIMITED = ErrorSpec(429, "rate_limited", "Rate limit exceeded. Retry later.")
UPSTREAM_CONNECTION_ERROR = ErrorSpec(
    502, "upstream_connection_error", "The upstream service could not be reached."
)
UPSTREAM_BAD_RESPONSE = ErrorSpec(
    502, "upstream_bad_response", "The upstream service returned an invalid response."
)
UPSTREAM_RESPONSE_TOO_LARGE = ErrorSpec(
    502, "upstream_response_too_large", "The upstream response exceeds the size limit."
)
NO_HEALTHY_UPSTREAM = ErrorSpec(503, "upstream_unavailable", "No healthy upstream is available.")
CIRCUIT_OPEN = ErrorSpec(
    503, "circuit_open", "The upstream is temporarily unavailable (circuit open)."
)
RATE_LIMITER_UNAVAILABLE = ErrorSpec(
    503, "rate_limiter_unavailable", "Rate limiting is temporarily unavailable."
)
UPSTREAM_TIMEOUT = ErrorSpec(504, "upstream_timeout", "The upstream service timed out.")
INTERNAL_ERROR = ErrorSpec(500, "internal_error", "An internal gateway error occurred.")


@dataclass(slots=True)
class GatewayError(Exception):
    """Raised inside the request pipeline; converted to a JSON response once."""

    spec: ErrorSpec
    headers: dict[str, str] = field(default_factory=dict)


def error_response(
    spec: ErrorSpec, request_id: str, headers: dict[str, str] | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=spec.status,
        content={"error": {"code": spec.code, "message": spec.message, "request_id": request_id}},
        headers={"cache-control": "no-store", **(headers or {})},
    )
