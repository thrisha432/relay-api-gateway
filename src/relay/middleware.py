"""Pure-ASGI middleware: request IDs, in-flight tracking, access logs, and a
last-resort JSON 500 handler.

Pure ASGI (rather than Starlette's BaseHTTPMiddleware) avoids an extra task
per request and keeps client-disconnect/cancellation semantics intact.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Callable
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from relay.errors import HEADERS_TOO_LARGE, INTERNAL_ERROR, MISSING_HOST, error_response
from relay.logging_config import get_logger
from relay.models import RequestContext

_REQUEST_ID_RE = re.compile(rb"^[A-Za-z0-9._:-]{1,128}$")
logger = get_logger("access")

# Signature of the hook the app uses to record request metrics.
RequestObserver = Callable[[RequestContext, str, int, float], None]


def _request_id_from(scope: Scope) -> str:
    """Reuse a well-formed incoming X-Request-ID, otherwise generate one."""
    for name, value in scope.get("headers", []):
        if name == b"x-request-id" and _REQUEST_ID_RE.match(value):
            return value.decode("ascii")
    return uuid.uuid4().hex


class RequestContextMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        observer: RequestObserver | None = None,
        on_start: Callable[[], None] | None = None,
        on_end: Callable[[], None] | None = None,
    ) -> None:
        self.app = app
        self.observer = observer
        self.on_start = on_start
        self.on_end = on_end

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        ctx = RequestContext(request_id=_request_id_from(scope))
        scope.setdefault("state", {})["relay"] = ctx
        rid = ctx.request_id.encode("ascii")
        status = 500
        response_started = False
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status = message["status"]
                headers = [(k, v) for k, v in message.get("headers", []) if k != b"x-request-id"]
                headers.append((b"x-request-id", rid))
                message = {**message, "headers": headers}
            await send(message)

        if self.on_start:
            self.on_start()
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            logger.exception("unhandled_error", extra={"fields": {"request_id": ctx.request_id}})
            ctx.error_code = INTERNAL_ERROR.code
            status = INTERNAL_ERROR.status
            if not response_started:
                await _send_internal_error(send, ctx.request_id)
        finally:
            if self.on_end:
                self.on_end()
            duration = time.perf_counter() - start
            method = scope.get("method", "")
            if self.observer:
                self.observer(ctx, method, status, duration)
            _log_access(ctx, method, status, duration)


async def _send_internal_error(send: Send, request_id: str) -> None:
    body = json.dumps(
        {
            "error": {
                "code": INTERNAL_ERROR.code,
                "message": INTERNAL_ERROR.message,
                "request_id": request_id,
            }
        }
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": INTERNAL_ERROR.status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                (b"x-request-id", request_id.encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


def _log_access(ctx: RequestContext, method: str, status: int, duration: float) -> None:
    fields: dict[str, Any] = {
        "request_id": ctx.request_id,
        "route_id": ctx.route_id,
        "upstream_id": ctx.upstream_id,
        "method": method,
        "status": status,
        "duration_ms": round(duration * 1000, 3),
        "attempts": ctx.attempts,
        "retries": max(0, ctx.attempts - 1),
        "cache": ctx.cache,
        "rate_limit": ctx.rate_limit,
    }
    if ctx.error_code:
        fields["error_code"] = ctx.error_code
    logger.info("request", extra={"fields": fields})


class RequestLimitsMiddleware:
    """Reject oversized header sets (431) and HTTP/1.1 requests without Host (400).

    Runs inside RequestContextMiddleware so rejections still get a request id,
    a log line and metrics, and before routing so nothing is forwarded.
    """

    def __init__(self, app: ASGIApp, max_header_bytes: int, max_header_count: int) -> None:
        self.app = app
        self.max_header_bytes = max_header_bytes
        self.max_header_count = max_header_count

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = scope.get("headers", [])
        # Count name + value + ": " + CRLF, approximating the wire size.
        size = sum(len(name) + len(value) + 4 for name, value in headers)
        spec = None
        if len(headers) > self.max_header_count or size > self.max_header_bytes:
            spec = HEADERS_TOO_LARGE
        elif scope.get("http_version") == "1.1" and not any(n == b"host" for n, _ in headers):
            spec = MISSING_HOST
        if spec is None:
            await self.app(scope, receive, send)
            return
        ctx: RequestContext = scope["state"]["relay"]
        ctx.error_code = spec.code
        await error_response(spec, ctx.request_id)(scope, receive, send)
