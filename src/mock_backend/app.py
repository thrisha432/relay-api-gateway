"""Mock upstream service.

Every non-control path echoes the request as JSON so tests can assert exactly
what Relay forwarded. Behaviour can be shaped per request with query params
(``delay_ms``, ``status``, ``set_cookie``, ``cache_control``, ``size``) or
globally through the ``/__mock/*`` control endpoints, which is how tests and
the demo inject failures. This service is a test fixture: it has no auth and
must only be reachable on a private network.
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from dataclasses import dataclass, field

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

MAX_DELAY_MS = 30_000
MAX_SIZE_BYTES = 20 * 1024 * 1024


@dataclass
class MockState:
    instance: str
    healthy: bool = True
    delay_ms: int = 0
    fail_status: int | None = None
    requests: Counter[str] = field(default_factory=Counter)

    def reset(self) -> None:
        self.healthy = True
        self.delay_ms = 0
        self.fail_status = None
        self.requests.clear()


def _bounded_int(raw: str | None, upper: int) -> int:
    try:
        return max(0, min(upper, int(raw))) if raw is not None else 0
    except ValueError:
        return 0


def create_mock_app(instance: str | None = None) -> Starlette:
    state = MockState(instance=instance or os.environ.get("MOCK_INSTANCE_NAME", "mock"))

    async def health(_: Request) -> Response:
        if not state.healthy:
            return JSONResponse({"status": "unhealthy", "instance": state.instance}, 503)
        return JSONResponse({"status": "ok", "instance": state.instance})

    async def configure(request: Request) -> Response:
        body = await request.json()
        if "healthy" in body:
            state.healthy = bool(body["healthy"])
        if "delay_ms" in body:
            state.delay_ms = _bounded_int(str(body["delay_ms"]), MAX_DELAY_MS)
        if "fail_status" in body:
            state.fail_status = int(body["fail_status"]) if body["fail_status"] else None
        return JSONResponse(_describe(state))

    async def reset(_: Request) -> Response:
        state.reset()
        return JSONResponse(_describe(state))

    async def stats(_: Request) -> Response:
        return JSONResponse(_describe(state))

    async def echo(request: Request) -> Response:
        state.requests[request.method] += 1
        params = request.query_params
        delay = _bounded_int(params.get("delay_ms"), MAX_DELAY_MS) or state.delay_ms
        if delay:
            await asyncio.sleep(delay / 1000)
        if state.fail_status:
            return JSONResponse(
                {"error": "injected", "instance": state.instance}, state.fail_status
            )

        status = _bounded_int(params.get("status"), 599) or 200
        headers: dict[str, str] = {"x-mock-instance": state.instance}
        if params.get("set_cookie"):
            headers["set-cookie"] = "session=mock; HttpOnly"
        if params.get("cache_control"):
            headers["cache-control"] = params["cache_control"]

        size = _bounded_int(params.get("size"), MAX_SIZE_BYTES)
        if size:
            return PlainTextResponse("x" * size, status_code=status, headers=headers)

        body = await request.body()
        return JSONResponse(
            {
                "instance": state.instance,
                "method": request.method,
                "path": request.url.path,
                "raw_path": request.scope.get("raw_path", b"").decode("latin-1"),
                "query": request.url.query,
                "headers": {k: v for k, v in request.headers.items()},
                "body": body.decode("utf-8", errors="replace"),
                "body_bytes": len(body),
            },
            status_code=status,
            headers=headers,
        )

    all_methods = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
    return Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/__mock/config", configure, methods=["POST"]),
            Route("/__mock/reset", reset, methods=["POST"]),
            Route("/__mock/stats", stats, methods=["GET"]),
            Route("/{path:path}", echo, methods=all_methods),
        ]
    )


def _describe(state: MockState) -> dict[str, object]:
    return {
        "instance": state.instance,
        "healthy": state.healthy,
        "delay_ms": state.delay_ms,
        "fail_status": state.fail_status,
        "requests": dict(state.requests),
    }


app = create_mock_app()
