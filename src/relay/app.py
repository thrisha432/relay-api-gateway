"""ASGI application factory.

Operational endpoints are registered before the catch-all proxy route, and
route prefixes under /health, /metrics and /__relay are rejected at config
validation, so user routes can never shadow them.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST
from redis.asyncio import Redis
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

from relay import __version__
from relay.cache.response_cache import ResponseCache
from relay.config import RelayConfig, load_config
from relay.logging_config import configure_logging, get_logger
from relay.metrics import RelayMetrics, method_label, status_class
from relay.middleware import RequestContextMiddleware
from relay.models import OPERATIONAL_ROUTE, RequestContext
from relay.proxy import Gateway

logger = get_logger("app")


def create_app(
    config: RelayConfig | None = None,
    *,
    http_client: httpx.AsyncClient | None = None,
    cache: ResponseCache | None = None,
    redis: Redis | None = None,
) -> FastAPI:
    """Build the Relay ASGI app.

    ``http_client``, ``cache`` and ``redis`` let tests inject dependencies;
    normally the gateway creates and owns them for the app's lifetime.
    """
    config = config or load_config()
    metrics = RelayMetrics()
    gateway = Gateway(config, metrics=metrics, http_client=http_client, cache=cache, redis=redis)

    def observe(ctx: RequestContext, method: str, status: int, duration: float) -> None:
        metrics.requests_total.labels(
            ctx.route_id, method_label(method), status_class(status)
        ).inc()
        if ctx.route_id != OPERATIONAL_ROUTE:
            metrics.request_duration.labels(ctx.route_id).observe(duration)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await gateway.start()
        logger.info("startup", extra={"fields": {"routes": [r.id for r in config.routes]}})
        try:
            yield
        finally:
            await gateway.close()
            logger.info("shutdown_complete")

    app = FastAPI(
        title="Relay",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.gateway = gateway
    app.add_middleware(
        RequestContextMiddleware,
        observer=observe,
        on_start=metrics.in_flight.inc,
        on_end=metrics.in_flight.dec,
    )

    def _ops(request: Request) -> None:
        request.scope["state"]["relay"].route_id = OPERATIONAL_ROUTE

    @app.get("/health/live")
    async def live(request: Request) -> dict[str, str]:
        _ops(request)
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready(request: Request) -> Response:
        """See Gateway.readiness for the readiness policy."""
        _ops(request)
        report = await gateway.readiness()
        return JSONResponse(report, status_code=200 if report["status"] == "ready" else 503)

    @app.get("/metrics")
    async def prometheus_metrics(request: Request) -> Response:
        _ops(request)
        return Response(metrics.render(), media_type=CONTENT_TYPE_LATEST)

    if config.server.enable_route_summary:

        @app.get("/__relay/routes")
        async def routes(request: Request) -> dict[str, object]:
            _ops(request)
            return {"routes": gateway.route_summary()}

    # Mounted as a raw ASGI app (not a function endpoint) so Starlette passes
    # every method through; the gateway answers 405 itself using the JSON
    # error contract instead of Starlette's plain-text 405.
    app.router.routes.append(Route("/{path:path}", _ProxyEndpoint(gateway)))
    return app


class _ProxyEndpoint:
    def __init__(self, gateway: Gateway) -> None:
        self.gateway = gateway

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        response = await self.gateway.handle(Request(scope, receive))
        await response(scope, receive, send)


def create_app_from_env() -> FastAPI:
    """Factory for ``uvicorn --factory relay.app:create_app_from_env``."""
    configure_logging()
    return create_app(load_config())
