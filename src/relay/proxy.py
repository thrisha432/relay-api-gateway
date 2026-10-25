"""The gateway request pipeline.

Pipeline order (see docs/ARCHITECTURE.md):
 1. Validate path/method and enforce the request body limit.
 2. Match a configured route (404 otherwise).
 3. Pick a healthy upstream (round-robin over instances that pass health checks).
 4. Forward with explicit timeouts.
 5. Return the response with hop-by-hop headers removed.
"""

from __future__ import annotations

import asyncio
from enum import StrEnum

import httpx
from starlette.requests import Request
from starlette.responses import Response

from relay.client_identity import ClientIdentityResolver
from relay.config import RelayConfig, RouteConfig, UpstreamConfig
from relay.errors import (
    INVALID_REQUEST,
    METHOD_NOT_ALLOWED,
    NO_HEALTHY_UPSTREAM,
    PAYLOAD_TOO_LARGE,
    ROUTE_NOT_FOUND,
    UPSTREAM_BAD_RESPONSE,
    UPSTREAM_CONNECTION_ERROR,
    UPSTREAM_RESPONSE_TOO_LARGE,
    UPSTREAM_TIMEOUT,
    ErrorSpec,
    GatewayError,
    error_response,
)
from relay.headers import (
    RawHeaders,
    filter_request_headers,
    filter_response_headers,
    get_all,
    get_header,
)
from relay.health import HealthChecker, HealthRegistry
from relay.load_balancer import RoundRobinBalancer
from relay.logging_config import get_logger
from relay.models import RequestContext, UpstreamResponse
from relay.routing import InvalidPathError, Router, validate_raw_path

logger = get_logger("proxy")

# TRACE and CONNECT are never proxied (cross-site tracing / tunnelling).
PROXY_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})


class FailureKind(StrEnum):
    TIMEOUT = "timeout"
    CONNECT = "connect_error"
    BAD_RESPONSE = "bad_response"
    TOO_LARGE = "too_large"


_FAILURE_ERRORS: dict[FailureKind, ErrorSpec] = {
    FailureKind.TIMEOUT: UPSTREAM_TIMEOUT,
    FailureKind.CONNECT: UPSTREAM_CONNECTION_ERROR,
    FailureKind.BAD_RESPONSE: UPSTREAM_BAD_RESPONSE,
    FailureKind.TOO_LARGE: UPSTREAM_RESPONSE_TOO_LARGE,
}


class UpstreamFailure(Exception):
    """A transport-level failure talking to one upstream instance."""

    def __init__(self, kind: FailureKind) -> None:
        super().__init__(kind.value)
        self.kind = kind


class Gateway:
    """Owns the shared HTTP client and runs the proxy pipeline."""

    def __init__(self, config: RelayConfig, *, http_client: httpx.AsyncClient | None = None):
        self.config = config
        self.router = Router(config.routes)
        self.identity = ClientIdentityResolver(config.server.trusted_proxies)
        self.health = HealthRegistry(config.routes)
        self.health_checker = HealthChecker(config.routes, self.health)
        self.balancers = {r.id: RoundRobinBalancer(r.upstreams) for r in config.routes}
        self._client = http_client
        self._owns_client = http_client is None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Gateway.start() has not been called")
        return self._client

    async def start(self) -> None:
        if self._client is None:
            # One pooled client for the process lifetime. trust_env=False: never
            # pick up HTTP(S)_PROXY from the environment; redirects are returned
            # to the client rather than followed by the gateway.
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
                follow_redirects=False,
                trust_env=False,
            )
        await self.health_checker.start()

    async def close(self) -> None:
        await self.health_checker.stop()
        if self._client is not None and self._owns_client:
            await self._client.aclose()
        self._client = None

    # ------------------------------------------------------------- operations

    def readiness(self) -> dict[str, object]:
        checks: dict[str, object] = {
            "health_checks_started": self.health_checker.first_round_complete()
        }
        routes_ok = {
            r.id: any(self.health.is_healthy(r.id, u.id) for u in r.upstreams)
            for r in self.config.routes
        }
        checks["routes_with_healthy_upstream"] = routes_ok
        ready = bool(checks["health_checks_started"]) and all(routes_ok.values())
        return {"status": "ready" if ready else "not_ready", "checks": checks}

    def route_summary(self) -> list[dict[str, object]]:
        """Route table for local debugging. Upstream URLs are deliberately omitted."""
        return [
            {
                "id": r.id,
                "path_prefix": r.path_prefix,
                "strip_prefix": r.strip_prefix,
                "upstreams": [
                    {"id": u.id, "healthy": self.health.is_healthy(r.id, u.id)} for u in r.upstreams
                ],
                "rate_limited": r.rate_limit is not None,
                "cache_enabled": r.cache.enabled,
                "max_attempts": r.retries.max_attempts,
            }
            for r in self.config.routes
        ]

    # ------------------------------------------------------------------ pipeline

    async def handle(self, request: Request) -> Response:
        ctx: RequestContext = request.scope["state"]["relay"]
        try:
            return await self._handle(request, ctx)
        except GatewayError as exc:
            ctx.error_code = exc.spec.code
            return error_response(exc.spec, ctx.request_id, exc.headers)

    async def _handle(self, request: Request, ctx: RequestContext) -> Response:
        method = request.method.upper()
        raw_path = self._raw_path(request)
        if method not in PROXY_METHODS:
            raise GatewayError(METHOD_NOT_ALLOWED)

        match = self.router.match(raw_path)
        if match is None:
            raise GatewayError(ROUTE_NOT_FOUND)
        route = match.route
        ctx.route_id = route.id

        body = await self._read_body(request)
        query = request.scope.get("query_string", b"").decode("latin-1")
        headers = self._upstream_headers(request, ctx)

        candidates = self.balancers[route.id].candidates(
            lambda u: self.health.is_healthy(route.id, u.id)
        )
        if not candidates:
            raise GatewayError(NO_HEALTHY_UPSTREAM)
        upstream = candidates[0]
        ctx.upstream_id = upstream.id
        ctx.attempts = 1
        try:
            result = await self._send_once(
                route, upstream, method, match.upstream_path, query, headers, body
            )
        except UpstreamFailure as failure:
            raise GatewayError(_FAILURE_ERRORS[failure.kind]) from None
        return build_client_response(result, method)

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _raw_path(request: Request) -> str:
        raw = request.scope.get("raw_path") or request.scope["path"].encode("utf-8")
        try:
            raw_path = raw.decode("ascii")
            validate_raw_path(raw_path)
        except (UnicodeDecodeError, InvalidPathError):
            raise GatewayError(INVALID_REQUEST) from None
        return raw_path

    async def _read_body(self, request: Request) -> bytes:
        """Read the request body, enforcing max_request_body_bytes while streaming."""
        limit = self.config.server.max_request_body_bytes
        declared = request.headers.get("content-length")
        if declared is not None:
            if not declared.isdigit():
                raise GatewayError(INVALID_REQUEST)
            if int(declared) > limit:
                raise GatewayError(PAYLOAD_TOO_LARGE)
        chunks: list[bytes] = []
        total = 0
        async for chunk in request.stream():
            total += len(chunk)
            if total > limit:
                raise GatewayError(PAYLOAD_TOO_LARGE)
            chunks.append(chunk)
        return b"".join(chunks)

    def _upstream_headers(self, request: Request, ctx: RequestContext) -> RawHeaders:
        raw = list(request.headers.raw)
        headers = filter_request_headers(raw)
        peer = request.client.host if request.client else None
        prior = [v.decode("latin-1") for v in get_all(raw, b"x-forwarded-for")]
        xff = self.identity.forwarded_for_header(peer, prior)
        headers.append((b"x-forwarded-for", xff.encode("latin-1")))
        headers.append((b"x-forwarded-proto", request.url.scheme.encode("ascii")))
        host = get_header(raw, b"host")
        if host:
            headers.append((b"x-forwarded-host", host))
        headers.append((b"x-request-id", ctx.request_id.encode("ascii")))
        return headers

    async def _send_once(
        self,
        route: RouteConfig,
        upstream: UpstreamConfig,
        method: str,
        path: str,
        query: str,
        headers: RawHeaders,
        body: bytes,
        budget_seconds: float | None = None,
    ) -> UpstreamResponse:
        """One upstream attempt with bounded time and response size."""
        timeouts = route.timeouts
        budget = min(timeouts.attempt_seconds, budget_seconds or timeouts.attempt_seconds)
        url = f"{upstream.url}{path}" + (f"?{query}" if query else "")
        request = self.client.build_request(
            method,
            url,
            headers=headers,
            content=body or None,
            timeout=httpx.Timeout(budget, connect=min(timeouts.connect_seconds, budget)),
        )
        limit = self.config.server.max_response_body_bytes
        try:
            async with asyncio.timeout(budget):
                response = await self.client.send(request, stream=True)
                try:
                    declared = response.headers.get("content-length")
                    if declared and declared.isdigit() and int(declared) > limit:
                        raise UpstreamFailure(FailureKind.TOO_LARGE)
                    chunks: list[bytes] = []
                    total = 0
                    # aiter_raw: pass bytes through untouched (no gzip decoding),
                    # so Content-Encoding stays valid for the client.
                    async for chunk in response.aiter_raw():
                        total += len(chunk)
                        if total > limit:
                            raise UpstreamFailure(FailureKind.TOO_LARGE)
                        chunks.append(chunk)
                finally:
                    await response.aclose()
        except UpstreamFailure:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise UpstreamFailure(FailureKind.TIMEOUT) from None
        except httpx.ConnectError:
            raise UpstreamFailure(FailureKind.CONNECT) from None
        except httpx.HTTPError:
            # RemoteProtocolError, malformed responses, etc.
            raise UpstreamFailure(FailureKind.BAD_RESPONSE) from None
        return UpstreamResponse(
            status=response.status_code,
            headers=list(response.headers.raw),
            body=b"".join(chunks),
            upstream_id=upstream.id,
        )


def build_client_response(upstream: UpstreamResponse, method: str) -> Response:
    """Turn a buffered upstream response into a client response.

    Repeated headers (e.g. several Set-Cookie) are preserved by writing raw
    headers directly. Content-Length is recomputed for the buffered body,
    except for HEAD where the upstream's declared length is passed through.
    """
    response = Response(content=upstream.body, status_code=upstream.status)
    if method == "HEAD":
        declared = get_header(upstream.headers, b"content-length")
        response.raw_headers = [(b"content-length", declared)] if declared else []
    response.raw_headers.extend(filter_response_headers(upstream.headers))
    return response
