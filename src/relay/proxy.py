"""The gateway request pipeline.

Pipeline order (see docs/ARCHITECTURE.md):
 1. Validate path/method.
 2. Match a configured route (404 otherwise).
 3. Rate limit by client identity (429, or 503 if Redis is down and the
    policy is fail-closed). Done before reading the body so limited clients
    cannot make the gateway buffer request bodies.
 4. Read the body, enforcing the size limit (413).
 5. Cache lookup for eligible GET requests (a hit returns immediately).
 6. Pick a healthy upstream (round-robin over instances that pass health
    checks), skipping instances whose circuit breaker is open.
 7. Forward with per-attempt and whole-request deadlines and a response-size
    cap; retry safe methods on transient failures with jittered backoff,
    preferring an instance not yet tried.
 8. Fill the cache if the response is eligible.
 9. Return the response with hop-by-hop headers removed.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import httpx
from redis.asyncio import Redis
from starlette.requests import Request
from starlette.responses import Response

from relay.cache.redis_cache import RedisResponseCache
from relay.cache.response_cache import (
    CACHE_HEADER,
    CachedResponse,
    LocalResponseCache,
    ResponseCache,
    cache_key,
    cacheable_headers,
    request_bypass_reason,
    response_ttl,
)
from relay.cache.singleflight import SingleFlight
from relay.client_identity import ClientIdentityResolver
from relay.config import RelayConfig, RouteConfig, UpstreamConfig
from relay.errors import (
    CIRCUIT_OPEN,
    INVALID_REQUEST,
    METHOD_NOT_ALLOWED,
    NO_HEALTHY_UPSTREAM,
    PAYLOAD_TOO_LARGE,
    RATE_LIMITED,
    RATE_LIMITER_UNAVAILABLE,
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
from relay.metrics import (
    CallbackGauge,
    RelayMetrics,
    count_orphaned_pool_connections,
    status_class,
)
from relay.models import RequestContext, UpstreamResponse
from relay.rate_limiter import RateLimitDecision, RateLimiterUnavailable, RedisRateLimiter
from relay.redis_client import create_redis, ping
from relay.resilience.circuit_breaker import STATE_VALUES, BreakerState, CircuitBreaker, Permit
from relay.resilience.retry import RetryPolicy
from relay.resilience.retry_budget import RetryBudget
from relay.resilience.timeouts import Deadline
from relay.routing import InvalidPathError, RouteMatch, Router, validate_raw_path

logger = get_logger("proxy")

# Fail-open warnings are logged at most this often to avoid log floods during
# a Redis outage (the metric still counts every occurrence).
_FAIL_OPEN_LOG_INTERVAL = 10.0

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


@dataclass(slots=True)
class ProxyRequest:
    """Everything the pipeline needs about one validated, routed request."""

    ctx: RequestContext
    route: RouteConfig
    method: str
    raw_path: str
    upstream_path: str
    query: str
    client_headers: RawHeaders
    upstream_headers: RawHeaders
    body: bytes
    peer: str | None
    request: Request


class Gateway:
    """Owns the shared clients/state and runs the proxy pipeline."""

    def __init__(
        self,
        config: RelayConfig,
        *,
        metrics: RelayMetrics | None = None,
        http_client: httpx.AsyncClient | None = None,
        cache: ResponseCache | None = None,
        redis: Redis | None = None,
    ) -> None:
        self.config = config
        self.metrics = metrics or RelayMetrics()
        self.router = Router(config.routes)
        self.identity = ClientIdentityResolver(config.server.trusted_proxies)
        self.health = HealthRegistry(config.routes)
        self.health_checker = HealthChecker(
            config.routes, self.health, on_probe=self._observe_probe
        )
        self.balancers = {r.id: RoundRobinBalancer(r.upstreams) for r in config.routes}
        # The Redis cache backend needs the Redis client, so it is built in start().
        self.cache: ResponseCache | None = cache
        if cache is None and config.cache.backend == "local":
            self.cache = LocalResponseCache(
                config.cache.max_entries, on_evict=self.metrics.cache_evictions_total.inc
            )
        self._singleflight: SingleFlight[str, UpstreamResponse] = SingleFlight()
        self.retry_policies = {r.id: RetryPolicy(r.retries) for r in config.routes}
        self.retry_budgets = {
            r.id: RetryBudget(
                r.retries.budget_ratio,
                r.retries.budget_min_retries_per_second,
                r.retries.budget_max_balance,
            )
            for r in config.routes
            if r.retries.budget_enabled
        }
        self.breakers = {
            (r.id, u.id): CircuitBreaker(
                r.circuit_breaker, on_transition=self._breaker_listener(r.id, u.id)
            )
            for r in config.routes
            for u in r.upstreams
        }
        self._client = http_client
        self._owns_client = http_client is None
        self.redis = redis
        self._owns_redis = redis is None
        self.rate_limiter: RedisRateLimiter | None = None
        self._last_fail_open_log = float("-inf")
        self._lag_task: asyncio.Task[None] | None = None

        self.health.add_listener(self._observe_health_change)
        for route in config.routes:
            for upstream in route.upstreams:
                self.metrics.upstream_healthy.labels(route.id, upstream.id).set(1)
        self.metrics.register_collector(
            CallbackGauge(
                "relay_upstream_pool_unstarted_connections",
                "Pooled upstream connections assigned but not yet dialed. Normally 0 or "
                "momentarily small; a value that stays > 0 means leaked slots (AUDIT A16).",
                [],
                lambda: (
                    [((), count_orphaned_pool_connections(self._client))]
                    if self._client is not None
                    else []
                ),
            )
        )
        self.metrics.register_collector(
            CallbackGauge(
                "relay_circuit_breaker_state",
                "Circuit-breaker state per upstream: 0=closed, 1=half_open, 2=open.",
                ["route", "upstream"],
                lambda: [(key, STATE_VALUES[b.state]) for key, b in self.breakers.items()],
            )
        )

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
                limits=httpx.Limits(
                    max_connections=self.config.server.upstream_max_connections,
                    max_keepalive_connections=self.config.server.upstream_max_keepalive_connections,
                ),
                follow_redirects=False,
                trust_env=False,
            )
        if self.redis is None and self.config.redis_required:
            self.redis = create_redis(self.config.redis)
        if self.redis is not None and any(r.rate_limit for r in self.config.routes):
            self.rate_limiter = RedisRateLimiter(
                self.redis, operation_timeout_seconds=self.config.redis.operation_timeout_seconds
            )
        if self.cache is None:
            assert self.redis is not None, "config validation guarantees redis.url"
            self.cache = RedisResponseCache(
                self.redis,
                operation_timeout_seconds=self.config.redis.operation_timeout_seconds,
                on_error=lambda op: self.metrics.redis_errors_total.labels(op).inc(),
            )
        if self.config.server.max_in_flight == 0:
            logger.warning(
                "admission_control_disabled",
                extra={
                    "fields": {
                        "risk": "unbounded queueing; upstream pool waits can leak connections "
                        "(docs/AUDIT.md A16, A17)"
                    }
                },
            )
        await self.health_checker.start()
        self._lag_task = asyncio.create_task(self._watch_loop_lag(), name="relay-loop-lag")

    async def close(self) -> None:
        """Release everything start() created. Safe to call more than once."""
        if self._lag_task is not None:
            self._lag_task.cancel()
            await asyncio.gather(self._lag_task, return_exceptions=True)
            self._lag_task = None
        await self.health_checker.stop()
        if self.cache is not None:
            await self.cache.close()
        if self._client is not None and self._owns_client:
            await self._client.aclose()
        self._client = None
        if self.redis is not None and self._owns_redis:
            await self.redis.aclose()
            self.redis = None

    # ------------------------------------------------------------- operations

    async def readiness(self) -> dict[str, object]:
        """Ready when health checks have run once, every route has a healthy
        upstream, and Redis answers PING (only if the config needs Redis)."""
        started = self.health_checker.first_round_complete()
        routes_ok = {
            r.id: any(self.health.is_healthy(r.id, u.id) for u in r.upstreams)
            for r in self.config.routes
        }
        checks: dict[str, object] = {
            "health_checks_started": started,
            "routes_with_healthy_upstream": routes_ok,
        }
        ready = started and all(routes_ok.values())
        if self.redis is not None:
            redis_ok = await ping(self.redis, self.config.redis.operation_timeout_seconds)
            if not redis_ok:
                self.metrics.redis_errors_total.labels("ping").inc()
            checks["redis"] = redis_ok
            ready = ready and redis_ok
        return {"status": "ready" if ready else "not_ready", "checks": checks}

    def route_summary(self) -> list[dict[str, object]]:
        """Route table for local debugging. Upstream URLs are deliberately omitted."""
        return [
            {
                "id": r.id,
                "path_prefix": r.path_prefix,
                "strip_prefix": r.strip_prefix,
                "upstreams": [
                    {
                        "id": u.id,
                        "healthy": self.health.is_healthy(r.id, u.id),
                        "circuit": self.breakers[(r.id, u.id)].state.value,
                    }
                    for u in r.upstreams
                ],
                "rate_limited": r.rate_limit is not None,
                "cache_enabled": r.cache.enabled,
                "max_attempts": r.retries.max_attempts,
            }
            for r in self.config.routes
        ]

    # --------------------------------------------------------------- pipeline

    async def handle(self, request: Request) -> Response:
        ctx: RequestContext = request.scope["state"]["relay"]
        try:
            return await self._handle(request, ctx)
        except GatewayError as exc:
            ctx.error_code = exc.spec.code
            return error_response(exc.spec, ctx.request_id, exc.headers)

    async def _handle(self, request: Request, ctx: RequestContext) -> Response:
        match = self._match(request, ctx)
        decision = None
        if match.route.rate_limit is not None:
            decision = await self._enforce_rate_limit(request, match.route, ctx)
        preq = await self._prepare(request, ctx, match)

        key: str | None = None
        if preq.route.cache.enabled:
            key, cached = await self._cache_lookup(preq)
            if cached is not None:
                return self._cached_response(cached)

        if key is not None and preq.route.cache.coalesce:
            # Concurrent misses for one key share a single upstream fetch.
            result, shared = await self._singleflight.do(
                key, lambda: self._fetch_and_fill(preq, key)
            )
            if shared:
                ctx.cache = "coalesced"
                ctx.upstream_id = result.upstream_id
                self.metrics.cache_coalesced_total.labels(preq.route.id).inc()
        elif key is not None:
            result = await self._fetch_and_fill(preq, key)
        else:
            result = await self._forward(preq)
        response = build_client_response(result, preq.method)
        if preq.route.cache.enabled:
            response.raw_headers.append((CACHE_HEADER, ctx.cache.upper().encode()))
        if decision is not None:
            response.raw_headers.extend(_rate_limit_headers(decision))
        return response

    def _match(self, request: Request, ctx: RequestContext) -> RouteMatch:
        raw_path = self._raw_path(request)
        if request.method.upper() not in PROXY_METHODS:
            raise GatewayError(METHOD_NOT_ALLOWED)
        match = self.router.match(raw_path)
        if match is None:
            raise GatewayError(ROUTE_NOT_FOUND)
        ctx.route_id = match.route.id
        return match

    async def _prepare(
        self, request: Request, ctx: RequestContext, match: RouteMatch
    ) -> ProxyRequest:
        method = request.method.upper()
        body = await self._read_body(request)
        client_headers = list(request.headers.raw)
        peer = request.client.host if request.client else None
        return ProxyRequest(
            ctx=ctx,
            route=match.route,
            method=method,
            raw_path=self._raw_path(request),
            upstream_path=match.upstream_path,
            query=request.scope.get("query_string", b"").decode("latin-1"),
            client_headers=client_headers,
            upstream_headers=self._upstream_headers(request, client_headers, peer, ctx),
            body=body,
            peer=peer,
            request=request,
        )

    # ------------------------------------------------------------ rate limit

    async def _enforce_rate_limit(
        self, request: Request, route: RouteConfig, ctx: RequestContext
    ) -> RateLimitDecision | None:
        assert route.rate_limit is not None
        if self.rate_limiter is None:
            raise RuntimeError("rate limiter not started")
        peer = request.client.host if request.client else None
        forwarded = request.headers.getlist("x-forwarded-for")
        client_id = self.identity.resolve(peer, forwarded)
        try:
            decision = await self.rate_limiter.acquire(route.id, client_id, route.rate_limit)
        except RateLimiterUnavailable as exc:
            self.metrics.redis_errors_total.labels("rate_limit").inc()
            if self.config.redis.fail_open_for_rate_limit:
                self._record_rate_limit(ctx, route, "fail_open")
                self._log_fail_open(route, str(exc))
                return None
            self._record_rate_limit(ctx, route, "fail_closed")
            raise GatewayError(RATE_LIMITER_UNAVAILABLE) from None
        if not decision.allowed:
            self._record_rate_limit(ctx, route, "limited")
            headers = {"retry-after": str(decision.retry_after_seconds)}
            headers.update({k.decode(): v.decode() for k, v in _rate_limit_headers(decision)})
            raise GatewayError(RATE_LIMITED, headers=headers)
        self._record_rate_limit(ctx, route, "allowed")
        return decision

    def _record_rate_limit(self, ctx: RequestContext, route: RouteConfig, decision: str) -> None:
        ctx.rate_limit = decision
        self.metrics.rate_limit_decisions_total.labels(route.id, decision).inc()

    def _log_fail_open(self, route: RouteConfig, error: str) -> None:
        now = time.monotonic()
        if now - self._last_fail_open_log >= _FAIL_OPEN_LOG_INTERVAL:
            self._last_fail_open_log = now
            logger.warning(
                "rate_limit_fail_open",
                extra={"fields": {"route_id": route.id, "error_type": error}},
            )

    # ------------------------------------------------------------------ cache

    async def _cache_lookup(self, preq: ProxyRequest) -> tuple[str | None, CachedResponse | None]:
        route = preq.route
        reason = request_bypass_reason(preq.method, preq.client_headers)
        if reason is not None:
            preq.ctx.cache = "bypass"
            self.metrics.cache_requests_total.labels(route.id, "bypass").inc()
            return None, None
        key = cache_key(
            route.id, preq.raw_path, preq.query, preq.client_headers, route.cache.vary_headers
        )
        assert self.cache is not None
        cached = await self.cache.get(key)
        preq.ctx.cache = "hit" if cached is not None else "miss"
        self.metrics.cache_requests_total.labels(route.id, preq.ctx.cache).inc()
        return key, cached

    async def _fetch_and_fill(self, preq: ProxyRequest, key: str) -> UpstreamResponse:
        result = await self._forward(preq)
        await self._cache_fill(preq, key, result)
        return result

    async def _cache_fill(self, preq: ProxyRequest, key: str, result: UpstreamResponse) -> None:
        ttl = response_ttl(result.status, result.headers, len(result.body), preq.route.cache)
        if ttl is None:
            self.metrics.cache_stores_total.labels(preq.route.id, "not_cacheable").inc()
            return
        entry = CachedResponse(
            status=result.status,
            headers=cacheable_headers(result.headers),
            body=result.body,
            stored_at=time.time(),
        )
        assert self.cache is not None
        await self.cache.set(key, entry, ttl)
        self.metrics.cache_stores_total.labels(preq.route.id, "stored").inc()

    @staticmethod
    def _cached_response(cached: CachedResponse) -> Response:
        response = build_client_response(
            UpstreamResponse(status=cached.status, headers=cached.headers, body=cached.body),
            "GET",
        )
        response.raw_headers.append((b"age", str(cached.age_seconds()).encode()))
        response.raw_headers.append((CACHE_HEADER, b"HIT"))
        return response

    # -------------------------------------------------------------- upstream

    async def _forward(self, preq: ProxyRequest) -> UpstreamResponse:
        """Send the request upstream with breaker checks and bounded safe retries.

        Returns the final upstream response (which may itself be a 5xx after
        retries were exhausted; the upstream's own error body is passed through)
        or raises GatewayError for transport failures and unavailability.
        """
        route = preq.route
        policy = self.retry_policies[route.id]
        max_attempts = policy.attempts_for(preq.method)
        deadline = Deadline(route.timeouts.request_seconds)
        retry_budget = self.retry_budgets.get(route.id)
        if retry_budget is not None:
            retry_budget.record_request()
        tried: set[str] = set()
        last_response: UpstreamResponse | None = None
        last_error: ErrorSpec | None = None

        for attempt in range(1, max_attempts + 1):
            selection = self._select_upstream(route, tried)
            if isinstance(selection, ErrorSpec):
                if attempt == 1:
                    raise GatewayError(selection)
                break  # nothing left to retry against; report the previous outcome
            upstream, permit = selection
            tried.add(upstream.id)
            preq.ctx.upstream_id = upstream.id
            preq.ctx.attempts = attempt
            breaker = self.breakers[(route.id, upstream.id)]
            budget = min(route.timeouts.attempt_seconds, deadline.remaining())

            outcome_recorded = False
            try:
                result = await self._attempt(preq, upstream, budget)
            except UpstreamFailure as failure:
                if not policy.is_retryable_failure(failure.kind.value):
                    raise GatewayError(_FAILURE_ERRORS[failure.kind]) from None
                breaker.record_failure(permit)
                outcome_recorded = True
                last_response, last_error = None, _FAILURE_ERRORS[failure.kind]
                reason = failure.kind.value
            else:
                if result.status in route.circuit_breaker.failure_status_codes:
                    breaker.record_failure(permit)
                else:
                    breaker.record_success(permit)
                outcome_recorded = True
                if not policy.is_retryable_status(result.status):
                    return result
                last_response, last_error = result, None
                reason = "status"
            finally:
                # Cancellation or a non-retryable failure: hand the permit back so
                # a HALF_OPEN breaker never leaks probe slots.
                if not outcome_recorded:
                    breaker.release(permit)

            if attempt == max_attempts:
                if max_attempts > 1:
                    self.metrics.retries_exhausted_total.labels(route.id).inc()
                break
            delay = policy.backoff_seconds(attempt)
            if not deadline.can_fit(delay) or await preq.request.is_disconnected():
                break
            if retry_budget is not None and not retry_budget.try_spend():
                self.metrics.retry_budget_exhausted_total.labels(route.id).inc()
                break
            self.metrics.retries_total.labels(route.id, reason).inc()
            await asyncio.sleep(delay)

        if last_response is not None:
            return last_response
        assert last_error is not None
        raise GatewayError(last_error)

    def _select_upstream(
        self, route: RouteConfig, tried: set[str]
    ) -> tuple[UpstreamConfig, Permit] | ErrorSpec:
        """Next healthy upstream whose breaker admits a request.

        Untried instances are preferred, so a retry goes to a different
        instance when one is available.
        """
        healthy = self.balancers[route.id].candidates(
            lambda u: self.health.is_healthy(route.id, u.id)
        )
        if not healthy:
            self.metrics.no_healthy_upstream_total.labels(route.id).inc()
            return NO_HEALTHY_UPSTREAM
        ordered = [u for u in healthy if u.id not in tried] + [u for u in healthy if u.id in tried]
        for upstream in ordered:
            permit = self.breakers[(route.id, upstream.id)].try_acquire()
            if permit is not None:
                return upstream, permit
            self.metrics.circuit_rejections_total.labels(route.id, upstream.id).inc()
        return CIRCUIT_OPEN

    async def _attempt(
        self, preq: ProxyRequest, upstream: UpstreamConfig, budget: float
    ) -> UpstreamResponse:
        """One upstream attempt, instrumented with latency and outcome metrics."""
        start = time.perf_counter()
        outcome = "unknown"
        try:
            result = await self._send_once(preq, upstream, budget)
            outcome = status_class(result.status)
            return result
        except UpstreamFailure as failure:
            outcome = failure.kind.value
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            route_id = preq.route.id
            self.metrics.upstream_duration.labels(route_id, upstream.id).observe(
                time.perf_counter() - start
            )
            self.metrics.upstream_requests_total.labels(route_id, upstream.id, outcome).inc()

    async def _send_once(
        self, preq: ProxyRequest, upstream: UpstreamConfig, budget: float
    ) -> UpstreamResponse:
        """Send one request with bounded time and response size; no retries here."""
        timeouts = preq.route.timeouts
        url = f"{upstream.url}{preq.upstream_path}" + (f"?{preq.query}" if preq.query else "")
        # Every phase gets httpx's own timeout (pool wait, connect, each read and
        # write), and the whole-attempt budget is checked cooperatively after
        # the headers and between body chunks.
        #
        # Deliberately NOT asyncio.timeout around client.send(): cancelling
        # httpcore from outside can orphan a pooled connection in CONNECTING
        # state (a connection assigned to a waiting request that is cancelled
        # before it dials is never reclaimed). Under overload that leaked every
        # pool slot and turned a load spike into a permanent outage; see
        # tests/integration/test_pool_leak.py and docs/AUDIT.md (A16).
        request = self.client.build_request(
            preq.method,
            url,
            headers=preq.upstream_headers,
            content=preq.body or None,
            timeout=httpx.Timeout(
                budget, connect=min(timeouts.connect_seconds, budget), pool=budget
            ),
        )
        limit = self.config.server.max_response_body_bytes
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            response = await self.client.send(request, stream=True)
            try:
                if loop.time() - started > budget:
                    raise UpstreamFailure(FailureKind.TIMEOUT)
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
                    if loop.time() - started > budget:
                        raise UpstreamFailure(FailureKind.TIMEOUT)
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

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _raw_path(request: Request) -> str:
        raw: bytes = request.scope.get("raw_path") or request.scope["path"].encode("utf-8")
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

    def _upstream_headers(
        self, request: Request, raw: RawHeaders, peer: str | None, ctx: RequestContext
    ) -> RawHeaders:
        headers = filter_request_headers(raw)
        prior = [v.decode("latin-1") for v in get_all(raw, b"x-forwarded-for")]
        xff = self.identity.forwarded_for_header(peer, prior)
        headers.append((b"x-forwarded-for", xff.encode("latin-1")))
        headers.append((b"x-forwarded-proto", request.url.scheme.encode("ascii")))
        host = get_header(raw, b"host")
        if host:
            headers.append((b"x-forwarded-host", host))
        headers.append((b"x-request-id", ctx.request_id.encode("ascii")))
        return headers

    # ------------------------------------------------------------ observers

    async def _watch_loop_lag(self, interval: float = 0.1) -> None:
        """Measure event-loop lag: how late a short sleep wakes up.

        The worst lag seen in each one-second window is exported, so CPU
        saturation of the gateway itself is visible separately from upstream
        latency.
        """
        loop = asyncio.get_running_loop()
        worst, window_start = 0.0, loop.time()
        while True:
            before = loop.time()
            await asyncio.sleep(interval)
            worst = max(worst, loop.time() - before - interval)
            if loop.time() - window_start >= 1.0:
                self.metrics.event_loop_lag.set(worst)
                worst, window_start = 0.0, loop.time()

    def _observe_probe(self, route_id: str, upstream_id: str, ok: bool) -> None:
        result = "success" if ok else "failure"
        self.metrics.health_checks_total.labels(route_id, upstream_id, result).inc()

    def _observe_health_change(self, route_id: str, upstream_id: str, healthy: bool) -> None:
        self.metrics.upstream_healthy.labels(route_id, upstream_id).set(1 if healthy else 0)

    def _breaker_listener(
        self, route_id: str, upstream_id: str
    ) -> Callable[[BreakerState, BreakerState], None]:
        def on_transition(old: BreakerState, new: BreakerState) -> None:
            self.metrics.circuit_transitions_total.labels(
                route_id, upstream_id, old.value, new.value
            ).inc()
            logger.warning(
                "circuit_breaker_transition",
                extra={
                    "fields": {
                        "route_id": route_id,
                        "upstream_id": upstream_id,
                        "from_state": old.value,
                        "to_state": new.value,
                    }
                },
            )

        return on_transition


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


def _rate_limit_headers(decision: RateLimitDecision) -> list[tuple[bytes, bytes]]:
    return [
        (b"x-ratelimit-limit", str(decision.limit).encode()),
        (b"x-ratelimit-remaining", str(decision.remaining).encode()),
    ]
