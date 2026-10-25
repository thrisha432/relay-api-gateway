"""Active health checks.

Each route with health checks enabled gets exactly one background task (so
the task count is bounded by the config). Every interval it probes all of the
route's upstreams concurrently with a short timeout:

* 2xx  -> success; anything else (status, timeout, connection error) -> failure.
* ``unhealthy_threshold`` consecutive failures mark a healthy instance unhealthy.
* ``healthy_threshold`` consecutive successes restore an unhealthy instance.

Instances start healthy (optimistic) so the gateway can serve immediately; the
first probe round runs right at startup. Probes use a dedicated small HTTP
client so a saturated proxy connection pool cannot cause false "unhealthy"
verdicts. Tasks are created in ``start()`` and cancelled+awaited in ``stop()``,
which the app lifespan calls.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import httpx

from relay.config import HealthCheckConfig, RouteConfig, UpstreamConfig
from relay.logging_config import get_logger

logger = get_logger("health")

TransitionListener = Callable[[str, str, bool], None]  # route_id, upstream_id, healthy


@dataclass(slots=True)
class UpstreamHealth:
    healthy: bool = True
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_probe_monotonic: float | None = None

    def record(self, ok: bool, cfg: HealthCheckConfig) -> bool:
        """Apply one probe result. Returns True if healthy/unhealthy flipped."""
        self.last_probe_monotonic = time.monotonic()
        if ok:
            self.consecutive_successes += 1
            self.consecutive_failures = 0
            if not self.healthy and self.consecutive_successes >= cfg.healthy_threshold:
                self.healthy = True
                return True
        else:
            self.consecutive_failures += 1
            self.consecutive_successes = 0
            if self.healthy and self.consecutive_failures >= cfg.unhealthy_threshold:
                self.healthy = False
                return True
        return False


class HealthRegistry:
    """Health state per (route id, upstream id)."""

    def __init__(self, routes: Iterable[RouteConfig]) -> None:
        self._states = {(r.id, u.id): UpstreamHealth() for r in routes for u in r.upstreams}
        self._listeners: list[TransitionListener] = []

    def add_listener(self, listener: TransitionListener) -> None:
        self._listeners.append(listener)

    def state(self, route_id: str, upstream_id: str) -> UpstreamHealth:
        return self._states[(route_id, upstream_id)]

    def is_healthy(self, route_id: str, upstream_id: str) -> bool:
        return self._states[(route_id, upstream_id)].healthy

    def record(self, route: RouteConfig, upstream_id: str, ok: bool) -> None:
        state = self._states[(route.id, upstream_id)]
        if state.record(ok, route.health_check):
            logger.warning(
                "upstream_health_changed",
                extra={
                    "fields": {
                        "route_id": route.id,
                        "upstream_id": upstream_id,
                        "healthy": state.healthy,
                    }
                },
            )
            for listener in self._listeners:
                listener(route.id, upstream_id, state.healthy)


ProbeObserver = Callable[[str, str, bool], None]  # route_id, upstream_id, ok


class HealthChecker:
    def __init__(
        self,
        routes: Iterable[RouteConfig],
        registry: HealthRegistry,
        *,
        client: httpx.AsyncClient | None = None,
        on_probe: ProbeObserver | None = None,
    ) -> None:
        self._routes = [r for r in routes if r.health_check.enabled]
        self._registry = registry
        self._client = client
        self._owns_client = client is None
        self._on_probe = on_probe
        self._tasks: list[asyncio.Task[None]] = []
        self._first_round: dict[str, asyncio.Event] = {r.id: asyncio.Event() for r in self._routes}

    @property
    def running_tasks(self) -> int:
        return sum(1 for t in self._tasks if not t.done())

    async def start(self) -> None:
        if self._tasks:
            return
        if self._client is None:
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                follow_redirects=False,
                trust_env=False,
            )
        for route in self._routes:
            task = asyncio.create_task(self._run(route), name=f"relay-health-{route.id}")
            self._tasks.append(task)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    def first_round_complete(self) -> bool:
        return all(event.is_set() for event in self._first_round.values())

    async def check_route(self, route: RouteConfig) -> None:
        """Probe every upstream of ``route`` once, concurrently."""
        results = await asyncio.gather(*(self._probe(route, u) for u in route.upstreams))
        for upstream, ok in zip(route.upstreams, results, strict=True):
            if self._on_probe:
                self._on_probe(route.id, upstream.id, ok)
            self._registry.record(route, upstream.id, ok)

    async def _run(self, route: RouteConfig) -> None:
        interval = route.health_check.interval_seconds
        while True:
            try:
                await self.check_route(route)
            except Exception:
                # Never let one bad round kill the loop; next round retries.
                logger.exception("health_round_failed", extra={"fields": {"route_id": route.id}})
            self._first_round[route.id].set()
            await asyncio.sleep(interval)

    async def _probe(self, route: RouteConfig, upstream: UpstreamConfig) -> bool:
        assert self._client is not None
        cfg = route.health_check
        try:
            async with asyncio.timeout(cfg.timeout_seconds):
                response = await self._client.get(
                    upstream.url + cfg.path, timeout=cfg.timeout_seconds
                )
        except (TimeoutError, httpx.HTTPError):
            return False
        return 200 <= response.status_code < 300
