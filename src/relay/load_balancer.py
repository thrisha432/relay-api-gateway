"""Health-aware round-robin load balancing.

Concurrency: ``candidates`` contains no ``await``, so under asyncio's single
-threaded event loop each call runs to completion without interleaving. That
makes the read-modify-write of the cursor atomic without a lock. (An
``asyncio.Lock`` would only add overhead; a ``threading.Lock`` would be needed
only if the balancer were shared across threads, which Relay never does.)

Distribution: the cursor rotates over the *currently available* instances,
so traffic is spread evenly among them. With 3 instances and one unhealthy,
the other two each get 1/2, not 2/3 and 1/3.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from relay.config import UpstreamConfig


class RoundRobinBalancer:
    def __init__(self, upstreams: Sequence[UpstreamConfig]) -> None:
        if not upstreams:
            raise ValueError("a balancer needs at least one upstream")
        self._upstreams = tuple(upstreams)
        self._counter = 0

    @property
    def upstreams(self) -> tuple[UpstreamConfig, ...]:
        return self._upstreams

    def candidates(
        self, is_available: Callable[[UpstreamConfig], bool] = lambda _: True
    ) -> list[UpstreamConfig]:
        """Available upstreams, starting with this request's round-robin pick.

        The rest of the list (in rotation order) is the failover order the
        proxy uses if the first choice's circuit breaker rejects the call or a
        safe retry needs a different instance. Returns [] if none is available.
        """
        available = [u for u in self._upstreams if is_available(u)]
        if not available:
            return []
        start = self._counter % len(available)
        self._counter += 1
        return available[start:] + available[:start]
