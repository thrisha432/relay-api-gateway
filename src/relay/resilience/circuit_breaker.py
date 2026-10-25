"""Per-upstream circuit breaker.

States::

    CLOSED --(failure_threshold consecutive failures)--> OPEN
    OPEN --(recovery_timeout elapsed, on next request)--> HALF_OPEN
    HALF_OPEN --(success_threshold probe successes)--> CLOSED
    HALF_OPEN --(any probe failure)--> OPEN

What counts as a failure is decided by the caller (the proxy): connection
errors, timeouts, malformed responses, and the configured 5xx statuses. 4xx
responses are successes: the upstream is working, the client is wrong.

Concurrency: no method awaits, so under asyncio each call is atomic and the
state/counters cannot race. Every ``try_acquire`` that returns a permit must
be followed by exactly one of ``record_success``, ``record_failure`` or
``release`` (the proxy guarantees this with try/finally, including on
cancellation). That is what prevents HALF_OPEN from getting stuck with probe
slots that are never returned.

Probe limiting: in HALF_OPEN at most ``half_open_max_probes`` requests are
in flight at once; everyone else is rejected immediately, so a burst of
traffic cannot stampede a recovering upstream.

Stale results: permits carry the breaker's ``generation``, which increments
on every transition. A result from a request that started under an older
generation (e.g. a slow request that began while CLOSED and finishes after
the breaker opened) is ignored, so it cannot corrupt the new state's counters.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from relay.config import CircuitBreakerConfig


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# Numeric encoding for the Prometheus state gauge.
STATE_VALUES = {BreakerState.CLOSED: 0, BreakerState.HALF_OPEN: 1, BreakerState.OPEN: 2}

TransitionListener = Callable[[BreakerState, BreakerState], None]


@dataclass(frozen=True, slots=True)
class Permit:
    generation: int
    probe: bool


class CircuitBreaker:
    def __init__(
        self,
        cfg: CircuitBreakerConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
        on_transition: TransitionListener | None = None,
    ) -> None:
        self._cfg = cfg
        self._clock = clock
        self._on_transition = on_transition
        self._state = BreakerState.CLOSED
        self._generation = 0
        self._consecutive_failures = 0
        self._probes_in_flight = 0
        self._probe_successes = 0
        self._opened_at = 0.0

    @property
    def state(self) -> BreakerState:
        return self._state

    @property
    def probes_in_flight(self) -> int:
        return self._probes_in_flight

    def try_acquire(self) -> Permit | None:
        """Permission to send one request, or None to fail fast."""
        if not self._cfg.enabled:
            return Permit(self._generation, probe=False)
        if self._state is BreakerState.OPEN:
            if self._clock() - self._opened_at < self._cfg.recovery_timeout_seconds:
                return None
            self._transition(BreakerState.HALF_OPEN)
        if self._state is BreakerState.HALF_OPEN:
            if self._probes_in_flight >= self._cfg.half_open_max_probes:
                return None
            self._probes_in_flight += 1
            return Permit(self._generation, probe=True)
        return Permit(self._generation, probe=False)

    def record_success(self, permit: Permit) -> None:
        if not self._cfg.enabled or permit.generation != self._generation:
            return
        if self._state is BreakerState.CLOSED:
            self._consecutive_failures = 0
        elif self._state is BreakerState.HALF_OPEN and permit.probe:
            self._probes_in_flight -= 1
            self._probe_successes += 1
            if self._probe_successes >= self._cfg.success_threshold:
                self._transition(BreakerState.CLOSED)

    def record_failure(self, permit: Permit) -> None:
        if not self._cfg.enabled or permit.generation != self._generation:
            return
        if self._state is BreakerState.CLOSED:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._cfg.failure_threshold:
                self._transition(BreakerState.OPEN)
        elif self._state is BreakerState.HALF_OPEN and permit.probe:
            self._transition(BreakerState.OPEN)

    def release(self, permit: Permit) -> None:
        """Return a permit without an outcome (cancelled, or a non-infrastructure error)."""
        if not self._cfg.enabled or permit.generation != self._generation:
            return
        if self._state is BreakerState.HALF_OPEN and permit.probe:
            self._probes_in_flight -= 1

    def _transition(self, new_state: BreakerState) -> None:
        old_state = self._state
        self._state = new_state
        self._generation += 1
        self._consecutive_failures = 0
        self._probes_in_flight = 0
        self._probe_successes = 0
        if new_state is BreakerState.OPEN:
            self._opened_at = self._clock()
        if self._on_transition is not None:
            self._on_transition(old_state, new_state)
