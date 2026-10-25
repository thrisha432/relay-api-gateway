"""Retry budget: cap retries as a fraction of recent traffic.

``max_attempts`` bounds retries *per request*, but not in aggregate: if an
upstream fails every other request, the circuit breaker (which needs
consecutive failures) never opens, and every failed request is retried. Under
a brownout that adds up to ``max_attempts - 1`` extra upstream requests per
client request, exactly when the upstream is struggling.

The budget (the "retry budget" pattern used by Finagle and Envoy) is a token
balance:
* every client request deposits ``ratio`` tokens (e.g. 0.2),
* a reserve trickles in at ``min_retries_per_second``, so low-traffic routes
  can still retry occasionally,
* every retry withdraws 1 token and is refused when the balance is below 1,
* the balance is capped at ``max_balance`` so idle time cannot bank a huge
  burst of retries.

Long run: retries <= ratio x requests + min_retries_per_second x elapsed.
With ratio 0.2, retries can add at most ~20% extra upstream load.

Per route, per process; synchronous methods (no awaits), so it is atomic on
the event loop.
"""

from __future__ import annotations

import time
from collections.abc import Callable


class RetryBudget:
    def __init__(
        self,
        ratio: float,
        min_retries_per_second: float,
        max_balance: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ratio = ratio
        self._min_rate = min_retries_per_second
        self._max = max_balance
        self._clock = clock
        self._balance = max_balance
        self._last = clock()

    @property
    def balance(self) -> float:
        self._refill()
        return self._balance

    def record_request(self) -> None:
        """Deposit for one client request (call once per request, not per attempt)."""
        self._refill()
        self._balance = min(self._max, self._balance + self._ratio)

    def try_spend(self) -> bool:
        """Withdraw one retry if the budget allows it."""
        self._refill()
        if self._balance >= 1.0:
            self._balance -= 1.0
            return True
        return False

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._balance = min(self._max, self._balance + elapsed * self._min_rate)
