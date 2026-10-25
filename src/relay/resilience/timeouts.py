"""Request deadlines.

Relay uses three layers of time limits:
* ``connect_seconds``: TCP connect to the upstream (httpx connect timeout).
* ``attempt_seconds``: one upstream attempt end to end (asyncio.timeout around
  the attempt, plus httpx read/write/pool timeouts).
* ``request_seconds``: the whole client request, across every retry and
  backoff. Each attempt's budget is min(attempt_seconds, time left).
"""

from __future__ import annotations

import time
from collections.abc import Callable

# Do not start an attempt with less time than this; it could not succeed and
# would only add load.
MIN_ATTEMPT_SECONDS = 0.01


class Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._expires_at = clock() + seconds

    def remaining(self) -> float:
        return max(0.0, self._expires_at - self._clock())

    def expired(self) -> bool:
        return self.remaining() <= 0

    def can_fit(self, delay: float) -> bool:
        """True if after sleeping ``delay`` a meaningful attempt still fits."""
        return self.remaining() - delay >= MIN_ATTEMPT_SECONDS
