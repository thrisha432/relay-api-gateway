"""Retry policy: which failures may be retried, and how long to wait.

Safety rules (enforced here and by config validation):
* Only GET, HEAD and OPTIONS are ever retried. POST/PUT/PATCH/DELETE get
  exactly one attempt, because Relay has no idempotency-key contract and a
  "failed" write may already have been applied upstream.
* Only transport failures (connect error, timeout, malformed response) and the
  configured 5xx statuses are retried. An over-sized response is not retried.
* ``max_attempts`` (validated to be 1..5) caps attempts per client request.
  The proxy additionally stops when the request deadline cannot fit another
  backoff + attempt, or when the client has disconnected. Together with the
  circuit breaker this bounds the extra load retries can generate.

Backoff is exponential with "full jitter": a uniform random delay in
[0, min(max, base * 2**(n-1))]. Jitter spreads retries from many clients so
they do not arrive at the upstream in synchronized waves.
"""

from __future__ import annotations

import random
from collections.abc import Callable

from relay.config import RetryConfig

RETRYABLE_FAILURES = frozenset({"timeout", "connect_error", "bad_response"})


class RetryPolicy:
    def __init__(self, cfg: RetryConfig, rng: Callable[[], float] = random.random) -> None:
        self._cfg = cfg
        self._rng = rng

    @property
    def max_attempts(self) -> int:
        return self._cfg.max_attempts

    def attempts_for(self, method: str) -> int:
        """Total attempts allowed for this method (1 = no retries)."""
        return self._cfg.max_attempts if method.upper() in self._cfg.retry_methods else 1

    def is_retryable_status(self, status: int) -> bool:
        return status in self._cfg.retry_status_codes

    @staticmethod
    def is_retryable_failure(kind: str) -> bool:
        return kind in RETRYABLE_FAILURES

    def backoff_seconds(self, attempts_made: int) -> float:
        """Delay before the next attempt, given how many attempts already ran."""
        ceiling = min(
            self._cfg.backoff_max_seconds,
            self._cfg.backoff_base_seconds * (2 ** max(0, attempts_made - 1)),
        )
        return self._rng() * ceiling
