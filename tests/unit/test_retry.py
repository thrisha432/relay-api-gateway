import pytest

from relay.config import RetryConfig
from relay.resilience.retry import RetryPolicy
from relay.resilience.timeouts import Deadline


def policy(rng=lambda: 1.0, **overrides) -> RetryPolicy:
    cfg = {
        "max_attempts": 3,
        "backoff_base_seconds": 0.1,
        "backoff_max_seconds": 0.35,
        **overrides,
    }
    return RetryPolicy(RetryConfig(**cfg), rng=rng)


@pytest.mark.parametrize(
    ("method", "attempts"),
    [
        ("GET", 3),
        ("get", 3),
        ("HEAD", 3),
        ("OPTIONS", 3),
        ("POST", 1),
        ("PUT", 1),
        ("PATCH", 1),
        ("DELETE", 1),
    ],
)
def test_only_safe_methods_get_retries(method: str, attempts: int) -> None:
    assert policy().attempts_for(method) == attempts


def test_retryable_outcomes() -> None:
    p = policy()
    assert p.is_retryable_status(503)
    assert not p.is_retryable_status(500)  # not in the configured list
    assert not p.is_retryable_status(404)
    for kind in ("timeout", "connect_error", "bad_response"):
        assert p.is_retryable_failure(kind)
    assert not p.is_retryable_failure("too_large")


def test_backoff_is_exponential_capped_and_jittered() -> None:
    upper = policy(rng=lambda: 1.0)
    assert [upper.backoff_seconds(n) for n in (1, 2, 3, 4)] == pytest.approx([0.1, 0.2, 0.35, 0.35])
    assert policy(rng=lambda: 0.0).backoff_seconds(3) == 0.0
    assert policy(rng=lambda: 0.5).backoff_seconds(2) == pytest.approx(0.1)


def test_deadline_accounting() -> None:
    now = [100.0]
    deadline = Deadline(1.0, clock=lambda: now[0])
    assert deadline.remaining() == pytest.approx(1.0)
    assert deadline.can_fit(0.5)
    now[0] += 0.95
    assert not deadline.can_fit(0.05)
    now[0] += 1
    assert deadline.remaining() == 0.0
    assert deadline.expired()
