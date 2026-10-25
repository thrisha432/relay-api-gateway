import asyncio
import random

from relay.config import CircuitBreakerConfig
from relay.resilience.circuit_breaker import BreakerState, CircuitBreaker


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def make(clock: Clock | None = None, **overrides) -> tuple[CircuitBreaker, Clock, list]:
    clock = clock or Clock()
    transitions: list[tuple[str, str]] = []
    cfg = CircuitBreakerConfig(
        **{
            "failure_threshold": 3,
            "recovery_timeout_seconds": 10,
            "half_open_max_probes": 1,
            "success_threshold": 1,
            **overrides,
        }
    )
    breaker = CircuitBreaker(
        cfg, clock=clock, on_transition=lambda a, b: transitions.append((a.value, b.value))
    )
    return breaker, clock, transitions


def fail(breaker: CircuitBreaker, times: int = 1) -> None:
    for _ in range(times):
        permit = breaker.try_acquire()
        assert permit is not None
        breaker.record_failure(permit)


def open_breaker(breaker: CircuitBreaker, clock: Clock) -> None:
    fail(breaker, 3)
    assert breaker.state is BreakerState.OPEN


def test_closed_until_consecutive_failures_reach_threshold() -> None:
    breaker, _, transitions = make()
    fail(breaker, 2)
    ok = breaker.try_acquire()
    breaker.record_success(ok)  # resets the streak
    fail(breaker, 2)
    assert breaker.state is BreakerState.CLOSED
    fail(breaker)
    assert breaker.state is BreakerState.OPEN
    assert transitions == [("closed", "open")]


def test_open_fails_fast_until_cooldown() -> None:
    breaker, clock, _ = make()
    open_breaker(breaker, clock)
    assert breaker.try_acquire() is None
    clock.now += 9.99
    assert breaker.try_acquire() is None
    clock.now += 0.02
    probe = breaker.try_acquire()
    assert probe is not None
    assert probe.probe
    assert breaker.state is BreakerState.HALF_OPEN


def test_half_open_limits_concurrent_probes() -> None:
    breaker, clock, _ = make(half_open_max_probes=2)
    open_breaker(breaker, clock)
    clock.now += 10
    first, second = breaker.try_acquire(), breaker.try_acquire()
    assert first
    assert second
    assert breaker.try_acquire() is None
    breaker.release(first)
    assert breaker.try_acquire() is not None


def test_probe_success_closes_after_success_threshold() -> None:
    breaker, clock, transitions = make(success_threshold=2)
    open_breaker(breaker, clock)
    clock.now += 10
    breaker.record_success(breaker.try_acquire())
    assert breaker.state is BreakerState.HALF_OPEN
    breaker.record_success(breaker.try_acquire())
    assert breaker.state is BreakerState.CLOSED
    assert transitions == [("closed", "open"), ("open", "half_open"), ("half_open", "closed")]


def test_probe_failure_reopens_and_restarts_cooldown() -> None:
    breaker, clock, transitions = make()
    open_breaker(breaker, clock)
    clock.now += 10
    breaker.record_failure(breaker.try_acquire())
    assert breaker.state is BreakerState.OPEN
    clock.now += 5
    assert breaker.try_acquire() is None  # cooldown restarted at reopen time
    assert transitions[-1] == ("half_open", "open")


def test_stale_results_from_previous_state_are_ignored() -> None:
    breaker, clock, _ = make()
    slow = breaker.try_acquire()  # started while CLOSED
    open_breaker(breaker, clock)
    clock.now += 10
    probe = breaker.try_acquire()
    breaker.record_success(slow)  # stale: must not count as a probe success
    assert breaker.state is BreakerState.HALF_OPEN
    assert breaker.probes_in_flight == 1
    breaker.record_failure(slow)  # stale failure must not reopen either
    assert breaker.state is BreakerState.HALF_OPEN
    breaker.record_success(probe)
    assert breaker.state is BreakerState.CLOSED


def test_disabled_breaker_always_allows() -> None:
    breaker, _, transitions = make(enabled=False)
    fail(breaker, 50)
    assert breaker.state is BreakerState.CLOSED
    assert transitions == []


async def test_concurrent_half_open_requests_get_bounded_probes() -> None:
    breaker, clock, _ = make(half_open_max_probes=3)
    open_breaker(breaker, clock)
    clock.now += 10

    async def attempt():
        await asyncio.sleep(0)
        return breaker.try_acquire()

    permits = await asyncio.gather(*(attempt() for _ in range(100)))
    assert sum(p is not None for p in permits) == 3


def test_random_operation_sequences_never_get_stuck() -> None:
    """Property: whatever happens, once in-flight permits resolve and the
    cooldown passes, the breaker admits a request again."""
    for seed in range(200):
        rng = random.Random(seed)
        breaker, clock, _ = make(
            failure_threshold=rng.randint(1, 4),
            half_open_max_probes=rng.randint(1, 3),
            success_threshold=rng.randint(1, 3),
        )
        in_flight = []
        for _ in range(300):
            clock.now += rng.choice([0, 0, 0.5, 3, 11])
            if rng.random() < 0.5:
                permit = breaker.try_acquire()
                if permit is not None:
                    in_flight.append(permit)
            elif in_flight:
                permit = in_flight.pop(rng.randrange(len(in_flight)))
                rng.choice([breaker.record_success, breaker.record_failure, breaker.release])(
                    permit
                )
            assert 0 <= breaker.probes_in_flight <= 3
        for permit in in_flight:
            breaker.release(permit)
        clock.now += 11
        assert breaker.try_acquire() is not None, f"stuck with seed {seed}"
