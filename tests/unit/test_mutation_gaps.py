"""Tests added after mutation testing showed the suite could not detect these
changes (see docs/QUALITY.md). Each test names the mutation it kills."""

import asyncio
import gc

import pytest

from relay.cache.response_cache import CachedResponse, LocalResponseCache, response_ttl
from relay.cache.singleflight import SingleFlight
from relay.client_identity import ClientIdentityResolver
from relay.config import CircuitBreakerConfig, RouteCacheConfig
from relay.resilience.circuit_breaker import BreakerState, CircuitBreaker
from relay.resilience.timeouts import Deadline
from relay.routing import InvalidPathError, validate_raw_path

CFG = RouteCacheConfig(enabled=True, ttl_seconds=30, max_entry_bytes=100)


def test_body_exactly_at_size_limit_is_cacheable() -> None:  # kills `>` -> `>=`
    assert response_ttl(200, [], 100, CFG) == 30
    assert response_ttl(200, [], 101, CFG) is None


def test_one_second_max_age_is_cacheable() -> None:  # kills `ttl > 0` -> `ttl > 1`
    assert response_ttl(200, [(b"cache-control", b"max-age=1")], 10, CFG) == 1


@pytest.mark.parametrize(
    ("value", "ttl"),
    [
        (b"max-age=60,no-store", None),  # no spaces: must still split on commas
        (b"public,max-age=5", 5),
        (b'max-age="7"', 7),  # quoted value
        (b"max-age", None),  # malformed: no value
        (b"max-age=-1", None),
        (b"max-age=1.5", None),
    ],
)
def test_cache_control_parsing_edge_cases(value: bytes, ttl) -> None:
    assert response_ttl(200, [(b"cache-control", value)], 10, CFG) == ttl


def test_fresh_entry_reports_age_zero() -> None:  # kills max(0, ..) -> max(1, ..)
    assert CachedResponse(200, [], b"", stored_at=100.0).age_seconds(now=100.4) == 0


async def test_entry_expires_exactly_at_its_deadline() -> None:  # kills `>=` -> `>`
    now = [0.0]
    cache = LocalResponseCache(4, clock=lambda: now[0])
    await cache.set("k", CachedResponse(200, [], b"x", 0), ttl_seconds=10)
    now[0] = 10.0
    assert await cache.get("k") is None


def test_recovered_breaker_needs_full_threshold_to_reopen() -> None:
    # kills `_consecutive_failures = 0` -> `= 1` in _transition
    now = [0.0]
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout_seconds=1),
        clock=lambda: now[0],
    )
    for _ in range(2):
        breaker.record_failure(breaker.try_acquire())
    now[0] += 1
    breaker.record_success(breaker.try_acquire())  # half-open probe closes it
    assert breaker.state is BreakerState.CLOSED
    breaker.record_failure(breaker.try_acquire())
    assert breaker.state is BreakerState.CLOSED  # one failure is not enough
    breaker.record_failure(breaker.try_acquire())
    assert breaker.state is BreakerState.OPEN


def test_deadline_not_expired_with_time_left() -> None:  # kills `<= 0` -> `<= 1`
    now = [0.0]
    deadline = Deadline(5.0, clock=lambda: now[0])
    now[0] = 4.5
    assert not deadline.expired()


@pytest.mark.parametrize("path", ["/api/a!b", "/api/~user", "/api/a-b_c.d"])
def test_printable_boundary_characters_are_allowed(path: str) -> None:
    validate_raw_path(path)  # kills `< 0x21` -> `<= 0x21` and `> 0x7E` -> `>= 0x7E`


@pytest.mark.parametrize("path", ["/api/a\x7fb", "/api/a#frag", "/api/a\x20b"])
def test_del_fragment_and_space_are_rejected(path: str) -> None:
    with pytest.raises(InvalidPathError):  # kills `> 0x7E` -> `> 127`, '#' removal
        validate_raw_path(path)


def test_forwarded_for_without_spaces() -> None:  # kills split(",") -> split()
    resolver = ClientIdentityResolver(["10.0.0.0/8"])
    assert resolver.resolve("10.0.0.7", ["6.6.6.6,203.0.113.9"]) == "203.0.113.9"


def test_trusted_proxy_cidr_with_host_bits_is_accepted() -> None:  # kills strict=True
    assert ClientIdentityResolver(["10.0.0.7/8"]).is_trusted("10.200.1.1")


async def test_singleflight_survives_cancellation_of_the_shared_task() -> None:
    # kills `if not task.cancelled()` -> `if task.cancelled()`: calling
    # task.exception() on a cancelled task would raise inside the callback.
    errors: list[dict] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    sf: SingleFlight[str, int] = SingleFlight()
    started = asyncio.Event()

    async def slow() -> int:
        started.set()
        await asyncio.sleep(10)
        return 1

    waiter = asyncio.create_task(sf.do("k", slow))
    await started.wait()
    sf._inflight["k"].cancel()  # white-box: cancel the shared task itself
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await asyncio.sleep(0)
    assert errors == []
    assert len(sf) == 0


async def test_singleflight_marks_abandoned_failures_retrieved() -> None:
    errors: list[dict] = []
    asyncio.get_running_loop().set_exception_handler(lambda _loop, ctx: errors.append(ctx))
    sf: SingleFlight[str, int] = SingleFlight()

    async def boom() -> int:
        await asyncio.sleep(0.01)
        raise RuntimeError("nobody listening")

    waiter = asyncio.create_task(sf.do("k", boom))
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.sleep(0.05)
    gc.collect()  # unretrieved exceptions are reported when the task is collected
    await asyncio.sleep(0)
    assert not [e for e in errors if "never retrieved" in str(e.get("message", ""))]
