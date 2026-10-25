import asyncio

import pytest

from relay.cache.singleflight import SingleFlight


async def test_concurrent_callers_share_one_execution() -> None:
    sf: SingleFlight[str, int] = SingleFlight()
    calls = 0

    async def fetch() -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return 42

    results = await asyncio.gather(*(sf.do("k", fetch) for _ in range(25)))
    assert calls == 1
    assert [value for value, _ in results] == [42] * 25
    assert sum(shared for _, shared in results) == 24  # one leader
    assert len(sf) == 0  # entry removed once finished


async def test_different_keys_do_not_share() -> None:
    sf: SingleFlight[str, str] = SingleFlight()

    async def fetch(key: str) -> str:
        await asyncio.sleep(0.01)
        return key

    results = await asyncio.gather(sf.do("a", lambda: fetch("a")), sf.do("b", lambda: fetch("b")))
    assert results == [("a", False), ("b", False)]


async def test_sequential_calls_fetch_again() -> None:
    sf: SingleFlight[str, int] = SingleFlight()
    counter = iter(range(10))

    async def fetch() -> int:
        return next(counter)

    assert await sf.do("k", fetch) == (0, False)
    assert await sf.do("k", fetch) == (1, False)


async def test_exceptions_reach_every_waiter() -> None:
    sf: SingleFlight[str, int] = SingleFlight()

    async def boom() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("upstream failed")

    results = await asyncio.gather(*(sf.do("k", boom) for _ in range(5)), return_exceptions=True)
    assert all(isinstance(r, ValueError) for r in results)
    assert len(sf) == 0


async def test_cancelled_leader_does_not_cancel_followers() -> None:
    sf: SingleFlight[str, int] = SingleFlight()

    async def slow() -> int:
        await asyncio.sleep(0.1)
        return 7

    leader = asyncio.create_task(sf.do("k", slow))
    await asyncio.sleep(0.01)
    follower = asyncio.create_task(sf.do("k", slow))
    await asyncio.sleep(0.01)
    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader
    assert await follower == (7, True)


async def test_abandoned_failure_is_not_reported_as_unretrieved(caplog) -> None:
    sf: SingleFlight[str, int] = SingleFlight()

    async def boom() -> int:
        await asyncio.sleep(0.01)
        raise RuntimeError("nobody is listening")

    waiter = asyncio.create_task(sf.do("k", boom))
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.sleep(0.05)
    assert "never retrieved" not in caplog.text
