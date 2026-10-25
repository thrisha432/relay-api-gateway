"""Single-flight request coalescing.

When several requests miss the cache for the same key at the same time, only
the first (the "leader") calls the upstream; the others ("followers") wait for
the leader's result. This stops a cache stampede: N concurrent misses become
one upstream request instead of N.

Design points:
* The shared work runs in its own task, so it is not tied to the leader's
  lifetime. If the leader's client disconnects (its coroutine is cancelled),
  the followers still get the result. Every waiter awaits through
  ``asyncio.shield``, so cancelling one waiter never cancels the shared task.
* Exceptions propagate to every waiter (each follower then returns the same
  error the leader would).
* The in-flight entry is removed when the task finishes, so a later miss
  starts a fresh fetch: results are shared only between *concurrent* callers.
  Reuse over time is the cache's job.
* The shared task is bounded by the request deadline of the work it runs, so
  an entry cannot be stuck forever.

No locks: dict access and task creation contain no ``await``, so on the event
loop "check for an in-flight task, otherwise create one" is atomic.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable


class SingleFlight[K, V]:
    def __init__(self) -> None:
        self._inflight: dict[K, asyncio.Task[V]] = {}

    def __len__(self) -> int:
        return len(self._inflight)

    async def do(self, key: K, fn: Callable[[], Awaitable[V]]) -> tuple[V, bool]:
        """Run ``fn`` once per key among concurrent callers.

        Returns ``(result, shared)`` where ``shared`` is True for followers.
        """
        task = self._inflight.get(key)
        shared = task is not None
        if task is None:
            task = asyncio.ensure_future(fn())
            self._inflight[key] = task
            task.add_done_callback(functools.partial(self._done, key))
        return await asyncio.shield(task), shared

    def _done(self, key: K, task: asyncio.Task[V]) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        # Mark the exception as retrieved even if every waiter went away, so
        # asyncio does not log "Task exception was never retrieved".
        if not task.cancelled():
            task.exception()
