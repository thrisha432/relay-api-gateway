"""A tiny fault-injecting TCP proxy for tests (a minimal Toxiproxy).

Sits between Relay and a dependency (Redis or an upstream) and can switch
behavior at runtime:

* ``pass``      forward bytes both ways (the default)
* ``reset``     accept, then close the connection immediately
* ``blackhole`` accept and read, but never forward or answer (a hung peer)
* ``truncate``  forward the request, but cut the response after N bytes

Switching modes also drops existing connections (``sever()``), the way a
network partition or a restart would, so pooled connections cannot hide a
fault.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class ChaosProxy:
    def __init__(self, target_host: str, target_port: int) -> None:
        self.target = (target_host, target_port)
        self.mode = "pass"
        self.truncate_after = 0
        self.port = 0
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self.sever()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def set_mode(self, mode: str, *, truncate_after: int = 0) -> None:
        self.mode = mode
        self.truncate_after = truncate_after
        self.sever()

    def sever(self) -> None:
        for writer in list(self._writers):
            writer.close()
        self._writers.clear()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        self._writers.add(writer)
        try:
            if self.mode == "reset":
                return
            if self.mode == "blackhole":
                while await reader.read(65536):
                    pass
                return
            up_reader, up_writer = await asyncio.open_connection(*self.target)
            self._writers.add(up_writer)
            limit = self.truncate_after if self.mode == "truncate" else None
            await asyncio.gather(
                self._pipe(reader, up_writer, None),
                self._pipe(up_reader, writer, limit),
                return_exceptions=True,
            )
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            self._writers.discard(writer)
            self._tasks.discard(task)

    async def _pipe(
        self, src: asyncio.StreamReader, dst: asyncio.StreamWriter, limit: int | None
    ) -> None:
        sent = 0
        try:
            while data := await src.read(65536):
                if limit is not None and sent + len(data) >= limit:
                    dst.write(data[: max(0, limit - sent)])
                    await dst.drain()
                    break
                dst.write(data)
                await dst.drain()
                sent += len(data)
        finally:
            with contextlib.suppress(OSError):
                dst.close()


@asynccontextmanager
async def chaos_proxy(host: str, port: int) -> AsyncIterator[ChaosProxy]:
    proxy = ChaosProxy(host, port)
    await proxy.start()
    try:
        yield proxy
    finally:
        await proxy.stop()
