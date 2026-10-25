"""Shared test helpers.

* Mock upstreams run as real Uvicorn servers on ephemeral localhost ports, so
  proxy tests exercise real sockets, connection pooling and timeouts.
* The gateway under test is usually driven in-process through
  ``httpx.ASGITransport`` with its lifespan running, which keeps tests fast
  while still executing startup/shutdown code.
"""

from __future__ import annotations

import asyncio
import copy
import socket
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx
import uvicorn
from starlette.types import ASGIApp

from relay.app import create_app
from relay.config import RelayConfig, parse_config


@asynccontextmanager
async def serve(app: ASGIApp, *, lifespan: str = "auto") -> AsyncIterator[str]:
    """Run an ASGI app with Uvicorn on 127.0.0.1:<random port>."""
    config = uvicorn.Config(
        app, host="127.0.0.1", port=0, log_level="warning", lifespan=lifespan, access_log=False
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    task = asyncio.create_task(server.serve())
    for _ in range(500):
        if server.started:
            break
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@dataclass
class MockUpstream:
    name: str
    url: str

    async def configure(self, **settings: Any) -> None:
        async with httpx.AsyncClient() as c:
            (await c.post(f"{self.url}/__mock/config", json=settings)).raise_for_status()

    async def stats(self) -> dict[str, Any]:
        async with httpx.AsyncClient() as c:
            return (await c.get(f"{self.url}/__mock/stats")).json()


def free_port_url() -> str:
    """A localhost URL nothing is listening on (connection refused)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def build_config(
    upstream_urls: list[str],
    *,
    route: dict[str, Any] | None = None,
    extra_routes: list[dict[str, Any]] | None = None,
    **top_level: Any,
) -> RelayConfig:
    """Config with one 'catalog' route over the given upstreams.

    Health checks are disabled unless a test enables them, so tests are not
    racing a background prober.
    """
    base_route: dict[str, Any] = {
        "id": "catalog",
        "path_prefix": "/api/catalog",
        "upstreams": [{"id": f"u{i}", "url": url} for i, url in enumerate(upstream_urls)],
        "health_check": {"enabled": False},
        "retries": {"backoff_base_seconds": 0.001, "backoff_max_seconds": 0.01},
    }
    for key, value in (route or {}).items():
        if isinstance(value, dict) and isinstance(base_route.get(key), dict):
            base_route[key] = {**base_route[key], **value}
        else:
            base_route[key] = value
    data: dict[str, Any] = {"routes": [base_route, *(extra_routes or [])]}
    data.update(copy.deepcopy(top_level))
    return parse_config(data)


@asynccontextmanager
async def gateway_client(
    config: RelayConfig, **app_kwargs: Any
) -> AsyncIterator[httpx.AsyncClient]:
    """Run the gateway app (with lifespan) and yield an in-process client."""
    app = create_app(config, **app_kwargs)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, client=("10.0.0.7", 50000))
        async with httpx.AsyncClient(transport=transport, base_url="http://relay.test") as client:
            client.app = app  # type: ignore[attr-defined]
            yield client


async def wait_for(predicate: Any, within_seconds: float = 3.0) -> None:
    """Poll ``predicate`` until true; fail the test if it takes too long."""
    deadline = time.monotonic() + within_seconds
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.02)
