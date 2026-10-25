"""Shared pytest fixtures (helpers live in tests/helpers.py)."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Callable

import fakeredis
import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from helpers import MockUpstream, free_port_url, serve
from mock_backend.app import create_mock_app
from relay.config import RedisConfig
from relay.redis_client import create_redis


@pytest.fixture
async def upstream_a() -> AsyncIterator[MockUpstream]:
    async with serve(create_mock_app("backend-a")) as url:
        yield MockUpstream("backend-a", url)


@pytest.fixture
async def upstream_b() -> AsyncIterator[MockUpstream]:
    async with serve(create_mock_app("backend-b")) as url:
        yield MockUpstream("backend-b", url)


@pytest.fixture(
    params=[
        pytest.param("fake", id="fakeredis"),
        pytest.param("real", id="real-redis", marks=pytest.mark.redis),
    ]
)
async def redis_factory(request) -> AsyncIterator[Callable[[], Redis]]:
    """Factory of Redis clients that all talk to the SAME server.

    Calling it twice simulates two gateway instances sharing one Redis.
    'real' uses $REDIS_URL (a throwaway DB; it is flushed) and is skipped when
    unset or unreachable; CI runs it against a Redis service container.
    """
    clients: list[Redis] = []
    if request.param == "fake":
        server = fakeredis.FakeServer()

        def make() -> Redis:
            client = fakeredis.FakeAsyncRedis(server=server)
            clients.append(client)
            return client
    else:
        url = os.environ.get("REDIS_URL")
        if not url:
            pytest.skip("REDIS_URL not set")
        probe = Redis.from_url(url, socket_connect_timeout=0.5)
        try:
            await probe.ping()
        except (RedisError, OSError):
            pytest.skip(f"Redis at {url} unreachable")
        await probe.flushdb()
        await probe.aclose()

        def make() -> Redis:
            client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
            clients.append(client)
            return client

    yield make
    if clients:
        await clients[0].flushdb()
    for client in clients:
        await client.aclose()


@pytest.fixture
def dead_redis() -> Redis:
    """A client pointed at a port with nothing listening (simulated outage)."""
    url = free_port_url().replace("http://", "redis://")
    return create_redis(
        RedisConfig(url=url, connect_timeout_seconds=0.2, operation_timeout_seconds=0.5)
    )


@pytest.fixture
def relay_logs(caplog):
    """Capture records from the 'relay' logger even if configure_logging()
    disabled propagation earlier in the session."""
    logger = logging.getLogger("relay")
    old_level = logger.level
    logger.addHandler(caplog.handler)
    logger.setLevel(logging.DEBUG)
    yield caplog
    logger.removeHandler(caplog.handler)
    logger.setLevel(old_level)
