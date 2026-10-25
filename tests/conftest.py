"""Shared pytest fixtures (helpers live in tests/helpers.py)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from helpers import MockUpstream, serve
from mock_backend.app import create_mock_app


@pytest.fixture
async def upstream_a() -> AsyncIterator[MockUpstream]:
    async with serve(create_mock_app("backend-a")) as url:
        yield MockUpstream("backend-a", url)


@pytest.fixture
async def upstream_b() -> AsyncIterator[MockUpstream]:
    async with serve(create_mock_app("backend-b")) as url:
        yield MockUpstream("backend-b", url)
