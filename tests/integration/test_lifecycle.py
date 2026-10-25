"""Graceful shutdown (audit A9) and rate limiting across real OS processes (A10)."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
from collections import Counter
from pathlib import Path

import httpx
import pytest
import uvicorn

from helpers import build_config, free_port_url
from relay.app import create_app


async def test_graceful_shutdown_finishes_in_flight_requests(upstream_a) -> None:
    app = create_app(build_config([upstream_a.url]))
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    )
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    serving = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - uvicorn exposes no started event
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"

    async with httpx.AsyncClient() as client:
        in_flight = asyncio.create_task(client.get(f"{base}/api/catalog/x?delay_ms=400"))
        await asyncio.sleep(0.1)
        server.should_exit = True  # what SIGTERM does
        response = await in_flight
        await serving
        with pytest.raises(httpx.ConnectError):
            await client.get(f"{base}/health/live")  # no longer accepting

    assert response.status_code == 200  # the in-flight request completed
    gateway = app.state.gateway
    assert gateway.health_checker.running_tasks == 0
    with pytest.raises(RuntimeError):
        _ = gateway.client  # pooled client closed during lifespan shutdown


def _spawn_gateway(config_path: Path, port: int) -> subprocess.Popen[bytes]:
    env = {**os.environ, "RELAY_LOG_LEVEL": "WARNING"}
    return subprocess.Popen(  # noqa: S603 - fixed argv built by the test itself
        [sys.executable, "-m", "relay", "--config", str(config_path), "--port", str(port)],
        env={**env, "RELAY_LOG_LEVEL": "INFO", "RELAY_LOG_JSON": "false"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


@pytest.mark.redis
async def test_three_gateway_processes_share_one_bucket_exactly(upstream_a, tmp_path) -> None:
    """Separate OS processes (not in-process apps) burst one shared bucket."""
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        pytest.skip("REDIS_URL not set")
    import redis.asyncio as aioredis

    probe = aioredis.Redis.from_url(redis_url)
    await probe.flushdb()
    await probe.aclose()

    config_path = tmp_path / "routes.yaml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            redis: {{url: "{redis_url}"}}
            routes:
              - id: limited
                path_prefix: /api/limited
                upstreams: [{{id: a, url: "{upstream_a.url}"}}]
                health_check: {{enabled: false}}
                rate_limit: {{capacity: 40, refill_rate_per_second: 0.001}}
            """
        )
    )
    ports = [int(free_port_url().rsplit(":", 1)[1]) for _ in range(3)]
    procs = [_spawn_gateway(config_path, port) for port in ports]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            for port in ports:
                for _ in range(200):
                    try:
                        if (
                            await client.get(f"http://127.0.0.1:{port}/health/ready")
                        ).status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail(f"gateway on {port} never became ready")

            burst = [
                client.get(f"http://127.0.0.1:{ports[i % 3]}/api/limited/x") for i in range(300)
            ]
            responses = await asyncio.gather(*burst)
        assert Counter(r.status_code for r in responses) == {200: 40, 429: 260}
        assert (await upstream_a.stats())["requests"] == {"GET": 40}
    finally:
        for proc in procs:
            proc.send_signal(signal.SIGTERM)
        outputs = [proc.communicate(timeout=10)[0].decode() for proc in procs]
    for proc, output in zip(procs, outputs, strict=True):
        # Lifespan shutdown ran to completion (clients and tasks closed) ...
        assert "shutdown_complete" in output
        assert "Traceback" not in output
        # ... then Uvicorn re-raises the captured SIGTERM by design, so the exit
        # status follows Unix convention (-15 here, 143 in a shell).
        assert proc.returncode == -signal.SIGTERM
