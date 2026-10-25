"""Adversarial tests: requests crafted to bypass Relay's guarantees.

Requests are written as raw bytes over a real socket to a Uvicorn-served
gateway, so the actual HTTP parser is in the loop and no client library
normalizes the input first.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from helpers import build_config, free_port_url, serve
from relay.app import create_app

pytestmark = pytest.mark.security


@asynccontextmanager
async def served_gateway(config) -> AsyncIterator[tuple[str, int]]:
    app = create_app(config)
    async with serve(app, lifespan="on") as url:
        host, port = url.removeprefix("http://").split(":")
        yield host, int(port)


async def raw_request(addr: tuple[str, int], data: bytes) -> tuple[int, dict[str, str], bytes]:
    reader, writer = await asyncio.open_connection(*addr)
    writer.write(data)
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), timeout=5)  # Connection: close
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    return status, headers, body


def get(path: str, *extra: str, host: str = "relay.test") -> bytes:
    lines = [f"GET {path} HTTP/1.1"]
    if host:
        lines.append(f"Host: {host}")
    lines += [*extra, "Connection: close", "", ""]
    return "\r\n".join(lines).encode("latin-1")


# ----------------------------------------------------------------- SSRF


@pytest.mark.parametrize(
    "request_target",
    [
        "http://evil.example/api/catalog/x",  # absolute-form, external host
        "http://169.254.169.254/api/catalog/x",  # cloud metadata address
        "http://127.0.0.1:6379/api/catalog/x",  # internal service
    ],
)
async def test_absolute_form_target_cannot_choose_the_destination(
    upstream_a, request_target
) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        status, _, body = await raw_request(addr, get(request_target, host="evil.example"))
    assert status == 200
    echo = json.loads(body)
    assert echo["instance"] == "backend-a"  # still the configured upstream
    assert echo["headers"]["host"] == upstream_a.url.removeprefix("http://")


@pytest.mark.parametrize(
    ("host", "header"),
    [
        ("169.254.169.254", "X-Probe: host-header-points-at-metadata-service"),
        ("relay.test", "X-Forwarded-Host: evil.example"),
        ("relay.test", "X-Original-URL: http://evil.example/"),
        ("relay.test", "X-Rewrite-URL: http://evil.example/"),
        ("relay.test", "Forwarded: host=evil.example;proto=https"),
    ],
)
async def test_headers_cannot_override_the_destination(upstream_a, host, header) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        status, _, body = await raw_request(addr, get("/api/catalog/x", header, host=host))
    assert status == 200
    assert json.loads(body)["instance"] == "backend-a"


@pytest.mark.parametrize(
    "path",
    [
        "/api/catalog/x?url=http://evil.example/",
        "/api/catalog/x?upstream=http%3A%2F%2F127.0.0.1%3A6379",
    ],
)
async def test_query_parameters_cannot_choose_the_destination(upstream_a, path) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        _, _, body = await raw_request(addr, get(path))
    assert json.loads(body)["instance"] == "backend-a"


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("//evil.example/api/catalog", 404),
        ("/api/catalog/%2e%2e/%2e%2e/admin", 400),
        ("/api/catalog/..%2F..%2Fadmin", 400),
        ("/api/catalog/%2E%2E%5Cadmin", 400),
        ("/api/catalog/%00", 400),
        ("/api/catalog@evil.example/x", 404),
        ("/api/catalog%2F..%2Fadmin", 400),  # encoded slash hiding a dot segment
    ],
)
async def test_encoding_tricks_do_not_escape_the_route(upstream_a, path, status) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        got, _, _ = await raw_request(addr, get(path))
    assert got == status
    assert (await upstream_a.stats())["requests"] == {}


async def test_upstream_redirects_are_returned_not_followed(upstream_b) -> None:
    async def redirecting(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 302,
                "headers": [(b"location", f"{upstream_b.url}/internal-only".encode())],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    async with serve(redirecting) as url, served_gateway(build_config([url])) as addr:
        status, headers, _ = await raw_request(addr, get("/api/catalog/x"))
    assert status == 302
    assert headers["location"].endswith("/internal-only")
    assert (await upstream_b.stats())["requests"] == {}  # Relay never fetched it


# ------------------------------------------------------- header handling


async def test_oversized_headers_rejected_with_431(upstream_a) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        too_big, _, body = await raw_request(addr, get("/api/catalog/x", "X-Big: " + "a" * 40_000))
        many = [f"X-H{i}: v" for i in range(150)]
        too_many, _, _ = await raw_request(addr, get("/api/catalog/x", *many))
        ok, _, _ = await raw_request(addr, get("/api/catalog/x", "X-Fine: " + "a" * 1000))
    assert too_big == 431
    assert json.loads(body)["error"]["code"] == "request_headers_too_large"
    assert too_many == 431
    assert ok == 200
    assert (await upstream_a.stats())["requests"] == {"GET": 1}


async def test_http11_without_host_rejected_but_http10_allowed(upstream_a) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        missing, _, _ = await raw_request(addr, get("/api/catalog/x", host=""))
        http10, _, _ = await raw_request(addr, b"GET /api/catalog/x HTTP/1.0\r\n\r\n")
    assert missing == 400
    assert http10 == 200


async def test_conflicting_framing_headers_never_reach_upstream(upstream_a) -> None:
    smuggle = (
        b"POST /api/catalog/x HTTP/1.1\r\nHost: relay.test\r\n"
        b"Content-Length: 4\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        b"0\r\n\r\nGET /api/catalog/smuggled HTTP/1.1\r\nHost: relay.test\r\n\r\n"
    )
    async with served_gateway(build_config([upstream_a.url])) as addr:
        status, _, _ = await raw_request(addr, smuggle)
    assert status == 400
    assert (await upstream_a.stats())["requests"] == {}


async def test_hop_by_hop_and_connection_listed_headers_are_stripped(upstream_a) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        _, _, body = await raw_request(
            addr,
            get(
                "/api/catalog/x",
                "Connection: X-Internal-Token, close",
                "X-Internal-Token: secret-hop",
                "Proxy-Authorization: Basic c2VjcmV0",
                "Upgrade: websocket",
            ),
        )
    sent = json.loads(body)["headers"]
    for name in ("x-internal-token", "proxy-authorization", "upgrade"):
        assert name not in sent
    # httpx sets its own Connection header for the upstream hop; the client's
    # value (and the header names it listed) must not be carried over.
    assert "x-internal-token" not in sent.get("connection", "").lower()
    assert "secret-hop" not in json.dumps(sent)


async def test_upstream_cannot_spoof_relay_headers() -> None:
    async def spoofing(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"x-relay-cache", b"HIT"),
                    (b"x-request-id", b"spoofed"),
                    (b"connection", b"x-leak"),
                    (b"x-leak", b"internal"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"{}"})

    async with serve(spoofing) as url:
        config = build_config([url], route={"cache": {"enabled": True}})
        async with served_gateway(config) as addr:
            _, headers, _ = await raw_request(addr, get("/api/catalog/x"))
    assert headers["x-relay-cache"] == "MISS"
    assert headers["x-request-id"] != "spoofed"
    assert "x-leak" not in headers


# -------------------------------------------------- information leakage


async def test_redis_credentials_never_appear_in_responses_logs_or_metrics(
    upstream_a, relay_logs
) -> None:
    secret = "s3cr3t-redis-password"
    dead = free_port_url().replace("http://", f"redis://:{secret}@")
    config = build_config(
        [upstream_a.url],
        route={"rate_limit": {"capacity": 5, "refill_rate_per_second": 1}},
        redis={"url": dead},
    )
    async with served_gateway(config) as addr:
        limited = await raw_request(addr, get("/api/catalog/x"))
        ready = await raw_request(addr, get("/health/ready"))
        routes = await raw_request(addr, get("/__relay/routes"))
        metrics = await raw_request(addr, get("/metrics"))
    assert limited[0] == 503
    for _status, headers, body in (limited, ready, routes, metrics):
        assert secret not in body.decode("latin-1")
        assert secret not in json.dumps(headers)
    assert secret not in relay_logs.text


async def test_errors_do_not_leak_exception_text_or_upstream_addresses() -> None:
    target = free_port_url()
    async with served_gateway(build_config([target])) as addr:
        status, _, body = await raw_request(addr, get("/api/catalog/x"))
    assert status == 502
    text = body.decode()
    port = target.rsplit(":", 1)[1]
    for leak in (port, "127.0.0.1", "Traceback", "ConnectError", "Errno"):
        assert leak not in text
    assert set(json.loads(body)["error"]) == {"code", "message", "request_id"}


async def test_access_log_omits_credentials_query_and_client_ip(upstream_a, relay_logs) -> None:
    async with served_gateway(build_config([upstream_a.url])) as addr:
        await raw_request(
            addr,
            get(
                "/api/catalog/x?token=query-secret",
                "Authorization: Bearer header-secret",
                "Cookie: session=cookie-secret",
            ),
        )
    records = [r for r in relay_logs.records if r.getMessage() == "request"]
    assert records
    logged = json.dumps([r.fields for r in records])
    for secret in ("query-secret", "header-secret", "cookie-secret", "127.0.0.1"):
        assert secret not in logged
