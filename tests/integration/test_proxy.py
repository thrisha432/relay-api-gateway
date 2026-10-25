"""Phase 1 exit gate: the gateway forwards correctly and unknown routes 404."""

from __future__ import annotations

import gzip

import httpx
import pytest

from helpers import build_config, free_port_url, gateway_client, serve
from relay.app import create_app


def assert_error(response: httpx.Response, status: int, code: str) -> None:
    assert response.status_code == status
    body = response.json()
    assert body["error"]["code"] == code
    assert body["error"]["request_id"] == response.headers["x-request-id"]
    assert set(body["error"]) == {"code", "message", "request_id"}


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
async def test_forwards_method_path_query_and_body(upstream_a, method) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        response = await client.request(
            method,
            "/api/catalog/items/42?color=red&size=m&tag=a&tag=b",
            content=b'{"qty": 3}' if method != "GET" else None,
            headers={"content-type": "application/json", "x-custom": "yes"},
        )
    assert response.status_code == 200
    echo = response.json()
    assert echo["instance"] == "backend-a"
    assert echo["method"] == method
    assert echo["path"] == "/api/catalog/items/42"
    assert echo["query"] == "color=red&size=m&tag=a&tag=b"
    assert echo["headers"]["x-custom"] == "yes"
    if method != "GET":
        assert echo["body"] == '{"qty": 3}'


async def test_head_and_options_are_forwarded(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        head = await client.head("/api/catalog/x")
        options = await client.options("/api/catalog/x")
    assert head.status_code == 200
    assert head.content == b""
    assert int(head.headers["content-length"]) > 0  # upstream's declared length
    assert options.json()["method"] == "OPTIONS"


async def test_encoded_path_segments_are_preserved(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        response = await client.get("/api/catalog/a%20b/c%2Fd?q=%26x")
    echo = response.json()
    assert echo["raw_path"] == "/api/catalog/a%20b/c%2Fd"
    assert echo["query"] == "q=%26x"


async def test_strip_prefix_route(upstream_a) -> None:
    config = build_config([upstream_a.url], route={"strip_prefix": True})
    async with gateway_client(config) as client:
        response = await client.get("/api/catalog/v1/items")
    assert response.json()["path"] == "/v1/items"


async def test_unknown_route_returns_stable_json_404(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        for path in ("/nope", "/api/catalogue", "/health/whatever"):
            assert_error(await client.get(path), 404, "route_not_found")


async def test_request_id_generated_or_propagated(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        generated = await client.get("/api/catalog/x")
        given = await client.get("/api/catalog/x", headers={"x-request-id": "req-123"})
        malformed = await client.get("/api/catalog/x", headers={"x-request-id": "bad id!"})
    assert len(generated.headers["x-request-id"]) == 32
    assert generated.json()["headers"]["x-request-id"] == generated.headers["x-request-id"]
    assert given.headers["x-request-id"] == "req-123"
    assert given.json()["headers"]["x-request-id"] == "req-123"
    assert malformed.headers["x-request-id"] != "bad id!"


async def test_hop_by_hop_headers_not_forwarded_and_host_is_upstream(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        response = await client.get(
            "/api/catalog/x",
            headers={
                "connection": "x-drop-me",
                "x-drop-me": "secret",
                "keep-alive": "timeout=1",
                "proxy-authorization": "Basic Zm9vOmJhcg==",
                "te": "trailers",
                "x-forwarded-for": "6.6.6.6",
            },
        )
    sent = response.json()["headers"]
    for name in ("x-drop-me", "keep-alive", "proxy-authorization", "te"):
        assert name not in sent
    assert sent["host"] == upstream_a.url.removeprefix("http://")
    assert sent["x-forwarded-host"] == "relay.test"
    assert sent["x-forwarded-proto"] == "http"
    # Peer is not a trusted proxy, so the spoofed X-Forwarded-For is replaced.
    assert sent["x-forwarded-for"] == "10.0.0.7"


async def test_trusted_proxy_appends_forwarded_for(upstream_a) -> None:
    config = build_config([upstream_a.url], server={"trusted_proxies": ["10.0.0.0/8"]})
    async with gateway_client(config) as client:
        response = await client.get("/api/catalog/x", headers={"x-forwarded-for": "203.0.113.9"})
    assert response.json()["headers"]["x-forwarded-for"] == "203.0.113.9, 10.0.0.7"


async def test_response_headers_preserved_and_hop_by_hop_removed() -> None:
    async def upstream(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 201,
                "headers": [
                    (b"set-cookie", b"a=1"),
                    (b"set-cookie", b"b=2"),
                    (b"x-upstream", b"yes"),
                    (b"connection", b"x-internal"),
                    (b"x-internal", b"leak"),
                    (b"content-encoding", b"gzip"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": gzip.compress(b"hello")})

    async with serve(upstream) as url, gateway_client(build_config([url])) as client:
        response = await client.get("/api/catalog/x")
    assert response.status_code == 201
    assert response.headers.get_list("set-cookie") == ["a=1", "b=2"]
    assert response.headers["x-upstream"] == "yes"
    assert "x-internal" not in response.headers
    # Body passed through still compressed; httpx decodes it for us here.
    assert response.headers["content-encoding"] == "gzip"
    assert response.text == "hello"


async def test_oversized_request_body_rejected_with_413(upstream_a) -> None:
    config = build_config([upstream_a.url], server={"max_request_body_bytes": 10})
    async with gateway_client(config) as client:
        declared = await client.post("/api/catalog/x", content=b"x" * 11)

        async def chunks():
            for _ in range(5):
                yield b"xxxx"

        # No Content-Length: limit enforced while streaming.
        streamed = await client.post("/api/catalog/x", content=chunks())
        ok = await client.post("/api/catalog/x", content=b"x" * 10)
    assert_error(declared, 413, "payload_too_large")
    assert_error(streamed, 413, "payload_too_large")
    assert ok.status_code == 200
    assert (await upstream_a.stats())["requests"] == {"POST": 1}


async def test_invalid_requests(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        assert_error(await client.get("/api/catalog/%2e%2e/admin"), 400, "invalid_request")
        assert_error(await client.request("TRACE", "/api/catalog/x"), 405, "method_not_allowed")
        bad_length = await client.post(
            "/api/catalog/x", content=b"x", headers={"content-length": "abc"}
        )
        assert bad_length.status_code == 400


async def test_unreachable_upstream_returns_502() -> None:
    async with gateway_client(build_config([free_port_url()])) as client:
        assert_error(await client.get("/api/catalog/x"), 502, "upstream_connection_error")


async def test_slow_upstream_returns_504(upstream_a) -> None:
    config = build_config(
        [upstream_a.url],
        route={
            "timeouts": {"connect_seconds": 0.1, "attempt_seconds": 0.2, "request_seconds": 0.5},
            "retries": {"max_attempts": 1},
        },
    )
    async with gateway_client(config) as client:
        response = await client.get("/api/catalog/x?delay_ms=1000")
    assert_error(response, 504, "upstream_timeout")


async def test_upstream_response_size_is_capped(upstream_a) -> None:
    config = build_config([upstream_a.url], server={"max_response_body_bytes": 1000})
    async with gateway_client(config) as client:
        assert (await client.get("/api/catalog/x?size=1000")).status_code == 200
        response = await client.get("/api/catalog/x?size=1001")
    assert_error(response, 502, "upstream_response_too_large")


async def test_upstream_status_codes_pass_through(upstream_a) -> None:
    async with gateway_client(build_config([upstream_a.url])) as client:
        response = await client.get("/api/catalog/x?status=418")
    assert response.status_code == 418
    assert response.json()["instance"] == "backend-a"


async def test_end_to_end_over_real_sockets(upstream_a) -> None:
    """The gateway itself served by Uvicorn, not just ASGITransport."""
    app = create_app(build_config([upstream_a.url]))
    async with serve(app, lifespan="on") as gateway_url, httpx.AsyncClient() as client:
        live = await client.get(f"{gateway_url}/health/live")
        proxied = await client.get(f"{gateway_url}/api/catalog/e2e?x=1")
    assert live.json() == {"status": "alive"}
    assert proxied.json()["query"] == "x=1"
    assert proxied.json()["headers"]["x-forwarded-for"] == "127.0.0.1"
