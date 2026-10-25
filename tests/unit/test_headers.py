from relay.headers import (
    connection_tokens,
    filter_request_headers,
    filter_response_headers,
    get_all,
)


def test_hop_by_hop_and_connection_named_headers_are_removed() -> None:
    headers = [
        (b"Connection", b"keep-alive, X-Secret-Hop"),
        (b"Keep-Alive", b"timeout=5"),
        (b"X-Secret-Hop", b"1"),
        (b"Transfer-Encoding", b"chunked"),
        (b"Upgrade", b"websocket"),
        (b"Proxy-Authorization", b"Basic abc"),
        (b"TE", b"trailers"),
        (b"Accept", b"application/json"),
        (b"Authorization", b"Bearer t"),
    ]
    assert connection_tokens(headers) == {b"keep-alive", b"x-secret-hop"}
    assert filter_request_headers(headers) == [
        (b"Accept", b"application/json"),
        (b"Authorization", b"Bearer t"),
    ]


def test_request_filter_drops_headers_relay_sets_itself() -> None:
    headers = [
        (b"host", b"relay.test"),
        (b"content-length", b"3"),
        (b"x-forwarded-for", b"1.2.3.4"),
        (b"x-request-id", b"abc"),
        (b"x-custom", b"kept"),
    ]
    assert filter_request_headers(headers) == [(b"x-custom", b"kept")]


def test_response_filter_keeps_repeated_set_cookie() -> None:
    headers = [
        (b"set-cookie", b"a=1"),
        (b"set-cookie", b"b=2"),
        (b"content-length", b"10"),
        (b"connection", b"close"),
        (b"content-type", b"text/plain"),
    ]
    filtered = filter_response_headers(headers)
    assert get_all(filtered, b"set-cookie") == [b"a=1", b"b=2"]
    assert (b"content-length", b"10") not in filtered
    assert (b"content-type", b"text/plain") in filtered
