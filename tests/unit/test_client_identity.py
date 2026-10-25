from relay.client_identity import UNKNOWN_CLIENT, ClientIdentityResolver


def test_untrusted_peer_ignores_forwarded_for() -> None:
    resolver = ClientIdentityResolver()
    assert resolver.resolve("10.0.0.7", ["1.2.3.4"]) == "10.0.0.7"
    assert resolver.forwarded_for_header("10.0.0.7", ["1.2.3.4"]) == "10.0.0.7"


def test_trusted_proxy_uses_rightmost_untrusted_hop() -> None:
    resolver = ClientIdentityResolver(["10.0.0.0/8"])
    # client-supplied spoof, real client, then two trusted proxies
    chain = ["6.6.6.6, 203.0.113.9", "10.1.1.1"]
    assert resolver.resolve("10.0.0.7", chain) == "203.0.113.9"
    assert resolver.forwarded_for_header("10.0.0.7", ["203.0.113.9"]) == "203.0.113.9, 10.0.0.7"


def test_trusted_proxy_with_garbage_chain_falls_back_to_peer() -> None:
    resolver = ClientIdentityResolver(["10.0.0.0/8"])
    assert resolver.resolve("10.0.0.7", ["not-an-ip"]) == "10.0.0.7"
    assert resolver.resolve("10.0.0.7", []) == "10.0.0.7"


def test_missing_peer() -> None:
    assert ClientIdentityResolver().resolve(None, []) == UNKNOWN_CLIENT
