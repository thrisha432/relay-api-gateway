"""Client identity resolution.

``X-Forwarded-For`` is trivially spoofable, so it is honoured only when the
direct TCP peer is one of the explicitly configured ``trusted_proxies``. In
that case the client is the right-most address in the chain that is *not* a
trusted proxy (addresses to its left were supplied by the client and cannot be
trusted). With no trusted proxies configured, the peer address is always used.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Iterable, Sequence

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
UNKNOWN_CLIENT = "unknown"


def _parse_ip(value: str) -> IPAddress | None:
    try:
        return ipaddress.ip_address(value.strip())
    except ValueError:
        return None


class ClientIdentityResolver:
    def __init__(self, trusted_proxies: Iterable[str] = ()) -> None:
        self._networks = [ipaddress.ip_network(n, strict=False) for n in trusted_proxies]

    def is_trusted(self, address: str | None) -> bool:
        ip = _parse_ip(address) if address else None
        return ip is not None and any(ip in net for net in self._networks)

    def resolve(self, peer: str | None, forwarded_for: Sequence[str]) -> str:
        """Return the client IP string used for rate limiting."""
        if not peer:
            return UNKNOWN_CLIENT
        if not forwarded_for or not self.is_trusted(peer):
            return peer
        chain = [part.strip() for value in forwarded_for for part in value.split(",")]
        for hop in reversed([h for h in chain if h]):
            if _parse_ip(hop) is None:
                # Garbage in the chain: stop and fall back to the last trusted hop.
                break
            if not self.is_trusted(hop):
                return hop
        return peer

    def forwarded_for_header(self, peer: str | None, forwarded_for: Sequence[str]) -> str:
        """Value of X-Forwarded-For to send upstream.

        Existing values are kept only if they came through a trusted proxy;
        otherwise the chain restarts at the direct peer.
        """
        if not peer:
            return UNKNOWN_CLIENT
        if forwarded_for and self.is_trusted(peer):
            return ", ".join([*forwarded_for, peer])
        return peer
