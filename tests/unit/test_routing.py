import pytest

from relay.config import parse_config
from relay.routing import InvalidPathError, Router, validate_raw_path


def make_router(*prefixes: str, strip: bool = False) -> Router:
    routes = [
        {
            "id": f"r{i}",
            "path_prefix": p,
            "strip_prefix": strip,
            "upstreams": [{"id": "a", "url": "http://a:1"}],
        }
        for i, p in enumerate(prefixes)
    ]
    return Router(parse_config({"routes": routes}).routes)


def matched_prefix(router: Router, path: str) -> str | None:
    match = router.match(path)
    return match.route.path_prefix if match else None


def test_prefix_matches_only_on_segment_boundary() -> None:
    router = make_router("/api/catalog")
    assert matched_prefix(router, "/api/catalog") == "/api/catalog"
    assert matched_prefix(router, "/api/catalog/") == "/api/catalog"
    assert matched_prefix(router, "/api/catalog/items/42") == "/api/catalog"
    assert matched_prefix(router, "/api/catalogue") is None
    assert matched_prefix(router, "/api") is None


def test_longest_prefix_wins_regardless_of_config_order() -> None:
    router = make_router("/api", "/api/catalog/admin", "/api/catalog")
    assert matched_prefix(router, "/api/catalog/admin/x") == "/api/catalog/admin"
    assert matched_prefix(router, "/api/catalog/x") == "/api/catalog"
    assert matched_prefix(router, "/api/orders") == "/api"


def test_full_path_forwarded_by_default_and_stripped_when_configured() -> None:
    assert make_router("/api/catalog").match("/api/catalog/x").upstream_path == "/api/catalog/x"
    stripped = make_router("/api/catalog", strip=True)
    assert stripped.match("/api/catalog/x/y").upstream_path == "/x/y"
    assert stripped.match("/api/catalog").upstream_path == "/"


def test_encoded_segments_are_matched_raw_and_preserved() -> None:
    router = make_router("/api/catalog")
    match = router.match("/api/catalog/a%20b/c%2Fd")
    assert match is not None
    assert match.upstream_path == "/api/catalog/a%20b/c%2Fd"
    # Encoded prefix characters do not match: deterministic raw matching.
    assert router.match("/api/%63atalog/x") is None


@pytest.mark.parametrize(
    "path",
    [
        "/api/catalog/../admin",
        "/api/catalog/./x",
        "/api/catalog/%2e%2e/admin",
        "/api/catalog/%2E%2e",
        "/api/catalog/..%2Fadmin",
        "/api/catalog/a\\b",
        "/api/catalog/%00",
        "relative",
        "/api/catalog/ space",
    ],
)
def test_unsafe_paths_rejected(path: str) -> None:
    with pytest.raises(InvalidPathError):
        validate_raw_path(path)


@pytest.mark.parametrize("path", ["/", "/api/catalog/a..b", "/api/catalog/%7Euser", "/a//b"])
def test_benign_paths_accepted(path: str) -> None:
    validate_raw_path(path)
