from pathlib import Path

import pytest

from relay.config import ConfigError, load_config, parse_config

ROOT = Path(__file__).resolve().parents[2]


def minimal(**route_overrides):
    route = {
        "id": "catalog",
        "path_prefix": "/api/catalog",
        "upstreams": [{"id": "a", "url": "http://backend-a:8001"}],
    }
    route.update(route_overrides)
    return {"routes": [route]}


def test_repository_config_is_valid() -> None:
    config = load_config(ROOT / "config" / "routes.yaml")
    assert [r.id for r in config.routes] == ["catalog", "orders", "bench", "bench-cached"]
    assert config.redis.fail_open_for_rate_limit is False


def test_defaults_merge_per_field_into_routes() -> None:
    data = minimal(retries={"max_attempts": 1})
    data["defaults"] = {"retries": {"backoff_base_seconds": 0.2, "backoff_max_seconds": 0.3}}
    route = parse_config(data).routes[0]
    assert route.retries.max_attempts == 1  # route override wins
    assert route.retries.backoff_base_seconds == 0.2  # default fills the rest


def test_trailing_slash_in_prefix_is_normalized_and_url_slash_stripped() -> None:
    data = minimal(path_prefix="/api/catalog/")
    data["routes"][0]["upstreams"][0]["url"] = "http://backend-a:8001/"
    route = parse_config(data).routes[0]
    assert route.path_prefix == "/api/catalog"
    assert route.upstreams[0].url == "http://backend-a:8001"


@pytest.mark.parametrize(
    ("data", "fragment"),
    [
        (minimal(unknown_key=1), "unknown_key"),
        (minimal(path_prefix="api"), "must start with '/'"),
        (minimal(path_prefix="/health"), "reserved"),
        (minimal(path_prefix="/metrics/x"), "reserved"),
        (minimal(path_prefix="/"), "reserved"),
        (minimal(path_prefix="/api/../x"), "invalid path segment"),
        (minimal(id="Bad ID"), "lowercase"),
        (minimal(upstreams=[]), "at least 1"),
        (
            minimal(upstreams=[{"id": "a", "url": "ftp://x"}]),
            "scheme must be http or https",
        ),
        (
            minimal(upstreams=[{"id": "a", "url": "http://user:pw@x"}]),
            "credentials must not be embedded",
        ),
        (
            minimal(upstreams=[{"id": "a", "url": "http://x?y=1"}]),
            "query string",
        ),
        (
            minimal(upstreams=[{"id": "a", "url": "http://x"}, {"id": "a", "url": "http://y"}]),
            "duplicate upstream ids",
        ),
        (minimal(retries={"retry_methods": ["GET", "POST"]}), "not safe to retry"),
        (minimal(retries={"max_attempts": 50}), "less than or equal to 5"),
        (minimal(retries={"retry_status_codes": [404]}), "must be a 5xx"),
        (minimal(timeouts={"attempt_seconds": 10, "request_seconds": 5}), "must not exceed"),
        (minimal(rate_limit={"capacity": 0, "refill_rate_per_second": 1}), "greater than"),
        (
            minimal(rate_limit={"capacity": 5, "refill_rate_per_second": 1}),
            "redis.url (or RELAY_REDIS_URL) is required",
        ),
        (minimal(cache={"vary_headers": ["Authorization"]}), "cannot be cache-key headers"),
        ({"routes": []}, "at least 1"),
    ],
)
def test_invalid_configs_fail_with_actionable_messages(data, fragment) -> None:
    with pytest.raises(ConfigError) as info:
        parse_config(data)
    assert fragment in str(info.value)


def test_duplicate_route_ids_and_prefixes_rejected() -> None:
    route = minimal()["routes"][0]
    with pytest.raises(ConfigError, match="duplicate route ids"):
        parse_config({"routes": [route, {**route, "path_prefix": "/other"}]})
    with pytest.raises(ConfigError, match="duplicate path_prefix"):
        parse_config({"routes": [route, {**route, "id": "other"}]})


def test_env_override_for_redis_url(tmp_path, monkeypatch) -> None:
    path = tmp_path / "routes.yaml"
    path.write_text(
        "routes:\n"
        "  - id: r\n"
        "    path_prefix: /api/r\n"
        "    upstreams: [{id: a, url: 'http://a:1'}]\n"
        "    rate_limit: {capacity: 1, refill_rate_per_second: 1}\n"
    )
    with pytest.raises(ConfigError, match=r"redis\.url"):
        load_config(path)
    monkeypatch.setenv("RELAY_REDIS_URL", "redis://localhost:6379/3")
    assert load_config(path).redis.url == "redis://localhost:6379/3"


def test_missing_and_malformed_files(tmp_path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("routes: [unclosed")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad)
    scalar = tmp_path / "scalar.yaml"
    scalar.write_text("42")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(scalar)


def test_rate_limit_key_ttl_default() -> None:
    data = minimal(rate_limit={"capacity": 10, "refill_rate_per_second": 2})
    data["redis"] = {"url": "redis://localhost:6379/0"}
    rl = parse_config(data).routes[0].rate_limit
    assert rl is not None
    assert rl.effective_key_ttl_seconds == 11  # 10 / 2 * 2 + 1
