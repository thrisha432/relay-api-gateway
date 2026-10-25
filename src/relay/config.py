"""Configuration schema, validation, and loading.

Routes and upstreams come only from this trusted, validated configuration.
Clients can never influence which host Relay connects to.

Loading order:
1. Read YAML from ``RELAY_CONFIG`` (default ``config/routes.yaml``).
2. Apply environment overrides (``RELAY_REDIS_URL``).
3. Merge ``defaults`` into each route section the route does not override
   (shallow merge per section: a route's ``retries.max_attempts`` overrides
   only that field, other retry fields still come from ``defaults.retries``).
4. Validate everything with Pydantic; any error aborts startup with a message
   that names the offending field.
"""

from __future__ import annotations

import ipaddress
import os
import re
from pathlib import Path
from typing import Any, Literal, Self

import httpx
import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

# Paths owned by the gateway itself. Routes may not shadow them.
RESERVED_PATH_PREFIXES: tuple[str, ...] = ("/health", "/metrics", "/__relay")

# Methods that are safe to retry automatically. Relay deliberately has no
# idempotency-key contract, so unsafe methods can never be configured here.
RETRYABLE_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})

_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
_PREFIX_SEGMENT = re.compile(r"^[A-Za-z0-9._~!$&'()*+,;=:@-]+$")


class ConfigError(Exception):
    """Raised when configuration cannot be loaded or is invalid."""


class _Strict(BaseModel):
    """Base model: unknown keys are errors so typos never silently do nothing."""

    model_config = ConfigDict(extra="forbid", frozen=True)


def _validate_id(value: str) -> str:
    if not _ID_PATTERN.match(value):
        raise ValueError(
            "must be 1-63 chars of lowercase letters, digits, '-' or '_', "
            "starting with a letter or digit"
        )
    return value


class ServerConfig(_Strict):
    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    max_request_body_bytes: int = Field(default=1_048_576, ge=0, le=100 * 1024 * 1024)
    max_response_body_bytes: int = Field(default=10 * 1024 * 1024, ge=1, le=100 * 1024 * 1024)
    # Request header limits (431 when exceeded). The HTTP parser itself does
    # not cap header size, so without these a client could send megabytes of
    # headers that Relay would forward upstream.
    max_header_bytes: int = Field(default=32 * 1024, ge=1024, le=1024 * 1024)
    max_header_count: int = Field(default=100, ge=8, le=10_000)
    # Peers whose X-Forwarded-For header is trusted. Empty means "trust nobody":
    # the client identity is always the direct TCP peer address.
    trusted_proxies: tuple[str, ...] = ()
    # Exposes GET /__relay/routes (route ids, upstream ids, health; no URLs).
    enable_route_summary: bool = True
    shutdown_grace_seconds: float = Field(default=5.0, gt=0, le=60)
    # Upstream connection pool (shared by all routes). httpcore scans the pool
    # on every request, so idle connections beyond what concurrency needs cost
    # CPU; see docs/ARCHITECTURE.md "Performance notes".
    upstream_max_connections: int = Field(default=100, ge=1, le=10_000)
    upstream_max_keepalive_connections: int = Field(default=20, ge=0, le=10_000)

    @field_validator("trusted_proxies")
    @classmethod
    def _valid_networks(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for item in value:
            try:
                ipaddress.ip_network(item, strict=False)
            except ValueError as exc:
                raise ValueError(f"invalid IP network {item!r}: {exc}") from exc
        return value


class RedisConfig(_Strict):
    url: str | None = None
    connect_timeout_seconds: float = Field(default=0.5, gt=0, le=10)
    operation_timeout_seconds: float = Field(default=0.5, gt=0, le=10)
    # Default fail-closed: when Redis is down, rate-limited routes return 503
    # instead of silently serving unlimited traffic.
    fail_open_for_rate_limit: bool = False

    @field_validator("url")
    @classmethod
    def _valid_url(cls, value: str | None) -> str | None:
        if not value:
            return None
        if not value.startswith(("redis://", "rediss://", "unix://")):
            raise ValueError("must start with redis://, rediss:// or unix://")
        return value


class CacheBackendConfig(_Strict):
    # "local": per-process LRU. "redis": shared between gateway instances.
    backend: Literal["local", "redis"] = "local"
    max_entries: int = Field(default=1024, ge=0, le=1_000_000)


class TimeoutConfig(_Strict):
    connect_seconds: float = Field(default=1.0, gt=0, le=30)
    # Per-attempt read/write/pool timeout.
    attempt_seconds: float = Field(default=2.0, gt=0, le=60)
    # Overall deadline for one client request, including every retry and backoff.
    request_seconds: float = Field(default=5.0, gt=0, le=120)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.connect_seconds > self.request_seconds:
            raise ValueError("connect_seconds must not exceed request_seconds")
        if self.attempt_seconds > self.request_seconds:
            raise ValueError("attempt_seconds must not exceed request_seconds")
        return self


class RetryConfig(_Strict):
    # Total attempts including the first one. 1 disables retries.
    max_attempts: int = Field(default=2, ge=1, le=5)
    retry_status_codes: tuple[int, ...] = (502, 503, 504)
    retry_methods: tuple[str, ...] = ("GET", "HEAD", "OPTIONS")
    backoff_base_seconds: float = Field(default=0.05, ge=0, le=5)
    backoff_max_seconds: float = Field(default=0.5, ge=0, le=10)
    # Retry budget (see resilience/retry_budget.py): retries may add at most
    # budget_ratio x requests (+ a small time-based reserve) of extra load.
    budget_enabled: bool = True
    budget_ratio: float = Field(default=0.2, ge=0, le=1)
    budget_min_retries_per_second: float = Field(default=5.0, ge=0, le=10_000)
    budget_max_balance: float = Field(default=50.0, ge=1, le=100_000)

    @field_validator("retry_methods")
    @classmethod
    def _safe_methods_only(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        upper = tuple(m.upper() for m in value)
        unsafe = sorted(set(upper) - RETRYABLE_METHODS)
        if unsafe:
            raise ValueError(
                f"methods {unsafe} are not safe to retry automatically; Relay has no "
                "idempotency-key contract, so only GET, HEAD and OPTIONS are allowed"
            )
        return upper

    @field_validator("retry_status_codes")
    @classmethod
    def _server_errors_only(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        for code in value:
            if not 500 <= code <= 599:
                raise ValueError(f"retry status {code} must be a 5xx status")
        return value

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.backoff_base_seconds > self.backoff_max_seconds:
            raise ValueError("backoff_base_seconds must not exceed backoff_max_seconds")
        return self


class HealthCheckConfig(_Strict):
    enabled: bool = True
    path: str = "/health"
    interval_seconds: float = Field(default=5.0, ge=0.05, le=300)
    timeout_seconds: float = Field(default=1.0, gt=0, le=30)
    # Consecutive failures before an instance is marked unhealthy.
    unhealthy_threshold: int = Field(default=3, ge=1, le=100)
    # Consecutive successes before an unhealthy instance is restored.
    healthy_threshold: int = Field(default=2, ge=1, le=100)

    @field_validator("path")
    @classmethod
    def _absolute(cls, value: str) -> str:
        if not value.startswith("/") or "?" in value or "#" in value:
            raise ValueError("must be an absolute path without query or fragment")
        return value


class CircuitBreakerConfig(_Strict):
    enabled: bool = True
    # Consecutive infrastructure failures (in CLOSED) that open the breaker.
    failure_threshold: int = Field(default=5, ge=1, le=1000)
    # How long the breaker stays OPEN before allowing half-open probes.
    recovery_timeout_seconds: float = Field(default=10.0, gt=0, le=600)
    # Maximum concurrent probe requests while HALF_OPEN.
    half_open_max_probes: int = Field(default=1, ge=1, le=100)
    # Successful probes required to close the breaker again.
    success_threshold: int = Field(default=1, ge=1, le=100)
    # Upstream statuses counted as infrastructure failures. 4xx never count.
    failure_status_codes: tuple[int, ...] = (502, 503, 504)

    @field_validator("failure_status_codes")
    @classmethod
    def _server_errors_only(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        for code in value:
            if not 500 <= code <= 599:
                raise ValueError(f"failure status {code} must be a 5xx status")
        return value


class RateLimitConfig(_Strict):
    capacity: int = Field(ge=1, le=1_000_000)
    refill_rate_per_second: float = Field(gt=0, le=1_000_000)
    # Idle buckets expire after this long. Default: time to refill fully, x2.
    key_ttl_seconds: int | None = Field(default=None, ge=1, le=86_400)

    @property
    def effective_key_ttl_seconds(self) -> int:
        if self.key_ttl_seconds is not None:
            return self.key_ttl_seconds
        full_refill = self.capacity / self.refill_rate_per_second
        return max(1, min(86_400, int(full_refill * 2) + 1))


class RouteCacheConfig(_Strict):
    enabled: bool = False
    ttl_seconds: float = Field(default=10.0, gt=0, le=86_400)
    max_entry_bytes: int = Field(default=65_536, ge=1, le=10 * 1024 * 1024)
    allowed_status_codes: tuple[int, ...] = (200,)
    # Request headers that select a representation and are part of the cache key.
    vary_headers: tuple[str, ...] = ("accept", "accept-encoding")
    # Coalesce concurrent misses for the same key into one upstream request.
    coalesce: bool = True

    @field_validator("vary_headers")
    @classmethod
    def _lowercase(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        lowered = tuple(h.strip().lower() for h in value)
        forbidden = {"authorization", "cookie"} & set(lowered)
        if forbidden:
            raise ValueError(
                f"{sorted(forbidden)} cannot be cache-key headers; requests carrying "
                "credentials always bypass the cache"
            )
        return lowered

    @field_validator("allowed_status_codes")
    @classmethod
    def _cacheable_statuses(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        allowed = {200, 203, 204, 300, 301, 308, 404, 410}
        bad = sorted(set(value) - allowed)
        if bad:
            raise ValueError(f"statuses {bad} are not supported for caching")
        return value


class UpstreamConfig(_Strict):
    id: str
    url: str

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        return _validate_id(value)

    @field_validator("url")
    @classmethod
    def _valid_url(cls, value: str) -> str:
        try:
            parsed = httpx.URL(value)
        except httpx.InvalidURL as exc:
            raise ValueError(f"invalid URL: {exc}") from exc
        if parsed.scheme not in ("http", "https"):
            raise ValueError("scheme must be http or https")
        if not parsed.host:
            raise ValueError("URL must include a host")
        if parsed.userinfo:
            raise ValueError(
                "credentials must not be embedded in upstream URLs; keep secrets out of routes.yaml"
            )
        if parsed.query or parsed.fragment:
            raise ValueError("upstream URL must not contain a query string or fragment")
        return value.rstrip("/")


class RouteConfig(_Strict):
    id: str
    path_prefix: str
    # False: forward the full original path. True: remove path_prefix first.
    strip_prefix: bool = False
    upstreams: tuple[UpstreamConfig, ...] = Field(min_length=1, max_length=64)
    rate_limit: RateLimitConfig | None = None
    cache: RouteCacheConfig = RouteCacheConfig()
    retries: RetryConfig = RetryConfig()
    timeouts: TimeoutConfig = TimeoutConfig()
    health_check: HealthCheckConfig = HealthCheckConfig()
    circuit_breaker: CircuitBreakerConfig = CircuitBreakerConfig()

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        return _validate_id(value)

    @field_validator("path_prefix")
    @classmethod
    def _valid_prefix(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("must start with '/'")
        normalized = value.rstrip("/") or "/"
        if normalized != "/":
            for segment in normalized[1:].split("/"):
                if segment in ("", ".", "..") or not _PREFIX_SEGMENT.match(segment):
                    raise ValueError(f"invalid path segment {segment!r} in prefix")
        for reserved in RESERVED_PATH_PREFIXES:
            if normalized == "/" or _prefix_overlaps(normalized, reserved):
                raise ValueError(
                    f"prefix {normalized!r} would shadow the reserved operational path {reserved!r}"
                )
        return normalized

    @model_validator(mode="after")
    def _unique_upstreams(self) -> Self:
        ids = [u.id for u in self.upstreams]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(f"duplicate upstream ids in route {self.id!r}: {dupes}")
        return self


def _prefix_overlaps(prefix: str, reserved: str) -> bool:
    """True if either prefix contains the other on a path-segment boundary."""

    def within(path: str, base: str) -> bool:
        return path == base or path.startswith(base + "/")

    return within(prefix, reserved) or within(reserved, prefix)


class DefaultsConfig(_Strict):
    retries: RetryConfig = RetryConfig()
    timeouts: TimeoutConfig = TimeoutConfig()
    health_check: HealthCheckConfig = HealthCheckConfig()
    circuit_breaker: CircuitBreakerConfig = CircuitBreakerConfig()


_MERGED_SECTIONS = ("retries", "timeouts", "health_check", "circuit_breaker")


class RelayConfig(_Strict):
    server: ServerConfig = ServerConfig()
    redis: RedisConfig = RedisConfig()
    cache: CacheBackendConfig = CacheBackendConfig()
    defaults: DefaultsConfig = DefaultsConfig()
    routes: tuple[RouteConfig, ...]

    @model_validator(mode="before")
    @classmethod
    def _merge_defaults(cls, data: Any) -> Any:
        """Fill each route's policy sections from ``defaults`` (per-field)."""
        if not isinstance(data, dict):
            return data
        defaults = data.get("defaults") or {}
        if isinstance(defaults, BaseModel):
            defaults = defaults.model_dump()
        routes = data.get("routes")
        if not isinstance(defaults, dict) or not isinstance(routes, list | tuple):
            return data
        merged_routes = []
        for route in routes:
            if isinstance(route, dict):
                route = dict(route)
                for section in _MERGED_SECTIONS:
                    base = defaults.get(section)
                    if isinstance(base, dict):
                        override = route.get(section) or {}
                        if isinstance(override, dict):
                            route[section] = {**base, **override}
            merged_routes.append(route)
        return {**data, "routes": merged_routes}

    @model_validator(mode="after")
    def _cross_checks(self) -> Self:
        if not self.routes:
            raise ValueError("at least 1 route must be configured")
        ids = [r.id for r in self.routes]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(f"duplicate route ids: {dupes}")
        prefixes = [r.path_prefix for r in self.routes]
        dupe_prefixes = sorted({p for p in prefixes if prefixes.count(p) > 1})
        if dupe_prefixes:
            raise ValueError(f"duplicate path_prefix values: {dupe_prefixes}")
        needs_redis = [r.id for r in self.routes if r.rate_limit is not None]
        if self.cache.backend == "redis":
            needs_redis.append("<cache.backend=redis>")
        if needs_redis and self.redis.url is None:
            raise ValueError(f"redis.url (or RELAY_REDIS_URL) is required by: {needs_redis}")
        return self

    @property
    def redis_required(self) -> bool:
        return self.redis.url is not None and (
            self.cache.backend == "redis" or any(r.rate_limit for r in self.routes)
        )


def parse_config(data: dict[str, Any]) -> RelayConfig:
    """Validate an already-parsed mapping, raising ConfigError with readable details."""
    try:
        return RelayConfig.model_validate(data)
    except ValidationError as exc:
        lines = []
        for err in exc.errors():
            location = ".".join(str(part) for part in err["loc"]) or "<root>"
            lines.append(f"  - {location}: {err['msg']}")
        raise ConfigError("invalid Relay configuration:\n" + "\n".join(lines)) from None


def load_config(path: str | os.PathLike[str] | None = None) -> RelayConfig:
    """Load, override from the environment, and validate the YAML config file."""
    config_path = Path(path or os.environ.get("RELAY_CONFIG", "config/routes.yaml"))
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {config_path}") from None
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file {config_path} is not valid YAML: {exc}") from None
    if not isinstance(raw, dict):
        raise ConfigError(f"config file {config_path} must contain a YAML mapping")

    redis_override = os.environ.get("RELAY_REDIS_URL")
    if redis_override:
        raw = {**raw, "redis": {**(raw.get("redis") or {}), "url": redis_override}}
    return parse_config(raw)
