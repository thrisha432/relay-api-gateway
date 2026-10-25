"""Prometheus metrics.

Label values are restricted to bounded sets: route ids and upstream ids from
config, a fixed list of HTTP methods (others become ``OTHER``), status
classes (``2xx``...), and small fixed enums. Raw paths, query strings, IPs,
user identifiers, and exception messages are never used as labels.

Each app gets its own ``CollectorRegistry`` so several gateways can run in one
process (as the tests do) without duplicate-metric errors.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.process_collector import ProcessCollector
from prometheus_client.registry import Collector

KNOWN_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})

# Latency buckets tuned for a local gateway: sub-millisecond to several seconds.
LATENCY_BUCKETS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


def method_label(method: str) -> str:
    method = method.upper()
    return method if method in KNOWN_METHODS else "OTHER"


def status_class(status: int) -> str:
    return f"{status // 100}xx" if 100 <= status <= 599 else "other"


class RelayMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        ProcessCollector(registry=r)  # CPU/memory on Linux (the Docker image); no-op on macOS

        self.requests_total = Counter(
            "relay_requests_total",
            "Requests handled by the gateway.",
            ["route", "method", "status_class"],
            registry=r,
        )
        self.request_duration = Histogram(
            "relay_request_duration_seconds",
            "End-to-end gateway request latency.",
            ["route"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.in_flight = Gauge(
            "relay_in_flight_requests", "Requests currently being processed.", registry=r
        )
        self.upstream_requests_total = Counter(
            "relay_upstream_requests_total",
            "Upstream attempts by outcome (status class or failure kind).",
            ["route", "upstream", "outcome"],
            registry=r,
        )
        self.upstream_duration = Histogram(
            "relay_upstream_request_duration_seconds",
            "Latency of individual upstream attempts.",
            ["route", "upstream"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.no_healthy_upstream_total = Counter(
            "relay_no_healthy_upstream_total",
            "Requests rejected because no upstream was healthy.",
            ["route"],
            registry=r,
        )
        self.health_checks_total = Counter(
            "relay_health_checks_total",
            "Active health-check probes by result.",
            ["route", "upstream", "result"],
            registry=r,
        )
        self.upstream_healthy = Gauge(
            "relay_upstream_healthy",
            "1 if the upstream instance is considered healthy, else 0.",
            ["route", "upstream"],
            registry=r,
        )
        self.rate_limit_decisions_total = Counter(
            "relay_rate_limit_decisions_total",
            "Rate-limit decisions (allowed, limited, fail_open, fail_closed).",
            ["route", "decision"],
            registry=r,
        )
        self.cache_requests_total = Counter(
            "relay_cache_requests_total",
            "Cache lookups by result (hit, miss, bypass).",
            ["route", "result"],
            registry=r,
        )
        self.cache_stores_total = Counter(
            "relay_cache_stores_total",
            "Cache fill attempts by outcome (stored, not_cacheable, error).",
            ["route", "outcome"],
            registry=r,
        )
        self.cache_coalesced_total = Counter(
            "relay_cache_coalesced_total",
            "Cache misses answered by joining an identical in-flight upstream request.",
            ["route"],
            registry=r,
        )
        self.cache_evictions_total = Counter(
            "relay_cache_evictions_total",
            "Entries evicted from the local LRU because it was full.",
            registry=r,
        )
        self.retries_total = Counter(
            "relay_retries_total",
            "Retry attempts by reason (status, timeout, connect_error, bad_response).",
            ["route", "reason"],
            registry=r,
        )
        self.retries_exhausted_total = Counter(
            "relay_retries_exhausted_total",
            "Requests that still failed after using every allowed attempt.",
            ["route"],
            registry=r,
        )
        self.retry_budget_exhausted_total = Counter(
            "relay_retry_budget_exhausted_total",
            "Retries skipped because the route's retry budget was empty.",
            ["route"],
            registry=r,
        )
        self.circuit_transitions_total = Counter(
            "relay_circuit_breaker_transitions_total",
            "Circuit-breaker state transitions.",
            ["route", "upstream", "from_state", "to_state"],
            registry=r,
        )
        self.circuit_rejections_total = Counter(
            "relay_circuit_breaker_rejections_total",
            "Attempts short-circuited because the breaker was open.",
            ["route", "upstream"],
            registry=r,
        )
        self.redis_errors_total = Counter(
            "relay_redis_errors_total",
            "Failed Redis operations.",
            ["operation"],
            registry=r,
        )

    def register_collector(self, collector: Collector) -> None:
        self.registry.register(collector)

    def render(self) -> bytes:
        return generate_latest(self.registry)


class CallbackGauge(Collector):
    """Gauge whose samples are computed at scrape time from a callback.

    Used for circuit-breaker state, so the exported value can never drift
    from the breaker's real state.
    """

    def __init__(
        self,
        name: str,
        documentation: str,
        labels: list[str],
        callback: Callable[[], Iterable[tuple[Sequence[str], float]]],
    ) -> None:
        self._name = name
        self._doc = documentation
        self._labels = labels
        self._callback = callback

    def collect(self) -> Iterator[Metric]:
        family = GaugeMetricFamily(self._name, self._doc, labels=self._labels)
        for label_values, value in self._callback():
            family.add_metric(list(label_values), value)
        yield family

    def describe(self) -> list[Metric]:
        return [GaugeMetricFamily(self._name, self._doc, labels=self._labels)]
