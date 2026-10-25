"""Generate deploy/grafana/dashboards/relay.json.

The dashboard is generated (rather than hand-edited JSON) so every panel's
PromQL is reviewable in one place. Run: python scripts/build_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "deploy" / "grafana" / "dashboards" / "relay.json"
DS = {"type": "prometheus", "uid": "relay-prometheus"}
R = 'route=~"$route"'
RI = "$__rate_interval"

_next_id = 0


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id


def target(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}


def stat(title, expr, x, unit="short", decimals=2, thresholds=None, description=""):
    return {
        "id": _id(),
        "type": "stat",
        "title": title,
        "description": description,
        "datasource": DS,
        "gridPos": {"h": 4, "w": 4, "x": x, "y": 0},
        "targets": [target(expr)],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": thresholds or [{"color": "green", "value": None}],
                },
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "area",
            "textMode": "auto",
        },
    }


def series(title, targets, x, y, unit="short", w=12, h=8, description="", extra=None):
    panel = {
        "id": _id(),
        "type": "timeseries",
        "title": title,
        "description": description,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [
            target(expr, legend, chr(ord("A") + i)) for i, (expr, legend) in enumerate(targets)
        ],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10},
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull"]},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }
    if extra:
        panel["fieldConfig"]["defaults"].update(extra)
    return panel


def quantile(q: float, by: str = "") -> str:
    group = f"le{', ' + by if by else ''}"
    return (
        f"histogram_quantile({q}, sum by ({group}) "
        f"(rate(relay_request_duration_seconds_bucket{{{R}}}[{RI}])))"
    )


def build() -> dict:
    total = f'sum(rate(relay_requests_total{{{R}, route!="_ops"}}[{RI}]))'
    # "or vector(0)": show 0 rather than "No data" when no 5xx has ever happened.
    errors = f'(sum(rate(relay_requests_total{{{R}, route!="_ops", status_class="5xx"}}[{RI}])) or vector(0))'
    hits = f'sum(rate(relay_cache_requests_total{{{R}, result="hit"}}[{RI}]))'
    lookups = f'sum(rate(relay_cache_requests_total{{{R}, result=~"hit|miss"}}[{RI}]))'
    # max by (route, upstream): with two gateway instances each reports the gauge.
    healthy = "max by (route, upstream) (relay_upstream_healthy{%s})" % R
    red = [{"color": "green", "value": None}, {"color": "red", "value": 1}]

    panels = [
        stat("Request rate", total, 0, unit="reqps"),
        stat(
            "5xx error rate",
            f"100 * {errors} / clamp_min({total}, 1e-9)",
            4,
            unit="percent",
            thresholds=[
                {"color": "green", "value": None},
                {"color": "orange", "value": 1},
                {"color": "red", "value": 5},
            ],
        ),
        stat("p95 latency", quantile(0.95), 8, unit="s", decimals=4),
        stat(
            "Cache hit ratio",
            f"{hits} / clamp_min({lookups}, 1e-9)",
            12,
            unit="percentunit",
            description="Hits / (hits + misses). Bypassed requests are excluded.",
        ),
        stat(
            "Healthy upstreams",
            f"sum({healthy}) / count({healthy})",
            16,
            unit="percentunit",
            thresholds=[
                {"color": "red", "value": None},
                {"color": "orange", "value": 0.5},
                {"color": "green", "value": 1},
            ],
        ),
        stat(
            "Open circuits",
            f"count(relay_circuit_breaker_state{{{R}}} == 2) or vector(0)",
            20,
            decimals=0,
            thresholds=red,
        ),
        series(
            "Requests by route and status class",
            [
                (
                    f'sum by (route, status_class) (rate(relay_requests_total{{{R}, route!="_ops"}}[{RI}]))',
                    "{{route}} {{status_class}}",
                )
            ],
            0,
            4,
            unit="reqps",
        ),
        series(
            "Gateway latency (p50 / p95 / p99)",
            [(quantile(0.5), "p50"), (quantile(0.95), "p95"), (quantile(0.99), "p99")],
            12,
            4,
            unit="s",
        ),
        series(
            "5xx error rate by route",
            [
                (
                    f'100 * sum by (route) (rate(relay_requests_total{{{R}, status_class="5xx"}}[{RI}]))'
                    f" / clamp_min(sum by (route) (rate(relay_requests_total{{{R}}}[{RI}])), 1e-9)",
                    "{{route}}",
                )
            ],
            0,
            12,
            unit="percent",
        ),
        series(
            "Upstream attempt latency p95 by instance",
            [
                (
                    "histogram_quantile(0.95, sum by (le, route, upstream) "
                    f"(rate(relay_upstream_request_duration_seconds_bucket{{{R}}}[{RI}])))",
                    "{{route}}/{{upstream}}",
                )
            ],
            12,
            12,
            unit="s",
        ),
        series(
            "Cache lookups (hit / miss / bypass)",
            [
                (
                    f"sum by (result) (rate(relay_cache_requests_total{{{R}}}[{RI}]))",
                    "{{result}}",
                ),
                ("sum(rate(relay_cache_evictions_total[%s]))" % RI, "evictions"),
            ],
            0,
            20,
            unit="ops",
        ),
        series(
            "Rate-limit decisions",
            [
                (
                    f"sum by (route, decision) (rate(relay_rate_limit_decisions_total{{{R}}}[{RI}]))",
                    "{{route}} {{decision}}",
                )
            ],
            12,
            20,
            unit="ops",
            description="limited = 429; fail_closed = 503 because Redis was unreachable.",
        ),
        series(
            "Upstream health (1 = healthy)",
            [(healthy, "{{route}}/{{upstream}}")],
            0,
            28,
            extra={
                "min": 0,
                "max": 1,
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "stepAfter",
                    "lineWidth": 2,
                    "fillOpacity": 0,
                },
            },
        ),
        series(
            "Circuit-breaker state (0 closed, 1 half-open, 2 open)",
            [
                (
                    f"max by (route, upstream) (relay_circuit_breaker_state{{{R}}})",
                    "{{route}}/{{upstream}}",
                )
            ],
            12,
            28,
            extra={
                "min": 0,
                "max": 2,
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "stepAfter",
                    "lineWidth": 2,
                    "fillOpacity": 0,
                },
            },
        ),
        series(
            "Retries and exhausted retries",
            [
                (
                    f"sum by (route, reason) (rate(relay_retries_total{{{R}}}[{RI}]))",
                    "retry {{route}} {{reason}}",
                ),
                (
                    f"sum by (route) (rate(relay_retries_exhausted_total{{{R}}}[{RI}]))",
                    "exhausted {{route}}",
                ),
            ],
            0,
            36,
            unit="ops",
        ),
        series(
            "Failures: no healthy upstream, circuit rejections, Redis errors",
            [
                (
                    f"sum by (route) (rate(relay_no_healthy_upstream_total{{{R}}}[{RI}]))",
                    "no healthy upstream {{route}}",
                ),
                (
                    f"sum by (route, upstream) (rate(relay_circuit_breaker_rejections_total{{{R}}}[{RI}]))",
                    "circuit rejected {{route}}/{{upstream}}",
                ),
                (
                    "sum by (operation) (rate(relay_redis_errors_total[%s]))" % RI,
                    "redis error {{operation}}",
                ),
            ],
            12,
            36,
            unit="ops",
        ),
        series(
            "In-flight requests",
            [("sum(relay_in_flight_requests)", "in flight")],
            0,
            44,
        ),
        series(
            "Gateway saturation: event-loop lag",
            [("max(relay_event_loop_lag_seconds)", "worst timer lateness")],
            0,
            52,
            unit="s",
            description="How late a 100 ms timer fires. High values mean Relay itself is CPU-bound, so latency is not the upstream's fault.",
        ),
        series(
            "Load shed (503 overloaded)",
            [(f"sum by (route) (rate(relay_load_shed_total{{{R}}}[{RI}]))", "{{route}}")],
            12,
            52,
            unit="ops",
            description="Requests rejected immediately because max_in_flight was reached.",
        ),
        series(
            "Health-check probe failures",
            [
                (
                    f'sum by (route, upstream) (rate(relay_health_checks_total{{{R}, result="failure"}}[{RI}]))',
                    "{{route}}/{{upstream}}",
                )
            ],
            12,
            44,
            unit="ops",
        ),
    ]
    return {
        "uid": "relay-overview",
        "title": "Relay API Gateway",
        "description": "Local portfolio demo: request rate, latency, errors, cache, rate limiting, upstream health, circuit breakers.",
        "tags": ["relay", "gateway"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {
            "list": [
                {
                    "name": "route",
                    "label": "Route",
                    "type": "query",
                    "datasource": DS,
                    "query": {
                        "query": "label_values(relay_requests_total, route)",
                        "refId": "route",
                    },
                    "definition": "label_values(relay_requests_total, route)",
                    "includeAll": True,
                    "multi": True,
                    "allValue": ".*",
                    "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
                    "refresh": 2,
                }
            ]
        },
        "panels": panels,
    }


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
