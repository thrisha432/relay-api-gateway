"""Chaos timelines: inject a failure into steady traffic and record what
clients and the gateway did, second by second.

Usage:
    python scripts/chaos_timeline.py                     # all scenarios, 3 reps (~25 min)
    python scripts/chaos_timeline.py --only backend-down --reps 1

For each run:
* k6 sends a fixed RATE req/s for 70 s (open loop), through Relay;
* at t=FAULT_AT the fault is injected; at t=RECOVER_AT it is undone;
* the client-side timeline (per-second status codes and latency buckets)
  comes from k6, and the gateway-side view (breaker state and health per
  upstream, once a second) from GET /__relay/routes;
* gateway counters (/metrics) are diffed across the run.

Outputs to results/<stamp>-chaos/: <scenario>-rep<N>.timeline.csv, a
<scenario>-rep<N>.json with events and derived numbers, and chaos_summary.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import threading
import time
import urllib.request
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"
GATEWAY = "http://127.0.0.1:8080"
BUCKETS = [2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
FAULT_AT, RECOVER_AT, DURATION, RATE = 20, 45, 70, 300


def sh(*args: str, check: bool = True) -> str:
    return subprocess.run(args, cwd=ROOT, check=check, capture_output=True, text=True).stdout  # noqa: S603


def compose(*args: str, check: bool = True) -> str:
    return sh("docker", "compose", "--profile", "bench", *args, check=check)


def http_json(url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=3) as resp:
        return json.load(resp)


def mock(port: int, path: str, body: dict | None = None) -> None:
    http_json(f"http://127.0.0.1:{port}{path}", body if body is not None else {})


def metrics_text() -> str:
    try:
        with urllib.request.urlopen(f"{GATEWAY}/metrics", timeout=3) as resp:
            return resp.read().decode()
    except OSError:
        return ""


def counter_totals(text: str, route: str) -> dict[str, float]:
    """Sum selected counters for one route, keyed by name and relevant labels."""
    out: dict[str, float] = defaultdict(float)
    for line in text.splitlines():
        if line.startswith("#") or f'route="{route}"' not in line:
            continue
        name, _, value = line.rpartition(" ")
        metric = name.split("{", 1)[0]
        if metric in (
            "relay_upstream_requests_total",
            "relay_retries_total",
            "relay_retries_exhausted_total",
            "relay_retry_budget_exhausted_total",
            "relay_circuit_breaker_transitions_total",
            "relay_circuit_breaker_rejections_total",
            "relay_no_healthy_upstream_total",
            "relay_rate_limit_decisions_total",
        ):
            labels = name.split("{", 1)[1].rstrip("}")
            keep = ",".join(p for p in labels.split(",") if not p.startswith(("route=", "method=")))
            out[f"{metric}{{{keep}}}"] += float(value)
    return dict(out)


@dataclass(frozen=True)
class Scenario:
    name: str
    route: str
    route_id: str
    description: str
    fault: Callable[[], None]
    recover: Callable[[], None]


def scenarios() -> list[Scenario]:
    def reset_all() -> None:
        for port in (8001, 8002):
            mock(port, "/__mock/reset")

    return [
        Scenario(
            "backend-down",
            "/api/bench-ha",
            "bench-ha",
            "backend-b container stopped (connection refused), then started",
            lambda: compose("stop", "backend-b"),
            lambda: compose("start", "backend-b"),
        ),
        Scenario(
            "backend-hang",
            "/api/bench-ha",
            "bench-ha",
            "backend-b accepts requests but responds after 10 s (health endpoint still fine)",
            lambda: mock(8002, "/__mock/config", {"delay_ms": 10000}),
            lambda: mock(8002, "/__mock/reset"),
        ),
        Scenario(
            "backend-hang-lr",
            "/api/bench-ha-lr",
            "bench-ha-lr",
            "same hang as backend-hang, but the route uses least-request balancing",
            lambda: mock(8002, "/__mock/config", {"delay_ms": 10000}),
            lambda: mock(8002, "/__mock/reset"),
        ),
        Scenario(
            "backend-503",
            "/api/bench-ha",
            "bench-ha",
            "backend-b returns 503 to every request and fails health checks",
            lambda: mock(8002, "/__mock/config", {"fail_status": 503, "healthy": False}),
            lambda: mock(8002, "/__mock/reset"),
        ),
        Scenario(
            "total-outage",
            "/api/bench-ha",
            "bench-ha",
            "both backends return 503 (no healthy capacity at all)",
            lambda: [mock(p, "/__mock/config", {"fail_status": 503}) for p in (8001, 8002)],
            reset_all,
        ),
        Scenario(
            "redis-down",
            "/api/bench-limited",
            "bench-limited",
            "Redis container stopped on a rate-limited route (fail-closed policy), then started",
            lambda: compose("stop", "redis"),
            lambda: compose("start", "redis"),
        ),
        Scenario(
            "gateway-restart",
            "/api/bench-ha",
            "bench-ha",
            "the Relay container is restarted under traffic (SIGTERM, then start)",
            lambda: compose("restart", "relay"),
            lambda: None,
        ),
    ]


def vm_clock_offset() -> float:
    """Docker VM clock minus host clock, in seconds.

    k6 timestamps come from the VM; fault injection and state polling use host
    time. After the host sleeps, the VM clock can be off by seconds or more,
    so every run measures the offset and converts between the two.
    """
    before = time.time()
    vm = float(
        compose("exec", "-T", "relay", "python", "-c", "import time; print(time.time())").strip()
    )
    after = time.time()
    return vm - (before + after) / 2


class RouteStatePoller(threading.Thread):
    """Records breaker state and health of each upstream once per second (VM-clock keyed)."""

    def __init__(self, route_id: str, vm_offset: float = 0.0) -> None:
        super().__init__(daemon=True)
        self.route_id = route_id
        self.vm_offset = vm_offset
        self.rows: dict[int, dict[str, str]] = {}
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            sec = int(time.time() + self.vm_offset)
            try:
                routes = http_json(f"{GATEWAY}/__relay/routes")["routes"]
                route = next(r for r in routes if r["id"] == self.route_id)
                state = {}
                for up in route["upstreams"]:
                    state[f"{up['id']}_circuit"] = up["circuit"]
                    state[f"{up['id']}_healthy"] = str(up["healthy"])
                self.rows[sec] = state
            except (OSError, StopIteration, KeyError, ValueError):
                self.rows[sec] = {"gateway": "unreachable"}
            self._stop.wait(1.0)

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=5)


def parse_tl(line: str) -> dict | None:
    """Parse one k6 console line: time=... level=info msg="TL {...}" source=console."""
    if "TL {" not in line:
        return None
    payload = line.split("TL ", 1)[1].split('" source=')[0]
    return json.loads(payload.replace('\\"', '"').strip())


def percentile_from_buckets(counts: list[int], q: float) -> float:
    total = sum(counts)
    if total == 0:
        return 0.0
    target = q * total
    running = 0
    for edge, count in zip(BUCKETS, counts, strict=False):
        running += count
        if running >= target:
            return float(edge)
    return float(BUCKETS[-1])


def run(scenario: Scenario, rep: int, out: Path, attempt: int = 1) -> dict:
    for port in (8001, 8002):
        mock(port, "/__mock/reset")
    time.sleep(3)  # let health checks and breakers settle
    vm_offset = vm_clock_offset()
    before = counter_totals(metrics_text(), scenario.route_id)
    poller = RouteStatePoller(scenario.route_id, vm_offset)
    poller.start()

    raw_name = f"{scenario.name}-rep{rep}.k6.json"
    proc = subprocess.Popen(  # noqa: S603
        [
            "docker",
            "compose",
            "--profile",
            "bench",
            "run",
            "--rm",
            "-e",
            f"ROUTE={scenario.route}",
            "-e",
            f"RATE={RATE}",
            "-e",
            f"DURATION={DURATION}s",
            "-e",
            f"OUT=/results/{out.relative_to(RESULTS).as_posix()}/{raw_name}",
            "k6",
            "run",
            "--quiet",
            "/scripts/chaos.js",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines: list[str] = []
    reader = threading.Thread(target=lambda: lines.extend(proc.stdout or []), daemon=True)
    reader.start()

    # Wait for the first TL line so event times are relative to real traffic start.
    start = None
    for _ in range(300):
        records = [r for r in map(parse_tl, list(lines)) if r]
        if records:
            start = min(r["s"] for r in records)
            break
        time.sleep(0.1)
    if start is None:
        start = int(time.time() + vm_offset)
    start_host = start - vm_offset  # the same instant on the host clock
    events = {}
    time.sleep(max(0, start_host + FAULT_AT - time.time()))
    events["fault"] = time.time() - start_host
    scenario.fault()
    time.sleep(max(0, start_host + RECOVER_AT - time.time()))
    events["recover"] = time.time() - start_host
    scenario.recover()
    proc.wait()
    reader.join(timeout=5)
    poller.stop()
    k6_summary = out / raw_name
    ran_s = (
        json.loads(k6_summary.read_text())["state"]["testRunDurationMs"] / 1000
        if k6_summary.exists()
        else 0
    )
    if not k6_summary.exists() or ran_s > DURATION * 1.5:
        # Paused (host sleep froze the Docker VM) or failed: never keep it.
        if k6_summary.exists():
            k6_summary.rename(out / f"{raw_name}.invalid-{int(ran_s)}s")
        print(
            f"  WARNING: {scenario.name} rep{rep} ran {ran_s:.0f}s for a {DURATION}s test; discarded, re-running",
            flush=True,
        )
        if attempt >= 3:
            raise SystemExit(f"{scenario.name}: 3 invalid runs in a row; is the machine sleeping?")
        scenario.recover()
        return run(scenario, rep, out, attempt + 1)
    after = counter_totals(metrics_text(), scenario.route_id)

    # Merge per-VU per-second records.
    per_sec: dict[int, dict] = defaultdict(
        lambda: {"n": 0, "codes": defaultdict(int), "b": [0] * (len(BUCKETS) + 1)}
    )
    for rec in filter(None, map(parse_tl, lines)):
        agg = per_sec[rec["s"] - start]
        agg["n"] += rec["n"]
        for code, count in rec["codes"].items():
            agg["codes"][code] += count
        agg["b"] = [x + y for x, y in zip(agg["b"], rec["b"], strict=True)]

    timeline = []
    codes_seen = sorted({c for s in per_sec.values() for c in s["codes"]})
    for t in sorted(per_sec):
        if t < 0 or t >= DURATION:
            continue
        s = per_sec[t]
        state = poller.rows.get(start + t, {})
        timeline.append(
            {
                "t": t,
                "requests": s["n"],
                **{f"status_{c}": s["codes"].get(c, 0) for c in codes_seen},
                "errors": sum(v for c, v in s["codes"].items() if not c.startswith(("2", "3"))),
                "p50_ms": percentile_from_buckets(s["b"], 0.5),
                "p99_ms": percentile_from_buckets(s["b"], 0.99),
                **state,
            }
        )
    with (out / f"{scenario.name}-rep{rep}.timeline.csv").open("w", newline="") as fh:
        fields = sorted({k for row in timeline for k in row}, key=lambda k: (k != "t", k))
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(timeline)

    def window(lo: float, hi: float) -> list[dict]:
        return [r for r in timeline if lo <= r["t"] < hi]

    fault_rows, after_rows = window(FAULT_AT, RECOVER_AT), window(RECOVER_AT, DURATION)
    baseline_rows = window(5, FAULT_AT)
    err = lambda rows: sum(r["errors"] for r in rows)  # noqa: E731
    total = lambda rows: sum(r["requests"] for r in rows)  # noqa: E731
    last_error = max((r["t"] for r in timeline if r["errors"]), default=None)
    first_error = min((r["t"] for r in fault_rows if r["errors"]), default=None)
    deltas = {k: after.get(k, 0) - before.get(k, 0) for k in set(before) | set(after)}
    result = {
        "scenario": scenario.name,
        "description": scenario.description,
        "rep": rep,
        "events_s": events,
        "vm_clock_offset_s": round(vm_offset, 3),
        "baseline_p99_ms": statistics.median([r["p99_ms"] for r in baseline_rows])
        if baseline_rows
        else None,
        "fault_window_requests": total(fault_rows),
        "fault_window_errors": err(fault_rows),
        "fault_window_error_pct": round(100 * err(fault_rows) / max(1, total(fault_rows)), 3),
        "fault_window_max_p99_ms": max((r["p99_ms"] for r in fault_rows), default=None),
        "after_recovery_errors": err(after_rows),
        "first_error_s": first_error,
        "last_error_s": last_error,
        "error_free_after_s": (last_error + 1 - RECOVER_AT)
        if last_error and last_error >= RECOVER_AT
        else 0,
        "total_requests": total(timeline),
        "total_errors": err(timeline),
        # Counters reset when the gateway restarts, so deltas are meaningless there.
        "gateway_counter_deltas": {k: v for k, v in sorted(deltas.items()) if v}
        if scenario.name != "gateway-restart"
        else "n/a (process restarted)",
    }
    (out / f"{scenario.name}-rep{rep}.json").write_text(json.dumps(result, indent=2))
    print(
        f"  {scenario.name:<16} rep{rep}: fault-window errors {result['fault_window_errors']}/"
        f"{result['fault_window_requests']} ({result['fault_window_error_pct']}%), "
        f"max p99 {result['fault_window_max_p99_ms']} ms, errors after recovery {result['after_recovery_errors']}",
        flush=True,
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS / f"{stamp}-chaos"
    out.mkdir(parents=True, exist_ok=True)
    compose("up", "-d", "--build")
    time.sleep(10)
    selected = [s for s in scenarios() if not args.only or s.name in args.only]
    results = []
    for rep in range(1, args.reps + 1):
        for scenario in selected:
            results.append(run(scenario, rep, out))
            time.sleep(5)
    keys = [k for k in results[0] if k not in ("gateway_counter_deltas", "events_s", "description")]
    with (out / "chaos_summary.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    print(f"results: {out}")


if __name__ == "__main__":
    main()
