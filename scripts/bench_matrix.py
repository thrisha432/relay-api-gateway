"""Benchmark matrix: run every experiment cell REPS times and save raw results.

Usage:
    python scripts/bench_matrix.py                      # everything (~75 min)
    python scripts/bench_matrix.py --only concurrency latency --reps 1 --duration 5s

Each run writes the raw k6 summary JSON to results/<stamp>/raw/. One row per
run goes to runs.csv (throughput, latency percentiles, errors, dropped
iterations, upstream request count, container CPU). summary.csv holds the
median and min/max per cell. scripts/plot_benchmarks.py turns these into
charts and SUMMARY.md. Nothing is copied by hand.

Fairness rules: one machine, one Docker network, same mock backend and
request mix for every path; repetitions are interleaved (rep 1 of every
cell, then rep 2, ...) so drift affects all cells equally; backend counters
are reset before each run, so upstream amplification is measured exactly.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import subprocess
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"
BACKENDS = {"a": "http://127.0.0.1:8001", "b": "http://127.0.0.1:8002"}

# mode -> (base URL inside the Docker network, route, KEYS, container(s) to sample)
MODES: dict[str, tuple[str, str, int, str]] = {
    "direct": ("http://backend-a:8001", "/api/bench", 100, "backend-a"),
    "nginx": ("http://nginx:8080", "/api/bench", 100, "nginx"),
    "relay-uncached": ("http://relay:8080", "/api/bench", 100, "relay"),
    "relay-cached": ("http://relay:8080", "/api/bench-cached", 50, "relay"),
    "relay-cold-cache": ("http://relay:8080", "/api/bench-cached", 0, "relay"),
    "relay-ratelimited": ("http://relay:8080", "/api/bench-limited", 100, "relay"),
    "relay-uncached-nolog": ("http://relay-nolog:8080", "/api/bench", 100, "relay-nolog"),
    "relay-cached-nolog": ("http://relay-nolog:8080", "/api/bench-cached", 50, "relay-nolog"),
    "relay-pool": ("http://relay-pool:8080", "/api/bench", 100, "relay-pool"),
    "relay-unlimited": ("http://relay-unlimited:8080", "/api/bench", 100, "relay-unlimited"),
    "relay-coalesce": ("http://relay:8080", "/api/bench-cached", 0, "relay"),
    "relay-nocoalesce": ("http://relay:8080", "/api/bench-nocoalesce", 0, "relay"),
    "relay-budget": ("http://relay:8080", "/api/bench-budget", 100, "relay"),
    "relay-nobudget": ("http://relay:8080", "/api/bench-nobudget", 100, "relay"),
}


@dataclass(frozen=True)
class Cell:
    experiment: str
    mode: str
    vus: int = 50
    executor: str = "closed"
    rate: int = 0
    delay_ms: int = 0
    herd_ms: int = 0
    instances: int = 1
    brownout: bool = False

    @property
    def name(self) -> str:
        parts = [self.experiment, self.mode]
        if self.executor == "open":
            parts.append(f"rate{self.rate}")
        else:
            parts.append(f"vus{self.vus}")
        if self.delay_ms:
            parts.append(f"delay{self.delay_ms}")
        if self.instances > 1 or self.mode == "relay-pool":
            parts.append(f"x{self.instances}")
        return "-".join(parts)


def experiments() -> dict[str, list[Cell]]:
    paths4 = ("direct", "nginx", "relay-uncached", "relay-cached")
    return {
        "concurrency": [
            Cell("concurrency", m, vus=v) for v in (1, 10, 25, 50, 100) for m in paths4
        ],
        "latency": [
            Cell("latency", m, vus=50, delay_ms=d)
            for d in (0, 10, 50, 100)
            for m in ("direct", "nginx", "relay-uncached")
        ],
        "openloop": [
            Cell("openloop", m, executor="open", rate=r)
            for r in (500, 1000, 1500, 2000, 2500, 3000, 3500)
            for m in ("nginx", "relay-uncached", "relay-unlimited")
        ],
        "scaling": [Cell("scaling", "relay-pool", vus=60, instances=n) for n in (1, 2, 4)],
        "ablation": [
            Cell("ablation", m, vus=50)
            for m in (
                "relay-cached",
                "relay-cached-nolog",
                "relay-uncached",
                "relay-uncached-nolog",
                "relay-ratelimited",
                "relay-cold-cache",
            )
        ],
        "stampede": [
            Cell("stampede", m, vus=100, herd_ms=200, delay_ms=50)
            for m in ("relay-coalesce", "relay-nocoalesce")
        ],
        "brownout": [
            Cell("brownout", m, vus=20, brownout=True) for m in ("relay-budget", "relay-nobudget")
        ],
    }


# ------------------------------------------------------------------ helpers


def sh(*args: str, check: bool = True, capture: bool = True) -> str:
    result = subprocess.run(args, cwd=ROOT, check=check, capture_output=capture, text=True)  # noqa: S603
    return result.stdout if capture else ""


def compose(*args: str, check: bool = True) -> str:
    return sh(
        "docker",
        "compose",
        "--profile",
        "multi",
        "--profile",
        "bench",
        "--profile",
        "scale",
        *args,
        check=check,
    )


def mock(backend: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body or {}).encode() if body is not None else None
    req = urllib.request.Request(
        f"{BACKENDS[backend]}{path}", data=data, method="POST" if data is not None else "GET"
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.load(resp)


def upstream_requests() -> int:
    return sum(sum(mock(b, "/__mock/stats")["requests"].values()) for b in BACKENDS)


class StatsSampler(threading.Thread):
    """Samples `docker stats` about once a second while a run is in progress."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.samples: list[tuple[str, float, float]] = []
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            out = sh(
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}},{{.CPUPerc}},{{.MemUsage}}",
                check=False,
            )
            for line in out.splitlines():
                try:
                    name, cpu, mem = line.split(",")
                    self.samples.append((name, float(cpu.rstrip("%")), _mib(mem)))
                except ValueError:
                    continue

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=10)

    def usage(self, container_prefix: str) -> tuple[float, float]:
        """Median total CPU % and peak memory (MiB) across matching containers."""
        pattern = re.compile(rf"^relay-{re.escape(container_prefix)}-\d+$")
        names = sorted({n for n, _, _ in self.samples if pattern.match(n)})
        if not names:
            return 0.0, 0.0
        cpu_rounds: list[float] = []
        current: dict[str, float] = {}
        mem_peak = 0.0
        for name, cpu, mem in self.samples:
            if name not in names:
                continue
            if name in current:  # a new sampling round started
                cpu_rounds.append(sum(current.values()))
                current = {}
            current[name] = cpu
            mem_peak = max(mem_peak, mem)
        if current:
            cpu_rounds.append(sum(current.values()))
        return (statistics.median(cpu_rounds) if cpu_rounds else 0.0), mem_peak


def _mib(value: str) -> float:
    used = value.split("/")[0].strip()
    num = float(re.sub(r"[^\d.]", "", used) or 0)
    return num / 1024 if "KiB" in used else num * 1024 if "GiB" in used else num


# --------------------------------------------------------------------- run


def wait_for_closed_breakers(timeout: float = 30.0) -> None:
    """Isolation between cells: never start a run while a previous run left a
    breaker open on the (host-reachable) main gateway."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen("http://127.0.0.1:8080/__relay/routes", timeout=2) as resp:
                routes = json.load(resp)["routes"]
            if all(u["circuit"] == "closed" for r in routes for u in r["upstreams"]):
                return
        except OSError:
            pass
        time.sleep(1)
    print("  WARNING: a circuit breaker stayed open; run may be affected", flush=True)


def run_cell(cell: Cell, rep: int, duration: str, raw_dir: Path, attempt: int = 1) -> dict:
    base, route, keys, container = MODES[cell.mode]
    wait_for_closed_breakers()
    out_name = f"{cell.name}-rep{rep}.json"
    for b in BACKENDS:
        mock(b, "/__mock/reset", {})
    if cell.brownout:  # backend-b fails every 2nd request with 503
        mock("b", "/__mock/config", {"fail_status": 503, "fail_every": 2})
    env = [
        f"BASE={base}",
        f"ROUTE={route}",
        f"KEYS={keys}",
        f"DURATION={duration}",
        f"EXECUTOR={cell.executor}",
        f"VUS={cell.vus}",
        f"RATE={cell.rate}",
        f"DELAY_MS={cell.delay_ms}",
        f"HERD_MS={cell.herd_ms}",
        f"OUT=/results/{raw_dir.relative_to(RESULTS).as_posix()}/{out_name}",
    ]
    sampler = StatsSampler()
    sampler.start()
    args = ["run", "--rm"]
    for item in env:
        args += ["-e", item]
    compose(*args, "k6", "run", "--quiet", "/scripts/matrix.js", check=False)
    sampler.stop()
    upstream = upstream_requests()  # read before any reset below
    if cell.brownout:
        mock("b", "/__mock/reset", {})

    summary = json.loads((raw_dir / out_name).read_text())
    planned_ms = _seconds(duration) * 1000
    actual_ms = summary["state"]["testRunDurationMs"]
    if actual_ms > planned_ms * 1.5:
        # The run was paused (e.g. the host slept, freezing the Docker VM) or
        # badly stalled: its numbers would be meaningless. Discard and redo it.
        (raw_dir / out_name).rename(raw_dir / f"{out_name}.invalid-{int(actual_ms)}ms")
        print(
            f"  WARNING: {cell.name} rep{rep} ran {actual_ms / 1000:.0f}s for a {duration} test; discarded, re-running",
            flush=True,
        )
        if attempt >= 3:
            raise SystemExit(f"{cell.name}: 3 stretched runs in a row; is the machine sleeping?")
        return run_cell(cell, rep, duration, raw_dir, attempt + 1)
    metrics = summary["metrics"]
    dur = metrics["http_req_duration"]["values"]
    reqs = metrics["http_reqs"]["values"]
    dropped = metrics.get("dropped_iterations", {}).get("values", {}).get("count", 0)
    cpu, mem = sampler.usage(container)
    row = {
        **asdict(cell),
        "cell": cell.name,
        "rep": rep,
        "rps": round(reqs["rate"], 1),
        "requests": int(reqs["count"]),
        "p50_ms": round(dur["med"], 3),
        "p95_ms": round(dur["p(95)"], 3),
        "p99_ms": round(dur["p(99)"], 3),
        "max_ms": round(dur["max"], 3),
        "error_pct": round(metrics["http_req_failed"]["values"]["rate"] * 100, 4),
        "dropped": int(dropped),
        "upstream_requests": upstream,
        "upstream_per_request": round(upstream / max(1, reqs["count"]), 4),
        "cpu_pct": round(cpu, 1),
        "mem_mib": round(mem, 1),
    }
    print(
        f"  {cell.name:<48} rep{rep}  {row['rps']:>9} req/s  p50 {row['p50_ms']:>8} ms  "
        f"p99 {row['p99_ms']:>8} ms  err {row['error_pct']}%  up/req {row['upstream_per_request']}  "
        f"cpu {row['cpu_pct']}%",
        flush=True,
    )
    return row


def _seconds(duration: str) -> float:
    return float(duration.rstrip("s"))


def set_pool_size(n: int) -> None:
    compose("up", "-d", "--scale", f"relay-pool={n}", "relay-pool")
    time.sleep(8)  # lifespan startup + first health-check round


def environment() -> str:
    lines = [
        f"timestamp_utc: {datetime.now(UTC).isoformat(timespec='seconds')}",
        f"git_commit: {sh('git', 'rev-parse', '--short', 'HEAD', check=False).strip()}",
        f"machine: {sh('sysctl', '-n', 'machdep.cpu.brand_string', check=False).strip()}, "
        f"{sh('sysctl', '-n', 'hw.ncpu', check=False).strip()} cores",
        f"os: macOS {sh('sw_vers', '-productVersion', check=False).strip()}",
        f"docker: {sh('docker', 'version', '--format', '{{.Server.Version}}', check=False).strip()}, "
        f"VM {sh('docker', 'info', '--format', '{{.NCPU}} CPUs {{.MemTotal}} bytes', check=False).strip()}",
        f"relay python: {compose('exec', '-T', 'relay', 'python', '--version', check=False).strip()}",
        "relay: 1 uvicorn worker (uvloop, httptools) per instance; access log on unless *-nolog",
        "nginx: 1.27.5, worker_processes 1, upstream keepalive 64, access_log off",
        "k6: grafana/k6:1.1.0 in the same Docker network; mock backend: 1 uvicorn worker",
    ]
    return "\n".join(lines) + "\n"


def summarize(rows: list[dict], out: Path) -> None:
    by_cell: dict[str, list[dict]] = {}
    for row in rows:
        by_cell.setdefault(row["cell"], []).append(row)
    metrics = [
        "rps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "error_pct",
        "dropped",
        "upstream_per_request",
        "cpu_pct",
        "mem_mib",
    ]
    keys = [
        "experiment",
        "mode",
        "vus",
        "executor",
        "rate",
        "delay_ms",
        "herd_ms",
        "instances",
        "cell",
    ]
    with out.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [*keys, "reps", *[f"{m}_{s}" for m in metrics for s in ("median", "min", "max")]]
        )
        for cell_rows in by_cell.values():
            first = cell_rows[0]
            values = []
            for m in metrics:
                col = [r[m] for r in cell_rows]
                values += [statistics.median(col), min(col), max(col)]
            writer.writerow([*[first[k] for k in keys], len(cell_rows), *values])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", help="experiment names to run")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration", default="20s")
    parser.add_argument("--out", help="output directory (default: results/<timestamp>-matrix)")
    parser.add_argument("--match", nargs="*", help="only cells whose name contains one of these")
    args = parser.parse_args()

    selected = {k: v for k, v in experiments().items() if not args.only or k in args.only}
    if args.match:
        selected = {
            k: [c for c in v if any(m in c.name for m in args.match)] for k, v in selected.items()
        }
        selected = {k: v for k, v in selected.items() if v}
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = (ROOT / args.out).resolve() if args.out else RESULTS / f"{stamp}-matrix"
    raw_dir = out / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("starting stack ...", flush=True)
    compose("up", "-d", "--build", "--scale", "relay-pool=1")
    # NGINX resolves upstream hostnames once at startup; if `up` recreated the
    # backends (new IPs), it would keep dialing the old address. Restart it so
    # it resolves the current ones. (Relay/httpx resolves per new connection.)
    compose("restart", "nginx")
    time.sleep(10)
    for path in ("/api/bench/items?key=check",):
        out_check = compose(
            "run",
            "--rm",
            "--entrypoint",
            "wget",
            "k6",
            "-q",
            "-O",
            "-",
            f"http://nginx:8080{path}",
            check=False,
        )
        if "backend-a" not in out_check:
            raise SystemExit(f"NGINX baseline is not proxying correctly: {out_check[:200]!r}")
    (out / "environment.txt").write_text(environment())

    rows: list[dict] = []
    runs_csv = out / "runs.csv"
    for rep in range(1, args.reps + 1):
        for name, cells in selected.items():
            print(f"== {name} (rep {rep}/{args.reps})", flush=True)
            if name == "scaling":
                for cell in cells:
                    set_pool_size(cell.instances)
                    rows.append(run_cell(cell, rep, args.duration, raw_dir))
                set_pool_size(1)
                continue
            for cell in cells:
                rows.append(run_cell(cell, rep, args.duration, raw_dir))
            # Write incrementally so a long run is never lost.
            with runs_csv.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
    with runs_csv.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summarize(rows, out / "summary.csv")
    print(f"results: {out}")


if __name__ == "__main__":
    main()
