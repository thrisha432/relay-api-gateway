"""A/B throughput comparison of two Relay images under identical load.

Usage: python scripts/ab_compare.py IMAGE_A IMAGE_B [--reps 3] [--vus 10] [--duration 15s]

Both images run side by side on the Compose network with the same config and
backend; runs are interleaved (A, B, A, B, ...). Raw k6 summaries go to
benchmarks/results/ab/<stamp>/ and a table is printed. Run under `caffeinate`.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout  # noqa: S603, S607


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("images", nargs=2)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--vus", type=int, default=10)
    parser.add_argument("--duration", default="15s")
    parser.add_argument("--route", default="/api/bench")
    args = parser.parse_args()

    out = ROOT / "benchmarks" / "results" / "ab" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True)
    names = {}
    for i, image in enumerate(args.images):
        name = f"relay-ab-{i}"
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)  # noqa: S603, S607
        docker(
            "run",
            "-d",
            "--name",
            name,
            "--network",
            "relay_default",
            "-e",
            "RELAY_CONFIG=/app/config/routes.yaml",
            "-e",
            "RELAY_REDIS_URL=redis://redis:6379/0",
            image,
        )
        names[image] = name
    time.sleep(8)
    results: dict[str, list[dict]] = {image: [] for image in args.images}
    try:
        for rep in range(1, args.reps + 1):
            for image, name in names.items():
                label = f"{image.replace(':', '_').replace('/', '_')}-rep{rep}"
                docker(
                    "run",
                    "--rm",
                    "--network",
                    "relay_default",
                    "-v",
                    f"{ROOT / 'benchmarks' / 'k6'}:/scripts:ro",
                    "-v",
                    f"{out}:/out",
                    "-e",
                    f"BASE=http://{name}:8080",
                    "-e",
                    f"ROUTE={args.route}",
                    "-e",
                    f"VUS={args.vus}",
                    "-e",
                    f"DURATION={args.duration}",
                    "-e",
                    f"OUT=/out/{label}.json",
                    "grafana/k6:1.1.0",
                    "run",
                    "--quiet",
                    "/scripts/matrix.js",
                )
                data = json.loads((out / f"{label}.json").read_text())
                m = data["metrics"]
                row = {
                    "rps": m["http_reqs"]["values"]["rate"],
                    "p50": m["http_req_duration"]["values"]["med"],
                    "p99": m["http_req_duration"]["values"]["p(99)"],
                    "failed": m["http_req_failed"]["values"]["rate"] * 100,
                    "duration_s": data["state"]["testRunDurationMs"] / 1000,
                }
                results[image].append(row)
                print(
                    f"{image:<16} rep{rep}: {row['rps']:7.1f} req/s  p50 {row['p50']:.2f} ms  p99 {row['p99']:.2f} ms  failed {row['failed']:.2f}%  (ran {row['duration_s']:.1f}s)",
                    flush=True,
                )
                time.sleep(3)
    finally:
        for name in names.values():
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)  # noqa: S603, S607
    lines = [
        "| Image | Runs | Median req/s (min-max) | Median p50 ms | Median p99 ms |",
        "|---|---|---|---|---|",
    ]
    for image, rows in results.items():
        rps = [r["rps"] for r in rows]
        lines.append(
            f"| `{image}` | {len(rows)} | {statistics.median(rps):,.0f} ({min(rps):,.0f}-{max(rps):,.0f}) | "
            f"{statistics.median(r['p50'] for r in rows):.2f} | {statistics.median(r['p99'] for r in rows):.2f} |"
        )
    table = "\n".join(lines)
    (out / "SUMMARY.md").write_text(
        f"A/B: {args.vus} VUs, {args.duration}, route {args.route}\n\n{table}\n"
    )
    print(table)
    print(f"raw: {out}")


if __name__ == "__main__":
    main()
