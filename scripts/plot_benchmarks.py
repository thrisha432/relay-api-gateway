"""Charts + SUMMARY.md from a benchmark matrix (and optional chaos) directory.

Usage:
    python scripts/plot_benchmarks.py benchmarks/results/<stamp>-matrix [benchmarks/results/<stamp>-chaos]

Reads only summary.csv / runs.csv / chaos *.json + *.timeline.csv. Points
are medians across repetitions; error bars show the min-max range.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import contextlib

import matplotlib.pyplot as plt

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
# Fixed categorical order (identity follows the entity, never its rank).
SERIES = {
    "direct": ("#2a78d6", "o", "Direct to backend"),
    "nginx": ("#eb6834", "s", "NGINX (1 worker)"),
    "relay-uncached": ("#1baf7a", "^", "Relay, uncached"),
    "relay-cached": ("#eda100", "D", "Relay, cached (warm)"),
}
STATUS_COLORS = {"2xx": "#1baf7a", "429": "#eda100", "5xx": "#e34948", "conn": "#6250d6"}

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "axes.edgecolor": GRID,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "axes.titlesize": 11,
        "axes.titleweight": "semibold",
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "xtick.color": INK2,
        "ytick.color": INK2,
        "font.size": 9,
        "legend.frameon": False,
        "lines.linewidth": 2,
        "lines.markersize": 6,
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


def load_summary(matrix: Path) -> list[dict]:
    with (matrix / "summary.csv").open() as fh:
        rows = list(csv.DictReader(fh))
    for row in rows:
        for key, value in row.items():
            with contextlib.suppress(TypeError, ValueError):
                row[key] = float(value)
    return rows


def series_plot(ax, rows, x: str, y: str, modes, *, label_last: bool = True) -> None:
    for mode in modes:
        pts = sorted((r for r in rows if r["mode"] == mode), key=lambda r: r[x])
        if not pts:
            continue
        color, marker, label = SERIES[mode]
        xs = [p[x] for p in pts]
        ys = [p[f"{y}_median"] for p in pts]
        lo = [p[f"{y}_median"] - p[f"{y}_min"] for p in pts]
        hi = [p[f"{y}_max"] - p[f"{y}_median"] for p in pts]
        ax.errorbar(
            xs, ys, yerr=[lo, hi], color=color, marker=marker, label=label, capsize=2, elinewidth=1
        )
        if label_last:
            ax.annotate(
                f"{ys[-1]:,.0f}" if ys[-1] >= 10 else f"{ys[-1]:.2f}",
                (xs[-1], ys[-1]),
                textcoords="offset points",
                xytext=(6, 0),
                va="center",
                color=INK2,
                fontsize=8,
            )


def save(fig, out: Path, name: str) -> None:
    fig.tight_layout()
    fig.savefig(out / name, dpi=160)
    plt.close(fig)


def chart_concurrency(rows, out: Path) -> None:
    data = [r for r in rows if r["experiment"] == "concurrency"]
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.8))
    series_plot(a, data, "vus", "rps", SERIES)
    a.set(
        title="Throughput vs concurrency (closed loop)",
        xlabel="virtual users",
        ylabel="requests / s",
    )
    series_plot(b, data, "vus", "p99_ms", SERIES, label_last=False)
    b.set(
        title="p99 latency vs concurrency",
        xlabel="virtual users",
        ylabel="p99 latency (ms, log)",
        yscale="log",
    )
    a.legend(loc="upper left", fontsize=8)
    save(fig, out, "concurrency.png")


def chart_latency(rows, out: Path) -> None:
    data = [r for r in rows if r["experiment"] == "latency"]
    direct = {r["delay_ms"]: r for r in data if r["mode"] == "direct"}
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.8))
    series_plot(a, data, "delay_ms", "rps", ("direct", "nginx", "relay-uncached"))
    a.set(
        title="Throughput vs upstream latency (50 VUs)",
        xlabel="upstream processing time (ms)",
        ylabel="requests / s",
    )
    a.legend(fontsize=8)
    for mode in ("nginx", "relay-uncached"):
        pts = sorted((r for r in data if r["mode"] == mode), key=lambda r: r["delay_ms"])
        xs = [p["delay_ms"] for p in pts]
        overhead = [
            100
            * (p["p50_ms_median"] - direct[p["delay_ms"]]["p50_ms_median"])
            / direct[p["delay_ms"]]["p50_ms_median"]
            for p in pts
        ]
        color, marker, label = SERIES[mode]
        b.plot(xs, overhead, color=color, marker=marker, label=label)
        b.annotate(
            f"{overhead[-1]:.0f}%",
            (xs[-1], overhead[-1]),
            textcoords="offset points",
            xytext=(6, 0),
            va="center",
            color=INK2,
            fontsize=8,
        )
    b.set(
        title="Added p50 latency vs direct (% of direct p50)",
        xlabel="upstream processing time (ms)",
        ylabel="overhead (%)",
    )
    b.legend(fontsize=8)
    save(fig, out, "upstream_latency.png")


def chart_openloop(rows, out: Path) -> None:
    data = [r for r in rows if r["experiment"] == "openloop"]
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.8))
    for mode in ("nginx", "relay-uncached"):
        pts = sorted((r for r in data if r["mode"] == mode), key=lambda r: r["rate"])
        color, marker, label = SERIES[mode]
        a.plot(
            [p["rate"] for p in pts],
            [p["p99_ms_median"] for p in pts],
            color=color,
            marker=marker,
            label=f"{label} p99",
        )
        a.plot(
            [p["rate"] for p in pts],
            [p["p50_ms_median"] for p in pts],
            color=color,
            marker=marker,
            linestyle="--",
            alpha=0.7,
            label=f"{label} p50",
        )
        b.plot(
            [p["rate"] for p in pts],
            [p["rps_median"] for p in pts],
            color=color,
            marker=marker,
            label=label,
        )
    rates = sorted({r["rate"] for r in data})
    b.plot(rates, rates, color=INK2, linestyle=":", linewidth=1, label="offered = achieved")
    a.set(
        title="Latency vs offered load (open loop)",
        xlabel="offered load (req/s)",
        ylabel="latency (ms, log)",
        yscale="log",
    )
    b.set(
        title="Achieved vs offered throughput",
        xlabel="offered load (req/s)",
        ylabel="achieved (req/s)",
    )
    a.legend(fontsize=7)
    b.legend(fontsize=8)
    save(fig, out, "open_loop.png")


def chart_scaling(rows, out: Path) -> None:
    data = sorted((r for r in rows if r["experiment"] == "scaling"), key=lambda r: r["instances"])
    if not data:
        return
    fig, ax = plt.subplots(figsize=(5, 3.6))
    xs = [r["instances"] for r in data]
    ys = [r["rps_median"] for r in data]
    ax.errorbar(
        xs,
        ys,
        yerr=[
            [r["rps_median"] - r["rps_min"] for r in data],
            [r["rps_max"] - r["rps_median"] for r in data],
        ],
        color=SERIES["relay-uncached"][0],
        marker="^",
        capsize=2,
        label="measured",
    )
    ax.plot(
        xs,
        [ys[0] * x for x in xs],
        color=INK2,
        linestyle=":",
        linewidth=1,
        label="linear from 1 instance",
    )
    for x, y in zip(xs, ys, strict=True):
        ax.annotate(
            f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(6, -10), color=INK2, fontsize=8
        )
    ax.set(
        title="Horizontal scaling (100 VUs, uncached)",
        xlabel="Relay instances",
        ylabel="requests / s",
        xticks=xs,
    )
    ax.legend(fontsize=8)
    save(fig, out, "scaling.png")


def bar_chart(
    rows, out: Path, experiment: str, metric: str, title: str, ylabel: str, name: str, fmt: str
) -> None:
    data = [r for r in rows if r["experiment"] == experiment]
    if not data:
        return
    fig, ax = plt.subplots(figsize=(7, 3.4))
    labels = [r["mode"].removeprefix("relay-") for r in data]
    values = [r[f"{metric}_median"] for r in data]
    bars = ax.bar(
        labels, values, color=SERIES["relay-uncached"][0], width=0.6, edgecolor=SURFACE, linewidth=2
    )
    for bar, v in zip(bars, values, strict=True):
        ax.annotate(
            fmt.format(v),
            (bar.get_x() + bar.get_width() / 2, v),
            textcoords="offset points",
            xytext=(0, 3),
            ha="center",
            color=INK,
            fontsize=8,
        )
    ax.set(title=title, ylabel=ylabel)
    ax.tick_params(axis="x", labelrotation=15)
    save(fig, out, name)


def chart_chaos(chaos: Path, out: Path) -> None:
    for timeline_file in sorted(chaos.glob("*-rep1.timeline.csv")):
        scenario = timeline_file.name.removesuffix("-rep1.timeline.csv")
        meta = json.loads((chaos / f"{scenario}-rep1.json").read_text())
        with timeline_file.open() as fh:
            rows = list(csv.DictReader(fh))
        t = [int(r["t"]) for r in rows]
        n_rows = len(rows)
        groups: dict[str, list[float]] = defaultdict(lambda n=n_rows: [0.0] * n)
        for i, r in enumerate(rows):
            for key, value in r.items():
                if not key.startswith("status_") or not value:
                    continue
                code = key.removeprefix("status_")
                bucket = (
                    "2xx"
                    if code.startswith("2")
                    else "429"
                    if code == "429"
                    else "conn"
                    if code == "0"
                    else "5xx"
                )
                groups[bucket][i] += float(value)
        fig, (a, b) = plt.subplots(2, 1, figsize=(9, 4.6), sharex=True, height_ratios=[2, 1])
        bottom = [0.0] * len(rows)
        for bucket in ("2xx", "429", "5xx", "conn"):
            if bucket in groups:
                label = {
                    "2xx": "success",
                    "429": "429 rate limited",
                    "5xx": "5xx",
                    "conn": "connection failed",
                }[bucket]
                a.bar(
                    t,
                    groups[bucket],
                    bottom=bottom,
                    color=STATUS_COLORS[bucket],
                    width=0.9,
                    label=label,
                    edgecolor=SURFACE,
                    linewidth=0.5,
                )
                bottom = [x + y for x, y in zip(bottom, groups[bucket], strict=True)]
        b.plot(
            t, [float(r["p99_ms"]) for r in rows], color=INK2, linewidth=1.5, label="p99 (bucketed)"
        )
        b.set_yscale("log")
        for ax in (a, b):
            ax.axvspan(
                meta["events_s"]["fault"],
                meta["events_s"]["recover"],
                color="#e34948",
                alpha=0.08,
                linewidth=0,
            )
        a.set(title=f"{scenario}: {meta['description']}", ylabel="requests / s")
        a.legend(loc="lower left", fontsize=7, ncols=4)
        b.set(xlabel="seconds (shaded = fault injected)", ylabel="p99 ms (log)")
        save(fig, out, f"chaos_{scenario}.png")


def md_table(rows: list[dict], cols: list[tuple[str, str, str]]) -> str:
    head = "| " + " | ".join(c[0] for c in cols) + " |\n|" + "---|" * len(cols) + "\n"
    body = ""
    for r in rows:
        body += (
            "| "
            + " | ".join(
                fmt.format(r[key]) if isinstance(r[key], float) else str(r[key])
                for _, key, fmt in cols
            )
            + " |\n"
        )
    return head + body


def rng(metric: str, fmt: str) -> tuple[str, str, str]:
    return (metric, metric, fmt)


def write_summary(rows, matrix: Path, chaos: Path | None) -> None:
    def pick(exp: str) -> list[dict]:
        out = []
        for r in rows:
            if r["experiment"] != exp:
                continue
            out.append(
                {
                    **r,
                    "rps": f"{r['rps_median']:,.0f} ({r['rps_min']:,.0f}-{r['rps_max']:,.0f})",
                    "p50": f"{r['p50_ms_median']:.2f}",
                    "p99": f"{r['p99_ms_median']:.2f}",
                    "err": f"{r['error_pct_median']:.3f}",
                    "up": f"{r['upstream_per_request_median']:.4f}",
                    "cpu": f"{r['cpu_pct_median']:.0f}",
                    "dropped": f"{r['dropped_median']:.0f}",
                }
            )
        return out

    cols_basic = [
        ("Cell", "cell", "{}"),
        ("Req/s median (min-max)", "rps", "{}"),
        ("p50 ms", "p50", "{}"),
        ("p99 ms", "p99", "{}"),
        ("Errors %", "err", "{}"),
        ("CPU % of target", "cpu", "{}"),
    ]
    env = (matrix / "environment.txt").read_text()
    lines = [
        f"# Benchmark matrix: {matrix.name}",
        "",
        "Generated by `scripts/plot_benchmarks.py` from `summary.csv` (median of repetitions, min-max range).",
        "Raw k6 JSON for every run is in `raw/`. Local laptop numbers, not capacity claims.",
        "",
        "## Environment",
        "",
        "```text",
        env.strip(),
        "```",
        "",
    ]
    sections = [
        (
            "concurrency",
            "Concurrency sweep (closed loop, 20 s per run)",
            cols_basic,
            "concurrency.png",
        ),
        ("latency", "Upstream-latency sweep (50 VUs)", cols_basic, "upstream_latency.png"),
        (
            "openloop",
            "Open-loop arrival-rate sweep",
            [*cols_basic[:5], ("Dropped iterations", "dropped", "{}")],
            "open_loop.png",
        ),
        ("scaling", "Horizontal scaling (60 VUs)", cols_basic, "scaling.png"),
        (
            "ablation",
            "Ablation: where the per-request cost goes (50 VUs)",
            cols_basic,
            "ablation.png",
        ),
        (
            "stampede",
            "Cache stampede: 100 VUs on one hot key that rotates every 200 ms, 50 ms upstream",
            [*cols_basic[:4], ("Upstream requests per client request", "up", "{}")],
            "stampede.png",
        ),
        (
            "brownout",
            "Brownout: backend-b fails every 2nd request with 503 (20 VUs)",
            [
                *cols_basic[:3],
                ("Errors %", "err", "{}"),
                ("Upstream requests per client request", "up", "{}"),
            ],
            "brownout.png",
        ),
    ]
    for exp, title, cols, chart in sections:
        data = pick(exp)
        if not data:
            continue
        lines += [f"## {title}", "", f"![{exp}](charts/{chart})", "", md_table(data, cols)]
    if chaos is not None:
        lines += ["## Chaos timelines (300 req/s open loop; fault from t=20 s to t=45 s)", ""]
        with (chaos / "chaos_summary.csv").open() as fh:
            crow = list(csv.DictReader(fh))
        by = defaultdict(list)
        for r in crow:
            by[r["scenario"]].append(r)
        lines += [
            "| Scenario | Reps | Fault-window errors % (per rep) | Max p99 in fault window ms (per rep) | Errors after recovery (per rep) |",
            "|---|---|---|---|---|",
        ]
        for scen, rs in by.items():
            lines.append(
                f"| {scen} | {len(rs)} | {', '.join(r['fault_window_error_pct'] for r in rs)} | "
                f"{', '.join(r['fault_window_max_p99_ms'] for r in rs)} | {', '.join(r['after_recovery_errors'] for r in rs)} |"
            )
        lines.append("")
        for png in sorted((matrix / "charts").glob("chaos_*.png")):
            lines += [f"![{png.stem}](charts/{png.name})", ""]
    (matrix / "SUMMARY.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    matrix = Path(sys.argv[1])
    chaos = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    out = matrix / "charts"
    out.mkdir(exist_ok=True)
    rows = load_summary(matrix)
    chart_concurrency(rows, out)
    chart_latency(rows, out)
    chart_openloop(rows, out)
    chart_scaling(rows, out)
    bar_chart(
        rows,
        out,
        "ablation",
        "rps",
        "Ablation: throughput per variant (50 VUs)",
        "requests / s",
        "ablation.png",
        "{:,.0f}",
    )
    bar_chart(
        rows,
        out,
        "stampede",
        "upstream_per_request",
        "Upstream requests per client request",
        "ratio",
        "stampede.png",
        "{:.4f}",
    )
    bar_chart(
        rows,
        out,
        "brownout",
        "upstream_per_request",
        "Upstream requests per client request (brownout)",
        "ratio",
        "brownout.png",
        "{:.3f}",
    )
    if chaos is not None:
        chart_chaos(chaos, out)
    write_summary(rows, matrix, chaos)
    print(f"wrote {matrix / 'SUMMARY.md'} and charts/")


if __name__ == "__main__":
    main()
