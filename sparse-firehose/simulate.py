#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
import statistics
from pathlib import Path

DEFAULT_SELECTIVITIES = (0.001, 0.005, 0.01, 0.05, 0.10)
DEFAULT_FRAME_SIZES = (1, 2, 4, 5, 10, 20, 25, 50, 100)

def geometric_failures(rng: random.Random, p: float) -> int:
    if p <= 0:
        return 10**18
    if p >= 1:
        return 0
    u = rng.random()
    return int(math.log1p(-u) / math.log1p(-p))

def bernoulli_positions(n: int, p: float, rng: random.Random) -> list[int]:
    out = []
    i = geometric_failures(rng, p)
    while i < n:
        out.append(i)
        i += 1 + geometric_failures(rng, p)
    return out

def geometric_length(rng: random.Random, mean: float) -> int:
    return 1 + geometric_failures(rng, 1.0 / mean)

def bursty_positions(n: int, rng: random.Random,
                     mean_gap=400.0, mean_burst=5.0,
                     p_burst=0.80, p_quiet=0.0001) -> list[int]:
    out = []
    i = 0
    while i < n:
        gap_end = min(n, i + geometric_length(rng, mean_gap))
        for x in bernoulli_positions(gap_end - i, p_quiet, rng):
            out.append(i + x)
        i = gap_end
        if i >= n:
            break
        burst_end = min(n, i + geometric_length(rng, mean_burst))
        for x in bernoulli_positions(burst_end - i, p_burst, rng):
            out.append(i + x)
        i = burst_end
    out.sort()
    return out

def io_metrics(positions, n, bundle_size, frame_size):
    touched_bundles = {p // bundle_size for p in positions}
    touched_frames = {p // frame_size for p in positions}
    current_blocks = len(touched_bundles) * bundle_size
    sparse_blocks = len(touched_frames) * frame_size
    return {
        "selectivity": len(positions) / n,
        "current_read_fraction": current_blocks / n,
        "sparse_read_fraction": sparse_blocks / n,
        "io_reduction_x": current_blocks / sparse_blocks if sparse_blocks else float("nan"),
    }

def aggregate(samples):
    keys = ("selectivity", "current_read_fraction", "sparse_read_fraction", "io_reduction_x")
    out = {k: statistics.mean(s[k] for s in samples) for k in keys}
    out["io_reduction_sd"] = statistics.stdev(s["io_reduction_x"] for s in samples)
    return out

def independent_runs(n, runs, bundle_size, frame_size, selectivities, seed):
    rng = random.Random(seed)
    rows = []
    for p in selectivities:
        samples = []
        for _ in range(runs):
            positions = bernoulli_positions(n, p, rng)
            samples.append(io_metrics(positions, n, bundle_size, frame_size))
        rows.append({"target_selectivity": p, **aggregate(samples)})
    return rows

def bursty_runs(n, runs, bundle_size, frame_size, seed):
    rng = random.Random(seed)
    samples = []
    for _ in range(runs):
        positions = bursty_positions(n, rng)
        samples.append(io_metrics(positions, n, bundle_size, frame_size))
    return [{
        "mean_gap": 400.0,
        "mean_burst": 5.0,
        "p_burst": 0.80,
        "p_quiet": 0.0001,
        **aggregate(samples),
    }]

def frame_sensitivity_runs(n, runs, bundle_size, frame_sizes, p, seed):
    rows = []
    for offset, frame_size in enumerate(frame_sizes):
        rng = random.Random(seed + offset)
        samples = []
        for _ in range(runs):
            positions = bernoulli_positions(n, p, rng)
            samples.append(io_metrics(positions, n, bundle_size, frame_size))
        rows.append({"frame_size": frame_size, "target_selectivity": p, **aggregate(samples)})
    return rows

def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--blocks", type=int, default=500_000)
    p.add_argument("--runs", type=int, default=30)
    p.add_argument("--bundle-size", type=int, default=100)
    p.add_argument("--frame-size", type=int, default=4)
    p.add_argument("--out", type=Path, default=Path("results"))
    args = p.parse_args()

    independent = independent_runs(
        args.blocks, args.runs, args.bundle_size, args.frame_size,
        DEFAULT_SELECTIVITIES, 42
    )
    bursty = bursty_runs(
        args.blocks, args.runs, args.bundle_size, args.frame_size, 123
    )
    sensitivity = frame_sensitivity_runs(
        args.blocks, args.runs, args.bundle_size,
        DEFAULT_FRAME_SIZES, 0.01, 2026
    )

    write_csv(args.out / "independent.csv", independent)
    write_csv(args.out / "bursty.csv", bursty)
    write_csv(args.out / "frame_sensitivity_1pct.csv", sensitivity)

    for row in independent:
        print(
            f"{row['target_selectivity']:.3%} "
            f"current={row['current_read_fraction']:.3%} "
            f"sparse={row['sparse_read_fraction']:.3%} "
            f"reduction={row['io_reduction_x']:.2f}x"
        )
    b = bursty[0]
    print(
        f"bursty {b['selectivity']:.3%} "
        f"current={b['current_read_fraction']:.3%} "
        f"sparse={b['sparse_read_fraction']:.3%} "
        f"reduction={b['io_reduction_x']:.2f}x"
    )

if __name__ == "__main__":
    main()
