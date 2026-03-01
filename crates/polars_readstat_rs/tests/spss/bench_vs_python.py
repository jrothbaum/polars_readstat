# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Benchmark Rust SPSS reader vs polars_readstat (ReadStat reference).

Run with:
  uv run tests/spss/bench_vs_python.py --file tests/spss/data/ess_data.sav --rows 100000 --repeat 3

For apples-to-apples with ReadStat defaults (no value-label mapping in Rust):
  uv run tests/spss/bench_vs_python.py --file tests/spss/data/ess_data.sav --rows 100000 --repeat 3 --readstat-default
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import time
from pathlib import Path

from polars_readstat import scan_readstat

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True, help="Path to .sav file")
    parser.add_argument("--rows", type=int, default=1000, help="Rows to read (ignored when --full)")
    parser.add_argument("--full", action="store_true", help="Read full file (default)")
    parser.add_argument("--repeat", type=int, default=2, help="Repetitions")
    parser.add_argument("--readstat-default", action="store_true", help="Match ReadStat by disabling value-label mapping in Rust")
    return parser.parse_args()


def bench_python(path: Path, rows: int, full: bool) -> float:
    start = time.perf_counter()
    lf = scan_readstat(str(path))
    if full:
        df = lf.collect()
    else:
        df = lf.head(rows).collect()
    del df
    return time.perf_counter() - start


def bench_rust(binary_path: Path, path: Path, rows: int, full: bool, readstat_default: bool) -> float:
    cmd = [
        str(binary_path),
        str(path),
        "full" if full else str(rows),
    ]
    if readstat_default:
        cmd.append("--no-labels")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Rust reader failed")
    line = result.stdout.strip().splitlines()[-1]
    if not line.startswith("elapsed_ms="):
        raise RuntimeError(f"Unexpected output: {line}")
    ms = float(line.split("=", 1)[1])
    return ms / 1000.0


def summarize(times: list[float]) -> dict[str, float]:
    return {
        "min": min(times),
        "median": statistics.median(times),
        "max": max(times),
        "mean": statistics.mean(times),
    }


def fmt_seconds(stats: dict[str, float]) -> str:
    return (
        f"min={stats['min']:.3f}s "
        f"median={stats['median']:.3f}s "
        f"max={stats['max']:.3f}s "
        f"mean={stats['mean']:.3f}s"
    )


def main() -> None:
    args = parse_args()
    target = args.file.resolve()
    binary = PROJECT_ROOT / "target" / "release" / "examples" / "spss_bench_read"

    build = subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--example",
            "spss_bench_read",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        raise RuntimeError(build.stderr.strip() or "Failed to build Rust benchmark")

    py_times = []
    rust_times = []
    for _ in range(args.repeat):
        py_times.append(bench_python(target, args.rows, args.full))
        rust_times.append(bench_rust(binary, target, args.rows, args.full, args.readstat_default))

    print(f"File: {target}")
    print("Rows:", "full" if args.full else args.rows)
    print(f"Python (polars_readstat): {fmt_seconds(summarize(py_times))}")
    print(f"Rust   (spss reader):     {fmt_seconds(summarize(rust_times))}")


if __name__ == "__main__":
    main()
