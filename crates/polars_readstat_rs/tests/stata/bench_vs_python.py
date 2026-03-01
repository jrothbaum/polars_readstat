# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Benchmark Rust Stata reader vs polars_readstat (ReadStat reference).

Run with:
  uv run tests/stata/bench_vs_python.py --file tests/stata/data/usa_00009.dta --rows 1000 --repeat 3
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import time
from pathlib import Path

from polars_readstat import scan_readstat

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True, help="Path to .dta file")
    parser.add_argument("--rows", type=int, default=1000, help="Rows to read (ignored when --full)")
    parser.add_argument("--full", action="store_true", help="Read full file (default)")
    parser.add_argument("--repeat", type=int, default=2, help="Repetitions")
    parser.add_argument("--columns", type=str, help="Comma-separated list of column names")
    parser.add_argument("--col-count", type=int, help="Read first N columns")
    parser.add_argument("--first-k", type=int, help="Read first K columns (uses schema)")
    parser.add_argument("--last-k", type=int, help="Read last K columns (uses schema)")
    return parser.parse_args()


def bench_python(path: Path, rows: int, full: bool, columns: list[str] | None, col_count: int | None) -> float:
    start = time.perf_counter()
    lf = scan_readstat(str(path))
    if columns:
        lf = lf.select(columns)
    elif col_count:
        names = list(lf.collect_schema().keys())[:col_count]
        lf = lf.select(names)
    if full:
        df = lf.collect()
    else:
        df = lf.head(rows).collect()
    del df
    return time.perf_counter() - start


def bench_rust(binary_path: Path, path: Path, rows: int, full: bool, columns: list[str] | None, col_count: int | None) -> float:
    result = subprocess.run(
        [
            str(binary_path),
            str(path),
            "full" if full else str(rows),
            *(["--columns", ",".join(columns)] if columns else []),
            *(["--col-count", str(col_count)] if col_count else []),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Rust reader failed")
    line = result.stdout.strip().splitlines()[-1]
    if not line.startswith("elapsed_ms="):
        raise RuntimeError(f"Unexpected output: {line}")
    ms = float(line.split("=", 1)[1])
    return ms / 1000.0


def main() -> None:
    args = parse_args()
    columns = [c.strip() for c in args.columns.split(",") if c.strip()] if args.columns else None
    if not args.full:
        args.full = True
    if args.first_k or args.last_k:
        lf_schema = scan_readstat(str(args.file)).collect_schema()
        names = list(lf_schema.keys())
        if args.first_k:
            columns = names[: args.first_k]
        else:
            columns = names[-args.last_k :]

    print("Building Rust stata_bench (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "stata_bench"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)

    bin_path = PROJECT_ROOT / "target" / "release" / "examples" / "stata_bench"

    py_times = []
    rs_times = []
    for _ in range(args.repeat):
        py_times.append(bench_python(args.file, args.rows, args.full, columns, args.col_count))
        rs_times.append(bench_rust(bin_path, args.file, args.rows, args.full, columns, args.col_count))

    def summarize(vals: list[float]) -> str:
        return f"mean={statistics.mean(vals):.3f}s median={statistics.median(vals):.3f}s"

    print("\nPython (polars_readstat):", summarize(py_times))
    print("Rust:", summarize(rs_times))


if __name__ == "__main__":
    main()
