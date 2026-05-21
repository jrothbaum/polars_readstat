"""
Benchmark reading the wide SPSS file (3853 rows x 108556 cols) using
subsets of columns, tracking wall time and peak RSS for each engine.
"""
import gc
import os
import sys
import time
import resource
from pathlib import Path

import pyreadstat as prs
import polars as pl
from polars_readstat import scan_readstat

FILE = Path(
    "/home/jrothbaum/Coding/claude_code/polars_readstat"
    "/crates/polars_readstat_rs/tests/spss/data/too_big/export_output_anon_v1.sav"
)


def rss_mb() -> float:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024  # kB → MB
    return float("nan")


def bench(label: str, fn):
    gc.collect()
    baseline = rss_mb()
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    peak = rss_mb()
    delta = peak - baseline
    rows = len(result) if hasattr(result, "__len__") else "?"
    if hasattr(result, "shape"):
        shape = result.shape
    else:
        shape = (rows, "?")
    print(f"  {label:<22}  {elapsed:6.2f}s  baseline={baseline:7.1f}MB  peak={peak:7.1f}MB  +{delta:6.1f}MB  shape={shape}")
    del result
    gc.collect()
    return elapsed, delta


def get_columns(n: int) -> list[str]:
    """Return first n column names from the file metadata."""
    _, meta = prs.read_sav(str(FILE), metadataonly=True)
    return meta.column_names[:n]


def run(n_cols: int, all_cols: list[str]):
    cols = all_cols[:n_cols]
    print(f"\n=== {n_cols} columns (all {3853} rows) ===")

    bench(
        "polars_readstat",
        lambda: scan_readstat(str(FILE)).select(cols).collect(),
    )
    bench(
        "pyreadstat",
        lambda: prs.read_sav(str(FILE), usecols=cols)[0],
    )


if __name__ == "__main__":
    print("Reading metadata...")
    all_cols = get_columns(600)
    print(f"Got {len(all_cols)} column names.  Starting benchmarks.\n")

    for n in [100, 250, 500]:
        run(n, all_cols)
