#!/usr/bin/env python3
"""
Benchmark write performance with and without metadata passthrough.

Compares three write paths:
  1. No metadata (baseline)
  2. Opaque Rust handle (fast path — avoids all Python dict overhead)
  3. Python dict (slow path — JSON serialize → dict → PyO3 boundary)

Usage:
    python scripts/benchmark_metadata_write.py [--rows N]

Default: 1000 rows.  Keep N small for very wide files (100k+ columns)
to avoid excessive memory use and long run times.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import polars_readstat as prs

FILE = Path(__file__).resolve().parents[1] / (
    "crates/polars_readstat_rs/tests/spss/data/too_big/export_output_anon_v1.sav"
)
OUTPUT = Path("/tmp/spss_metadata_bench_out.sav")


def _ms(seconds: float) -> str:
    return f"{seconds * 1000:8.1f} ms"


def timed(label: str, fn):
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    print(f"  {label:<60} {_ms(elapsed)}", flush=True)
    return result, elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Rows to read (default 1000 — keep small for very wide files)",
    )
    args = parser.parse_args()
    n_rows = args.rows

    print(f"File : {FILE}")
    print(f"Rows : {n_rows}")
    print()

    # ------------------------------------------------------------------ #
    # Step 1 — Read                                                        #
    # ------------------------------------------------------------------ #
    print("=== Step 1: Read ===")
    reader = prs.ScanReadstat(str(FILE))
    (df, t_read) = timed(
        f"scan_readstat + collect({n_rows} rows)",
        lambda: reader.df.head(n_rows).collect(),
    )
    print(f"  DataFrame shape: {df.shape}  ({df.width} cols × {df.height} rows)")

    # ------------------------------------------------------------------ #
    # Step 2 — Metadata access                                            #
    # ------------------------------------------------------------------ #
    print("\n=== Step 2: Metadata access ===")

    (handle, t_handle) = timed(
        "reader.metadata_handle  (opaque Rust handle, no JSON)",
        lambda: reader.metadata_handle,
    )

    (metadata, t_meta_dict) = timed(
        "reader.metadata  (Rust JSON serialize → Python json.loads, cached)",
        lambda: reader.metadata,
    )
    variables_raw = metadata.get("variables") or []
    print(f"  Variables in metadata : {len(variables_raw)}")

    # ------------------------------------------------------------------ #
    # Step 3 — Write paths                                                 #
    # ------------------------------------------------------------------ #
    print("\n=== Step 3: Write paths ===")

    print("  --- baseline: no metadata ---", flush=True)
    (_, t_write_none) = timed(
        "write_spss(df)  [no metadata]",
        lambda: prs.write_spss(df, str(OUTPUT)),
    )

    print("  --- opaque handle (fast path) ---", flush=True)
    (_, t_write_handle) = timed(
        "write_readstat(df, metadata=handle)  [fast path]",
        lambda: prs.write_readstat(df, str(OUTPUT), metadata=handle),
    )

    print("  --- Python dict (slow path) ---", flush=True)
    (_, t_write_dict) = timed(
        "write_readstat(df, metadata=dict)  [dict path]",
        lambda: prs.write_readstat(df, str(OUTPUT), metadata=metadata),
    )

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    print("\n=== Summary ===")
    rows = [
        ("read",                           t_read),
        ("metadata_handle (Rust handle)",  t_handle),
        ("metadata dict (JSON + parse)",   t_meta_dict),
        ("write — baseline (no meta)",     t_write_none),
        ("write — handle (fast path)",     t_write_handle),
        ("write — dict  (slow path)",      t_write_dict),
    ]
    for label, t in rows:
        print(f"  {label:<42} {_ms(t)}")

    print()
    overhead_handle = t_write_handle - t_write_none
    overhead_dict   = t_write_dict   - t_write_none
    print(f"  Handle overhead vs baseline : {_ms(overhead_handle)}")
    print(f"  Dict   overhead vs baseline : {_ms(overhead_dict)}")
    if overhead_dict > 1e-3:
        print(f"  Dict   overhead vs handle   : {_ms(t_write_dict - t_write_handle)}")


if __name__ == "__main__":
    main()
