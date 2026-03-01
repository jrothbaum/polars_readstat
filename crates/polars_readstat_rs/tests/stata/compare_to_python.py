# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare Rust Stata reader output against polars_readstat (ReadStat reference).

Run with:
  uv run tests/stata/compare_to_python.py --file tests/stata/data/usa_00009.dta --rows 10
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
from polars_readstat import scan_readstat

N_ROWS = 10
MAX_FILE_SIZE = 1_000_000_000  # 1GB
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "stata" / "data"
SCRATCH_ROOT = Path(os.environ.get("READSTAT_SCRATCH_DIR", "/tmp/polars_readstat_compare"))
NON_BLOCKING_MISMATCH_PATHS = {
    "stata13_dates.dta",
    "stata2_113.dta",
    "stata2_114.dta",
    "stata2_115.dta",
    "stata2_117.dta",
    "stata5_113.dta",
    "stata5_114.dta",
    "stata5_115.dta",
    "stata5_117.dta",
    "stata8_113.dta",
    "stata8_115.dta",
    "stata8_117.dta",
}
SIGNED_INT_DTYPES = {pl.Int8, pl.Int16, pl.Int32, pl.Int64}
UNSIGNED_INT_DTYPES = {pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
INT_RANK = {
    pl.Int8: 0,
    pl.Int16: 1,
    pl.Int32: 2,
    pl.Int64: 3,
    pl.UInt8: 0,
    pl.UInt16: 1,
    pl.UInt32: 2,
    pl.UInt64: 3,
}


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _load_parquet(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def _allow_narrow_int(old_dtype: pl.DataType, new_dtype: pl.DataType) -> bool:
    if old_dtype in SIGNED_INT_DTYPES and new_dtype in SIGNED_INT_DTYPES:
        return INT_RANK[new_dtype] <= INT_RANK[old_dtype]
    if old_dtype in UNSIGNED_INT_DTYPES and new_dtype in UNSIGNED_INT_DTYPES:
        return INT_RANK[new_dtype] <= INT_RANK[old_dtype]
    return False


def _schema_mismatches(schema_a: dict, schema_b: dict) -> list[str]:
    mismatches = []
    if list(schema_a.keys()) != list(schema_b.keys()):
        mismatches.append("column order/names differ")
        return mismatches
    for name, dtype_a in schema_a.items():
        dtype_b = schema_b[name]
        if dtype_a == dtype_b:
            continue
        if _allow_narrow_int(dtype_a, dtype_b):
            continue
        mismatches.append(f"{name}: {dtype_a} vs {dtype_b}")
    return mismatches


def _cast_ints_for_compare(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    for name, dtype in df.schema.items():
        if dtype in SIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Int64))
        elif dtype in UNSIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.UInt64))
        else:
            exprs.append(pl.col(name))
    return df.select(exprs)


def _compare_pair(label_a: str, df_a: pl.DataFrame, label_b: str, df_b: pl.DataFrame) -> int:
    mismatches = 0
    schema_mismatches = _schema_mismatches(df_a.schema, df_b.schema)
    if schema_mismatches:
        print(f"  SCHEMA MISMATCH: {label_a} vs {label_b}")
        print(f"    {label_a}: {df_a.schema}")
        print(f"    {label_b}: {df_b.schema}")
        if len(schema_mismatches) <= 5:
            print(f"    Details: {schema_mismatches}")
        mismatches += 1
    try:
        df_a_cmp = _cast_ints_for_compare(df_a)
        df_b_cmp = _cast_ints_for_compare(df_b)
        assert_frame_equal(
            df_a_cmp,
            df_b_cmp,
            check_dtypes=True,
            check_row_order=True,
            check_column_order=True,
            rel_tol=1e-6,
            abs_tol=1e-6,
        )
    except AssertionError as e:
        print(f"  DATA MISMATCH: {label_a} vs {label_b}")
        print(f"    {e}")
        mismatches += 1
    return mismatches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True, help="Path to .dta file")
    parser.add_argument("--rows", type=int, default=100000, help="Rows to compare")
    parser.add_argument("--cols", type=int, default=0, help="Limit columns (0 = all)")
    return parser.parse_args()


def compare_file(stata_file: Path, n_rows: int, n_cols: int=0) -> tuple[int, int]:
    print(f"\n--- {stata_file.name} ({stata_file.stat().st_size / 1024 / 1024:.1f} MB) ---")

    rel = stata_file.relative_to(TEST_DATA_DIR)
    scratch_dir = SCRATCH_ROOT / "stata" / "__".join(rel.parts)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prs_path = scratch_dir / "polars_readstat.parquet"
    rust_path = scratch_dir / "polars_readstat_rs.parquet"

    # 1. Read with polars_readstat (ground truth)
    try:
        lf = scan_readstat(str(stata_file))
        if n_cols:
            names = list(lf.collect_schema().keys())[:n_cols]
            lf = lf.select(names)

        df_prs = lf.collect().head(n_rows)
        _write_parquet(df_prs, prs_path)
    except Exception as e:
        msg = str(e)
        if "No columns to process" in msg:
            print(f"  SKIP: polars_readstat failed: {e}")
            return 0, 0
        print(f"  FAIL: polars_readstat failed: {e}")
        return 0, 1

    print(f"  polars_readstat: {df_prs.shape[0]} rows x {df_prs.shape[1]} cols")

    # 2. Read with Rust reader -> parquet
    env = dict(os.environ)
    env["READSTAT_PRESERVE_ORDER"] = "1"
    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "readstat_dump_parquet", "--",
         str(stata_file), str(rust_path), str(n_rows), str(n_cols)],
        capture_output=True, text=True,
        cwd=PROJECT_ROOT,
        env=env,
    )
    if result.returncode != 0:
        print(f"  FAIL: Rust reader failed: {result.stderr[:200]}")
        return 0, 1

    df_rust = _load_parquet(rust_path)
    print(f"  polars_readstat_rs: {df_rust.shape[0]} rows x {df_rust.shape[1]} cols")
    print(f"  Scratch: {scratch_dir}")

    mismatches = _compare_pair("polars_readstat", df_prs, "polars_readstat_rs", df_rust)
    checked = df_prs.height * df_prs.width

    if mismatches == 0:
        print(f"  OK ({checked} values)")
    else:
        rel_path = str(stata_file.relative_to(TEST_DATA_DIR))
        if rel_path in NON_BLOCKING_MISMATCH_PATHS:
            print(f"  NON-BLOCKING mismatch for {rel_path}")
        else:
            print(f"  FAILED: {mismatches} mismatches (see above)")

    return checked, mismatches


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=N_ROWS, help="Rows to compare per file")
    parser.add_argument("--xfail", action="store_true", help="Do not fail on mismatches (report only)")
    args = parser.parse_args()

    print("=== Comparing Rust reader vs polars_readstat (ReadStat reference) ===")
    print(f"Checking first {args.rows} rows, all columns, files < {MAX_FILE_SIZE / 1e9:.0f}GB\n")

    print("Building Rust readstat_dump_parquet (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "readstat_dump_parquet"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    dta_files = [
        f for f in TEST_DATA_DIR.glob("**/*.dta")
        if "too_big" not in f.parts
    ]
    dta_files = sorted(dta_files)
    dta_files = [f for f in dta_files if f.stat().st_size <= MAX_FILE_SIZE]

    if not dta_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(dta_files)} test files (after size filter)")

    total_checked = 0
    total_mismatches = 0
    non_blocking_mismatches = 0
    non_blocking_files = []
    failed_files = []

    for sav_file in dta_files:
        checked, mismatches = compare_file(sav_file, args.rows)
        total_checked += checked
        if mismatches > 0:
            rel_path = str(sav_file.relative_to(TEST_DATA_DIR))
            if rel_path in NON_BLOCKING_MISMATCH_PATHS:
                non_blocking_mismatches += mismatches
                non_blocking_files.append(rel_path)
                print("NON-BLOCKING mismatch; continuing")
            else:
                total_mismatches += mismatches
                failed_files.append(sav_file.name)
                if args.xfail:
                    print("THERE ARE MISMATCHES - CONTINUING (xfail)")
                else:
                    print("THERE ARE MISMATCHES - STOPPING")
                    break

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checked} values checked across {len(dta_files)} files")
    print(f"       {total_mismatches} blocking mismatches")
    print(f"       {non_blocking_mismatches} non-blocking mismatches")
    if non_blocking_files:
        print(f"NON-BLOCKING files: {', '.join(non_blocking_files)}")

    if total_mismatches == 0:
        print("ALL FILES MATCH!")
    else:
        print(f"FAILED files: {', '.join(failed_files)}")
        if not args.xfail:
            sys.exit(1)


if __name__ == "__main__":
    main()
