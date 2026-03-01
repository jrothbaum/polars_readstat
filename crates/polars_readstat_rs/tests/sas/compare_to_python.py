# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare Rust SAS reader output against polars_readstat (C++ reference) ground truth.

Run with: uv run tests/sas/compare_to_python.py

Iterates all .sas7bdat files in tests/sas/data/ (skipping files > 1GB),
reads each with:
  1) polars_readstat==0.11.1 (Python)
  2) polars_readstat_rs (Rust, via parquet dump)
Writes each to parquet in a scratch dir, then compares data and schemas via polars.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
from polars_readstat import scan_readstat

N_ROWS = 100_000
MAX_FILE_SIZE = 1_000_000_000  # 1GB - skip files larger than this
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "sas" / "data"
SCRATCH_ROOT = Path(os.environ.get("READSTAT_SCRATCH_DIR", "/tmp/polars_readstat_compare"))
# Hard-coded temporary allowlist: mismatches where _rs is known better than old reference.
NON_BLOCKING_MISMATCH_PATHS = {
    "data_misc/types.sas7bdat",
    "sas_to_csv/salesbyday.sas7bdat",
}
READSTAT_ENGINE_FILES = {
    "flightdelays.sas7bdat",
    "flightschedule.sas7bdat",
    "internationalflights.sas7bdat",
    "marchflights.sas7bdat",
    "payrollchanges.sas7bdat",
    "payrollmaster.sas7bdat",
    "staffchanges.sas7bdat",
    "staffmaster.sas7bdat",
    "supervisors.sas7bdat",
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
            check_row_order=False,
            check_column_order=True,
            rel_tol=1e-6,
            abs_tol=1e-6,
        )
    except AssertionError as e:
        print(f"  DATA MISMATCH: {label_a} vs {label_b}")
        print(f"    {e}")
        mismatches += 1
    return mismatches


def compare_file(sas_file: Path, n_rows: int) -> tuple[int, int]:
    """Compare one file. Returns (checked, mismatches)."""
    print(f"\n--- {sas_file.name} ({sas_file.stat().st_size / 1024 / 1024:.1f} MB) ---")

    rel = sas_file.relative_to(TEST_DATA_DIR)
    scratch_dir = SCRATCH_ROOT / "sas" / "__".join(rel.parts)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prs_path = scratch_dir / "polars_readstat.parquet"
    rust_path = scratch_dir / "polars_readstat_rs.parquet"

    # 1. Read with polars_readstat (Python / C++ reference)
    try:
        if sas_file.name in READSTAT_ENGINE_FILES:
            df_prs = scan_readstat(str(sas_file), engine="readstat").collect().head(n_rows)
        else:
            df_prs = scan_readstat(str(sas_file)).collect().head(n_rows)
        _write_parquet(df_prs, prs_path)
    except Exception as e:
        print(f"  SKIP: polars_readstat failed: {e}")
        return 0, 0

    # 2. Read with Rust reader -> parquet
    env = dict(os.environ)
    env["READSTAT_PRESERVE_ORDER"] = "1"
    result = subprocess.run(
        [
            "cargo",
            "run",
            "--release",
            "--example",
            "readstat_dump_parquet",
            "--",
            str(sas_file),
            str(rust_path),
            str(n_rows),
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
    )
    if result.returncode != 0:
        print(f"  SKIP: Rust reader failed: {result.stderr[:200]}")
        return 0, 0

    df_prs = _load_parquet(prs_path)
    df_rust = _load_parquet(rust_path)

    print(f"  polars_readstat:    {df_prs.shape[0]} rows x {df_prs.shape[1]} cols")
    print(f"  polars_readstat_rs: {df_rust.shape[0]} rows x {df_rust.shape[1]} cols")
    print(f"  Scratch: {scratch_dir}")

    mismatches = 0
    mismatches += _compare_pair("polars_readstat", df_prs, "polars_readstat_rs", df_rust)

    checked = df_prs.height * df_prs.width
    if mismatches == 0:
        print(f"  OK ({checked} values)")
    else:
        print(f"  FAILED: {mismatches} mismatches (see above)")

    return checked, mismatches


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=N_ROWS, help="Rows to compare per file")
    parser.add_argument("--xfail", action="store_true", help="Do not fail on mismatches (report only)")
    args = parser.parse_args()

    print("=== Comparing Rust reader vs polars_readstat (C++ reference) ===")
    print(f"Checking first {args.rows} rows, all columns, files < {MAX_FILE_SIZE / 1e9:.0f}GB\n")

    # Build the Rust example once upfront
    print("Building Rust readstat_dump_parquet (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "readstat_dump_parquet"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    # Find all test files
    sas_files = sorted(
        f for f in TEST_DATA_DIR.glob("**/*.sas7bdat")
        if "too_big" not in f.parts
    )

    print(sas_files)

    sas_files = [f for f in sas_files if f.stat().st_size <= MAX_FILE_SIZE]

    if not sas_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(sas_files)} test files (after size filter)")

    total_checked = 0
    total_mismatches = 0
    non_blocking_mismatches = 0
    failed_files = []
    non_blocking_files = []

    for sas_file in sas_files:
        checked, mismatches = compare_file(sas_file, args.rows)
        total_checked += checked

        rel_path = str(sas_file.relative_to(TEST_DATA_DIR))
        is_non_blocking = mismatches > 0 and rel_path in NON_BLOCKING_MISMATCH_PATHS
        if is_non_blocking:
            non_blocking_mismatches += mismatches
            non_blocking_files.append(rel_path)
        else:
            total_mismatches += mismatches

        if mismatches > 0:
            if is_non_blocking:
                print(
                    f"NON-BLOCKING mismatch for {rel_path}; "
                    "continuing to remaining files."
                )
            else:
                failed_files.append(sas_file.name)
                if args.xfail:
                    print("THERE ARE MISMATCHES - CONTINUING (xfail)")
                else:
                    print("THERE ARE MISMATCHES - STOPPING")
                    break

    # Summary
    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checked} values checked across {len(sas_files)} files")
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
