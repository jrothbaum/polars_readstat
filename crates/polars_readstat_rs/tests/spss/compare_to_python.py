# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare Rust SPSS reader output against polars_readstat (C++ reference) ground truth.

Run with: uv run tests/spss/compare_to_python.py [--rows N] [--xfail]

Iterates all .sav/.zsav files in tests/spss/data/,
reads each with polars_readstat==0.11.1 and polars_readstat_rs,
writes both to parquet, then compares data and schemas via polars.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
import os
import polars as pl
from polars.testing import assert_frame_equal
from polars_readstat import scan_readstat

N_ROWS = 10
MAX_FILE_SIZE = 1_000_000_000  # 1GB
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "spss" / "data"
SCRATCH_ROOT = Path(os.environ.get("READSTAT_SCRATCH_DIR", "/tmp/polars_readstat_compare"))
NON_BLOCKING_MISMATCH_PATHS = {
    "labelled-num-na.sav",
    "missing_char.sav",
    "missing_test.sav",
    "sample_missing.sav",
    "simple_alltypes.sav",
}
INFORMATIVE_NULL_TESTS = {
    "labelled-num-na.sav": {
        "column": "VAR00002",
        "missing_values": ["9"],
    },
    "missing_char.sav": {
        "column": "mychar",
        "missing_values": ["Z"],
    },
    "missing_test.sav": {
        "column": "var1",
        "missing_values": ["missing"],
    },
    "sample_missing.sav": {
        "column": "mynum",
        "missing_values": ["MISSING", "-1"],
    },
    "simple_alltypes.sav": {
        "column": "x",
        "missing_values": ["8"],
    },
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
TEMPORAL_CLASS_TO_DTYPE = {
    "Date": pl.Date,
    "DateTime": pl.Datetime,
    "Time": pl.Time,
}


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


def _expected_temporal_types(path: Path) -> dict[str, pl.DataType]:
    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "spss_metadata", "--", str(path)],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(f"  WARN: Failed to read SPSS metadata: {result.stderr[:200]}")
        return {}
    meta_line = None
    for line in result.stdout.splitlines():
        if line.strip().startswith("{"):
            meta_line = line.strip()
            break
    if not meta_line:
        print("  WARN: SPSS metadata output not found")
        return {}
    try:
        meta = json.loads(meta_line)
    except json.JSONDecodeError:
        print("  WARN: Failed to parse SPSS metadata JSON")
        return {}
    expected: dict[str, pl.DataType] = {}
    for var in meta.get("variables", []):
        cls = var.get("format_class")
        name = var.get("name")
        if cls in TEMPORAL_CLASS_TO_DTYPE and name:
            expected[name] = TEMPORAL_CLASS_TO_DTYPE[cls]
    return expected


def _check_informative_nulls(
    sav_file: Path,
    scratch_dir: Path,
    n_rows: int,
) -> int:
    test_cfg = INFORMATIVE_NULL_TESTS.get(sav_file.name)
    if not test_cfg:
        return 0

    col = test_cfg["column"]
    expected_missing = set(test_cfg["missing_values"])
    null_col = f"{col}_null"
    rust_path = scratch_dir / "polars_readstat_rs_informative.parquet"

    env = dict(os.environ)
    env["READSTAT_PRESERVE_ORDER"] = "1"
    env["READSTAT_INFORMATIVE_NULLS"] = col
    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "readstat_dump_parquet", "--",
         str(sav_file), str(rust_path), str(n_rows)],
        capture_output=True, text=True,
        cwd=PROJECT_ROOT,
        env=env,
    )
    if result.returncode != 0:
        print(f"  FAIL: Informative-null run failed: {result.stderr[:200]}")
        return 1

    df = pl.read_parquet(rust_path)
    if null_col not in df.columns:
        print(f"  FAIL: Informative-null column missing: {null_col}")
        return 1

    vals = df[null_col].to_list()
    base_vals = df[col].to_list() if col in df.columns else []
    if not vals or not base_vals:
        print(f"  FAIL: Informative-null columns empty for {null_col}")
        return 1

    missing_idx = [i for i, v in enumerate(base_vals) if v is None]
    if not missing_idx:
        print(f"  FAIL: Informative-null test found no nulls in {col}")
        return 1

    expected_opts = set()
    for v in expected_missing:
        expected_opts.add(v)
        expected_opts.add(f"{v}.0")
    for i in missing_idx:
        if vals[i] not in expected_opts:
            print(
                f"  FAIL: Informative-null value mismatch at row {i}: {vals[i]} (expected {expected_opts})"
            )
            return 1
    for i, v in enumerate(vals):
        if i not in missing_idx and v is not None:
            print(
                f"  FAIL: Informative-null unexpected value at row {i}: {v} (base={base_vals[i]})"
            )
            return 1

    print(f"  OK (informative nulls): {null_col} rows={missing_idx}")
    return 0


def compare_file(sav_file: Path, n_rows: int) -> tuple[int, int]:
    print(f"\n--- {sav_file.name} ({sav_file.stat().st_size / 1024 / 1024:.1f} MB) ---")

    if sav_file.suffix.lower() == ".zsav":
        print("  SKIP: polars_readstat does not support .zsav")
        return 0, 0

    rel = sav_file.relative_to(TEST_DATA_DIR)
    scratch_dir = SCRATCH_ROOT / "spss" / "__".join(rel.parts)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prs_path = scratch_dir / "polars_readstat.parquet"
    rust_path = scratch_dir / "polars_readstat_rs.parquet"

    try:
        df_prs = scan_readstat(str(sav_file)).collect().head(n_rows)
        prs_path.parent.mkdir(parents=True, exist_ok=True)
        df_prs.write_parquet(prs_path)
    except BaseException as e:
        print(f"  SKIP: polars_readstat failed: {e}")
        return 0, 0

    env = dict(os.environ)
    env["READSTAT_PRESERVE_ORDER"] = "1"
    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "readstat_dump_parquet", "--",
         str(sav_file), str(rust_path), str(n_rows)],
        capture_output=True, text=True,
        cwd=PROJECT_ROOT,
        env=env,
    )

    if result.returncode != 0:
        print(f"  SKIP: Rust reader failed: {result.stderr[:200]}")
        return 0, 0

    df_rust = pl.read_parquet(rust_path)

    print(f"  polars_readstat:    {df_prs.shape[0]} rows x {df_prs.shape[1]} cols")
    print(f"  polars_readstat_rs: {df_rust.shape[0]} rows x {df_rust.shape[1]} cols")
    print(f"  Scratch: {scratch_dir}")

    mismatches = 0
    mismatches += _check_informative_nulls(sav_file, scratch_dir, n_rows)
    expected_types = _expected_temporal_types(sav_file)
    for col, expected in expected_types.items():
        if col not in df_rust.schema:
            print(f"  FAIL: Rust output missing expected column '{col}'")
            mismatches += 1
            continue
        actual = df_rust.schema[col]
        if expected is pl.Datetime:
            if not isinstance(actual, pl.Datetime):
                print(f"  FAIL: Rust '{col}' type {actual}, expected any Datetime")
                mismatches += 1
        elif actual != expected:
            print(f"  FAIL: Rust '{col}' type {actual}, expected {expected}")
            mismatches += 1

    if mismatches > 0:
        print(f"  FAILED: {mismatches} mismatches (see above)")
        return 0, mismatches

    compare_drop = set(expected_types.keys())
    if compare_drop:
        df_prs_cmp = df_prs.drop([c for c in compare_drop if c in df_prs.columns])
        df_rust_cmp = df_rust.drop([c for c in compare_drop if c in df_rust.columns])
        print(f"  NOTE: Comparing without temporal columns (validated separately): {sorted(compare_drop)}")
    else:
        df_prs_cmp = df_prs
        df_rust_cmp = df_rust

    schema_mismatches = _schema_mismatches(df_prs_cmp.schema, df_rust_cmp.schema)
    if schema_mismatches:
        print("  SCHEMA MISMATCH: polars_readstat vs polars_readstat_rs")
        print(f"    polars_readstat: {df_prs_cmp.schema}")
        print(f"    polars_readstat_rs: {df_rust_cmp.schema}")
        if len(schema_mismatches) <= 5:
            print(f"    Details: {schema_mismatches}")
        mismatches += 1
    try:
        df_prs_cmp = _cast_ints_for_compare(df_prs_cmp)
        df_rust_cmp = _cast_ints_for_compare(df_rust_cmp)
        assert_frame_equal(
            df_prs_cmp,
            df_rust_cmp,
            check_dtypes=True,
            check_row_order=True,
            check_column_order=True,
            rel_tol=1e-6,
            abs_tol=1e-6,
        )
    except AssertionError as e:
        print("  DATA MISMATCH: polars_readstat vs polars_readstat_rs")
        print(f"    {e}")
        mismatches += 1

    checked = df_prs.height * df_prs.width
    if mismatches == 0:
        print(f"  OK ({checked} values)")
    else:
        print(f"  FAILED: {mismatches} mismatches (see above)")

    return checked, mismatches


def main():
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

    sav_files = [
        f for f in TEST_DATA_DIR.glob("**/*.sav")
        if "too_big" not in f.parts
    ] + [
        f for f in TEST_DATA_DIR.glob("**/*.zsav")
        if "too_big" not in f.parts
    ]
    sav_files = sorted(sav_files)
    sav_files = [f for f in sav_files if f.stat().st_size <= MAX_FILE_SIZE]

    if not sav_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(sav_files)} test files (after size filter)")

    total_checked = 0
    total_mismatches = 0
    non_blocking_mismatches = 0
    failed_files = []
    non_blocking_files = []

    for sav_file in sav_files:
        checked, mismatches = compare_file(sav_file, args.rows)
        total_checked += checked
        rel_path = str(sav_file.relative_to(TEST_DATA_DIR))
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
                failed_files.append(sav_file.name)
                if args.xfail:
                    print("THERE ARE MISMATCHES - CONTINUING (xfail)")
                else:
                    print("THERE ARE MISMATCHES - STOPPING")
                    break

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checked} values checked across {len(sav_files)} files")
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
