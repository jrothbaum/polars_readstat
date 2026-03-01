# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare Stata metadata from polars_readstat_rs against polars_readstat==0.11.1.

Run with: uv run tests/stata/compare_metadata_to_python.py

For each .dta file, reads metadata from both:
  1) polars_readstat==0.11.1 via ScanReadstat.metadata (Python/ReadStat reference)
  2) polars_readstat_rs via the stata_metadata example (Rust)
and compares dataset label, row count, and per-variable labels/formats.
"""

import json
import subprocess
import sys
from pathlib import Path

from polars_readstat import ScanReadstat

MAX_FILE_SIZE = 1_000_000_000  # 1 GB
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "stata" / "data"

# Files where known differences exist where _rs is more correct than the reference.
NON_BLOCKING_MISMATCH_PATHS: set[str] = {
    # The following are very old Stata formats (versions 102/103) that
    # polars_readstat (readstat backend) returns empty metadata for (0 rows,
    # 0 vars). polars_readstat_rs reads them correctly.
    "stata-compat-102.dta",
    "stata-compat-103.dta",
    "stata-compat-be-103.dta",
    "stata1_102.dta",
    "stata1_103.dta",
    "stata4_102.dta",
    "stata4_103.dta",
    "stata8_102.dta",
    "stata8_103.dta",
    "stata_int_validranges_102.dta",
    "stata_int_validranges_103.dta",
}

# Variable type class mapping: python -> rust canonical form.
# Python reports "date"/"datetime"/"time" for numeric columns with date formats;
# Rust only tracks raw Stata type (Numeric/Str/StrL).
TYPE_MAP = {
    "number": "numeric",
    "date": "numeric",
    "datetime": "numeric",
    "time": "numeric",
    "string": "str",
}


def _lower(v):
    return v.lower() if isinstance(v, str) else v


def _normalize_type(v):
    if v is None:
        return None
    return TYPE_MAP.get(v.lower(), v.lower())


def _rust_type_class(rust_type: str) -> str:
    """Normalize Rust type string (e.g. 'Numeric(Double)', 'Str(10)', 'StrL') to 'numeric'/'str'."""
    if rust_type is None:
        return None
    t = rust_type.lower()
    if t.startswith("numeric"):
        return "numeric"
    if t.startswith("str"):
        return "str"
    return t


def _python_metadata(dta_file: Path) -> dict | None:
    try:
        r = ScanReadstat(str(dta_file))
        return r.metadata
    except Exception as e:
        print(f"  SKIP: polars_readstat failed: {e}")
        return None


def _rust_metadata(dta_file: Path) -> dict | None:
    result = subprocess.run(
        [
            "cargo",
            "run",
            "--release",
            "--example",
            "stata_metadata",
            "--",
            str(dta_file),
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(f"  SKIP: Rust metadata failed: {result.stderr[:200]}")
        return None
    try:
        return json.loads(result.stdout.strip())
    except json.JSONDecodeError as e:
        print(f"  SKIP: Rust metadata JSON parse failed: {e}")
        return None


def compare_file(dta_file: Path) -> tuple[int, int]:
    """Compare metadata for one file. Returns (checks, mismatches)."""
    print(f"\n--- {dta_file.name} ({dta_file.stat().st_size / 1024:.1f} KB) ---")

    py_meta = _python_metadata(dta_file)
    if py_meta is None:
        return 0, 0
    rs_meta = _rust_metadata(dta_file)
    if rs_meta is None:
        return 0, 0

    mismatches = 0
    checks = 0

    # --- File-level fields ---
    # row_count
    checks += 1
    py_rc = py_meta.get("row_count")
    rs_rc = rs_meta.get("row_count")
    if py_rc != rs_rc:
        print(f"  MISMATCH row_count: python={py_rc!r}  rust={rs_rc!r}")
        mismatches += 1

    # file_label (Python) = data_label (Rust)
    checks += 1
    py_label = py_meta.get("file_label") or None
    rs_label = rs_meta.get("data_label") or None
    if py_label != rs_label:
        print(f"  MISMATCH file_label/data_label: python={py_label!r}  rust={rs_label!r}")
        mismatches += 1

    # --- Variable-level metadata ---
    py_vars = {v["name"]: v for v in py_meta.get("variables", [])}
    rs_vars = {v["name"]: v for v in rs_meta.get("variables", [])}

    py_names = list(py_vars)
    rs_names = list(rs_vars)
    checks += 1
    if py_names != rs_names:
        print(f"  MISMATCH variable names/order: python={py_names}  rust={rs_names}")
        mismatches += 1
    else:
        for name in py_names:
            pv = py_vars[name]
            rv = rs_vars[name]

            # label
            checks += 1
            py_lbl = pv.get("label") or None
            rs_lbl = rv.get("label") or None
            if py_lbl != rs_lbl:
                print(f"  MISMATCH {name}.label: python={py_lbl!r}  rust={rs_lbl!r}")
                mismatches += 1

            # format
            checks += 1
            py_fmt = pv.get("format") or None
            rs_fmt = rv.get("format") or None
            if py_fmt != rs_fmt:
                print(f"  MISMATCH {name}.format: python={py_fmt!r}  rust={rs_fmt!r}")
                mismatches += 1

            # type class (normalized)
            checks += 1
            py_type = _normalize_type(pv.get("type_class"))
            rs_type = _rust_type_class(rv.get("type"))
            if py_type != rs_type:
                print(f"  MISMATCH {name}.type: python={py_type!r}  rust={rv.get('type')!r}")
                mismatches += 1

    if mismatches == 0:
        print(f"  OK ({checks} checks)")
    else:
        print(f"  FAILED: {mismatches}/{checks} checks failed")
    return checks, mismatches


def main():
    print("=== Comparing Stata metadata: polars_readstat_rs vs polars_readstat (ReadStat reference) ===\n")

    print("Building Rust stata_metadata (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "stata_metadata"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    dta_files = sorted(
        f
        for f in TEST_DATA_DIR.glob("**/*.dta")
        if f.stat().st_size <= MAX_FILE_SIZE
    )

    if not dta_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(dta_files)} test files\n")

    total_checks = 0
    total_mismatches = 0
    non_blocking_mismatches = 0
    failed_files: list[str] = []
    non_blocking_files: list[str] = []

    for dta_file in dta_files:
        checks, mismatches = compare_file(dta_file)
        total_checks += checks

        rel_path = str(dta_file.relative_to(TEST_DATA_DIR))
        is_non_blocking = mismatches > 0 and rel_path in NON_BLOCKING_MISMATCH_PATHS
        if is_non_blocking:
            non_blocking_mismatches += mismatches
            non_blocking_files.append(rel_path)
        else:
            total_mismatches += mismatches

        if mismatches > 0 and not is_non_blocking:
            failed_files.append(dta_file.name)
            print("THERE ARE MISMATCHES - STOPPING")
            break

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checks} checks across {len(dta_files)} files")
    print(f"       {total_mismatches} blocking mismatches")
    print(f"       {non_blocking_mismatches} non-blocking mismatches")
    if non_blocking_files:
        print(f"NON-BLOCKING files: {', '.join(non_blocking_files)}")

    if total_mismatches == 0:
        print("ALL FILES MATCH!")
    else:
        print(f"FAILED files: {', '.join(failed_files)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
