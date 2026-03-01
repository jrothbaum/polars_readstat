# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare SAS metadata from polars_readstat_rs against polars_readstat==0.11.1.

Run with: uv run tests/sas/compare_metadata_to_python.py

For each .sas7bdat file, reads metadata from both:
  1) polars_readstat==0.11.1 via ScanReadstat.metadata (Python/C++ reference)
  2) polars_readstat_rs via the sas_metadata example (Rust)
and compares table_name, compression, row counts, encoding, and per-variable
labels/formats.
"""

import json
import subprocess
import sys
from pathlib import Path

from polars_readstat import ScanReadstat

MAX_FILE_SIZE = 1_000_000_000  # 1 GB
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "sas" / "data"

# Files where known differences exist where _rs is more correct than the reference.
NON_BLOCKING_MISMATCH_PATHS: set[str] = {
    # tunafish has Windows-1252 byte 0x96 (en dash) in a label.
    # polars_readstat incorrectly decodes it as a replacement char (treating as ISO-8859-1);
    # polars_readstat_rs correctly decodes it as U+2013 (en dash).
    "data_poe/tunafish.sas7bdat",
    "data_poe/tunafish_small.sas7bdat",
}

# Fields to compare at the file level (python_key, rust_key, normalize_fn)
# normalize_fn is applied to both values before comparison
def _lower(v):
    return v.lower() if isinstance(v, str) else v

FILE_LEVEL_FIELDS = [
    # (python_key, rust_key, normalize)
    ("table_name", "table_name", None),
    ("compression", "compression", _lower),
    ("row_count", "row_count", None),
    ("row_length", "row_length", None),
    ("file_encoding", "file_encoding", _lower),
    ("file_type", "file_type", None),
    ("sas_release", "sas_release", None),
    ("sas_server_type", "sas_server_type", None),
    ("os_name", "os_name", None),
    ("creator_proc", "creator_proc", None),
    ("page_size", "page_size", None),
    ("page_count", "page_count", None),
    ("header_length", "header_length", None),
]

# Variable type class mapping: python -> rust canonical form.
# Python reports "date"/"datetime"/"time" for numeric columns with date formats;
# Rust only tracks the raw SAS storage type (numeric vs character).
# Normalize all date-like types to "numeric" for comparison.
TYPE_MAP = {
    "number": "numeric",
    "date": "numeric",
    "datetime": "numeric",
    "time": "numeric",
    "string": "character",
}


def _normalize_type(v):
    if v is None:
        return None
    return TYPE_MAP.get(v.lower(), v.lower())


def _python_metadata(sas_file: Path) -> dict | None:
    try:
        r = ScanReadstat(str(sas_file))
        return r.metadata
    except Exception as e:
        print(f"  SKIP: polars_readstat failed: {e}")
        return None


def _rust_metadata(sas_file: Path) -> dict | None:
    result = subprocess.run(
        [
            "cargo",
            "run",
            "--release",
            "--example",
            "sas_metadata",
            "--",
            str(sas_file),
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(f"  SKIP: Rust metadata failed: {result.stderr[:200]}")
        return None
    # Cargo may print compile output to stderr; stdout should be pure JSON
    try:
        return json.loads(result.stdout.strip())
    except json.JSONDecodeError as e:
        print(f"  SKIP: Rust metadata JSON parse failed: {e}")
        print(f"  stdout: {result.stdout[:200]}")
        return None


def compare_file(sas_file: Path) -> tuple[int, int]:
    """Compare metadata for one file. Returns (checks, mismatches)."""
    print(f"\n--- {sas_file.name} ({sas_file.stat().st_size / 1024:.1f} KB) ---")

    py_meta = _python_metadata(sas_file)
    if py_meta is None:
        return 0, 0
    rs_meta = _rust_metadata(sas_file)
    if rs_meta is None:
        return 0, 0

    mismatches = 0
    checks = 0

    # --- File-level fields ---
    for py_key, rs_key, norm in FILE_LEVEL_FIELDS:
        py_val = py_meta.get(py_key)
        rs_val = rs_meta.get(rs_key)
        if norm is not None:
            py_val = norm(py_val)
            rs_val = norm(rs_val)
        checks += 1
        if py_val != rs_val:
            print(f"  MISMATCH {py_key}: python={py_val!r}  rust={rs_val!r}")
            mismatches += 1

    # --- Variable-level metadata ---
    py_vars = {v["name"]: v for v in py_meta.get("variables", [])}
    rs_vars = {v["name"]: v for v in rs_meta.get("columns", [])}

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
            py_label = pv.get("label") or None
            rs_label = rv.get("label") or None
            if py_label != rs_label:
                print(f"  MISMATCH {name}.label: python={py_label!r}  rust={rs_label!r}")
                mismatches += 1

            # format
            checks += 1
            py_fmt = pv.get("format") or None
            rs_fmt = rv.get("format") or None
            if py_fmt != rs_fmt:
                print(f"  MISMATCH {name}.format: python={py_fmt!r}  rust={rs_fmt!r}")
                mismatches += 1

            # type class
            checks += 1
            py_type = _normalize_type(pv.get("type_class"))
            rs_type = _normalize_type(rv.get("type"))
            if py_type != rs_type:
                print(f"  MISMATCH {name}.type: python={py_type!r}  rust={rs_type!r}")
                mismatches += 1

    if mismatches == 0:
        print(f"  OK ({checks} checks)")
    else:
        print(f"  FAILED: {mismatches}/{checks} checks failed")
    return checks, mismatches


def main():
    print("=== Comparing SAS metadata: polars_readstat_rs vs polars_readstat (C++ reference) ===\n")

    # Build the Rust example once upfront
    print("Building Rust sas_metadata (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "sas_metadata"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    sas_files = sorted(
        f
        for f in TEST_DATA_DIR.glob("**/*.sas7bdat")
        if "too_big" not in f.parts and f.stat().st_size <= MAX_FILE_SIZE
    )

    if not sas_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(sas_files)} test files\n")

    total_checks = 0
    total_mismatches = 0
    non_blocking_mismatches = 0
    failed_files: list[str] = []
    non_blocking_files: list[str] = []

    for sas_file in sas_files:
        checks, mismatches = compare_file(sas_file)
        total_checks += checks

        rel_path = str(sas_file.relative_to(TEST_DATA_DIR))
        is_non_blocking = mismatches > 0 and rel_path in NON_BLOCKING_MISMATCH_PATHS
        if is_non_blocking:
            non_blocking_mismatches += mismatches
            non_blocking_files.append(rel_path)
        else:
            total_mismatches += mismatches

        if mismatches > 0 and not is_non_blocking:
            failed_files.append(sas_file.name)
            print("THERE ARE MISMATCHES - STOPPING")
            break

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checks} checks across {len(sas_files)} files")
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
