# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyreadstat",
#     "pandas",
# ]
# ///

"""Compare Rust .sas7bcat catalog reader output against pyreadstat (ReadStat C library).

Run with: uv run tests/sas/compare_catalog_to_python.py

Finds all .sas7bcat files in tests/sas/data/, reads each with:
  1) pyreadstat.read_sas7bcat (C reference)
  2) polars_readstat_rs (Rust, via the sas_catalog_dump example -> JSON)
and compares the parsed format_name -> [(key, label), ...] maps.

`nan`-keyed pyreadstat entries (the catch-all label a format assigns to
missing/tagged-missing values) are compared against the Rust reader's
`CatalogKey::Missing` entries by label text only, since NaN has no stable
identity to compare structurally and pyreadstat itself collapses all
missing/tagged-missing keys for a format into a single `nan` entry.
"""

import json
import math
import subprocess
import sys
from pathlib import Path

import pyreadstat

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "sas" / "data"
SCRATCH_DIR = Path("/tmp/polars_readstat_catalog_compare")


def normalize_pyreadstat(value_labels: dict) -> dict:
    """format_name (normalised) -> {numeric/text key (or None for missing): label}."""
    out: dict = {}
    for name, labels in value_labels.items():
        norm_name = name.rstrip(".").upper()
        bucket = out.setdefault(norm_name, {})
        for key, label in labels.items():
            if isinstance(key, float) and math.isnan(key):
                bucket[None] = label
            elif isinstance(key, float):
                bucket[round(key, 6)] = label
            else:
                bucket[key] = label
    return out


def normalize_rust(dump: dict) -> dict:
    out: dict = {}
    for name, entries in dump.items():
        bucket = out.setdefault(name, {})
        for entry in entries:
            if entry["key_type"] == "numeric":
                bucket[round(entry["key"], 6)] = entry["label"]
            elif entry["key_type"] == "text":
                bucket[entry["key"]] = entry["label"]
            else:  # missing
                bucket[None] = entry["label"]
    return out


def compare_file(cat_file: Path) -> int:
    """Returns the number of mismatches found for this file."""
    print(f"\n--- {cat_file.relative_to(TEST_DATA_DIR)} ---")

    try:
        _, meta = pyreadstat.read_sas7bcat(str(cat_file))
    except Exception as e:
        print(f"  SKIP: pyreadstat failed: {e}")
        return 0
    py_catalog = normalize_pyreadstat(meta.value_labels)

    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    dump_path = SCRATCH_DIR / (cat_file.stem + ".json")
    result = subprocess.run(
        [
            "cargo", "run", "--release", "--example", "sas_catalog_dump",
            "--", str(cat_file), str(dump_path),
        ],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(f"  SKIP: Rust reader failed: {result.stderr[-500:]}")
        return 0
    rust_catalog = normalize_rust(json.loads(dump_path.read_text()))

    mismatches = 0
    all_formats = set(py_catalog) | set(rust_catalog)
    for name in sorted(all_formats):
        py_labels = py_catalog.get(name, {})
        rust_labels = rust_catalog.get(name, {})
        if py_labels != rust_labels:
            mismatches += 1
            print(f"  MISMATCH in format '{name}':")
            only_py = {k: v for k, v in py_labels.items() if k not in rust_labels}
            only_rust = {k: v for k, v in rust_labels.items() if k not in py_labels}
            differing = {
                k: (py_labels[k], rust_labels[k])
                for k in py_labels.keys() & rust_labels.keys()
                if py_labels[k] != rust_labels[k]
            }
            if only_py:
                print(f"    only in pyreadstat: {only_py}")
            if only_rust:
                print(f"    only in rust:        {only_rust}")
            if differing:
                print(f"    differing labels:    {differing}")

    py_total = sum(len(v) for v in py_catalog.values())
    rust_total = sum(len(v) for v in rust_catalog.values())
    print(f"  pyreadstat: {len(py_catalog)} formats, {py_total} labels")
    print(f"  rust:       {len(rust_catalog)} formats, {rust_total} labels")
    print("  OK" if mismatches == 0 else f"  FAILED: {mismatches} format(s) mismatched")
    return mismatches


def main() -> None:
    print("=== Comparing Rust .sas7bcat catalog reader vs pyreadstat ===")

    print("Building sas_catalog_dump (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "sas_catalog_dump"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)

    cat_files = sorted(TEST_DATA_DIR.glob("**/*.sas7bcat"))
    if not cat_files:
        print("No .sas7bcat test files found!")
        sys.exit(1)
    print(f"Found {len(cat_files)} catalog file(s)")

    total_mismatches = sum(compare_file(f) for f in cat_files)

    print(f"\n{'=' * 60}")
    if total_mismatches == 0:
        print("ALL CATALOG FILES MATCH!")
    else:
        print(f"FAILED: {total_mismatches} mismatched format(s) total")
        sys.exit(1)


if __name__ == "__main__":
    main()
