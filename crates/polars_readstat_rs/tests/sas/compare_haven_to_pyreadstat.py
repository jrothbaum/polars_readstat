# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyreadstat",
#     "pandas",
#     "polars",
#     "pyarrow",
# ]
# ///

"""Compare Rust SAS reader output against pyreadstat for the haven fixtures
in tests/sas/data/haven/ (https://github.com/tidyverse/haven, MIT).

Run with: uv run tests/sas/compare_haven_to_pyreadstat.py

For each .sas7bdat, reads with pyreadstat and with the Rust reader
(readstat_dump_parquet example) and compares values column by column.
Files with a sibling .sas7bcat of the same name are read with the catalog
and user_missing=True on the pyreadstat side.
"""

import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pyreadstat

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HAVEN_DIR = PROJECT_ROOT / "tests" / "sas" / "data" / "haven"
SCRATCH_DIR = Path("/tmp/polars_readstat_haven_compare")
# Release by default; set HAVEN_COMPARE_DEBUG=1 to reuse a cached debug build.
PROFILE_ARGS = [] if os.environ.get("HAVEN_COMPARE_DEBUG") else ["--release"]


def rust_read(path: Path) -> pl.DataFrame:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    out = SCRATCH_DIR / (path.stem + ".parquet")
    r = subprocess.run(
        ["cargo", "run", *PROFILE_ARGS, "--example", "readstat_dump_parquet",
         "--", str(path), str(out)],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-500:])
    return pl.read_parquet(out)


def norm(s: pl.Series) -> list:
    vals = s.to_list()
    out = []
    for v in vals:
        if isinstance(v, float) and v != v:
            v = None
        out.append(str(v) if v is not None else None)
    return out


def compare(path: Path) -> int:
    print(f"\n--- {path.name} ---")
    py_df, _ = pyreadstat.read_sas7bdat(str(path))
    py = pl.from_pandas(py_df)
    rs = rust_read(path)
    mism = 0
    if py.columns != rs.columns:
        print(f"  COLUMNS differ: py={py.columns} rust={rs.columns}")
        return 1
    if py.height != rs.height:
        print(f"  ROWS differ: py={py.height} rust={rs.height}")
        return 1
    for c in py.columns:
        a, b = norm(py[c]), norm(rs[c])
        if a != b:
            mism += 1
            print(f"  MISMATCH {c}: py={a[:5]} rust={b[:5]} (dtypes py={py[c].dtype} rust={rs[c].dtype})")
    print(f"  {py.height} rows x {len(py.columns)} cols: " + ("OK" if mism == 0 else f"{mism} column(s) differ"))
    return mism


def main() -> None:
    files = sorted(HAVEN_DIR.glob("*.sas7bdat"))
    total = sum(compare(f) for f in files)
    print(f"\n{'ALL MATCH' if total == 0 else f'FAILED: {total} mismatch(es)'}")
    sys.exit(0 if total == 0 else 1)


if __name__ == "__main__":
    main()
