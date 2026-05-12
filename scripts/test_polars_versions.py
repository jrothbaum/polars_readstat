"""Simple smoke test: read one SAS, Stata, and SPSS file."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import polars_readstat as prs

REPO_ROOT = Path(__file__).resolve().parents[1]
RS_DATA = REPO_ROOT / "crates/polars_readstat_rs/tests"

FILES = {
    "sas":   RS_DATA / "sas/data/data_pandas/test1.sas7bdat",
    "stata": RS_DATA / "stata/data/stata15.dta",
    "spss":  RS_DATA / "spss/data/sample.sav",
}

print(f"polars {pl.__version__}")
print()

ok = True
for fmt, path in FILES.items():
    try:
        df = prs.read_readstat(str(path))
        print(f"  [{fmt}] OK  shape={df.shape}")
    except Exception as exc:
        print(f"  [{fmt}] FAIL  {type(exc).__name__}: {exc}")
        ok = False

sys.exit(0 if ok else 1)
