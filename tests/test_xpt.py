"""Compare XPT reading against pyreadstat."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pyreadstat = pytest.importorskip("pyreadstat")

import polars_readstat as prs

REPO_ROOT = Path(__file__).resolve().parents[1]
XPT_DIR = REPO_ROOT / "crates/polars_readstat_rs/tests/sas/data/xpt"
TOO_BIG_DIR = REPO_ROOT / "crates/polars_readstat_rs/tests/sas/data/too_big"


def read_prs(path: Path, **kwargs) -> pl.DataFrame:
    return prs.scan_readstat(str(path), **kwargs).collect()


def get_metadata(path: Path) -> dict:
    return prs.ScanReadstat(str(path)).metadata


def read_pyreadstat(path: Path) -> pl.DataFrame:
    df_pd, meta = pyreadstat.read_xport(str(path))
    return pl.from_pandas(df_pd)


def approx_equal(a: pl.DataFrame, b: pl.DataFrame, tol: float = 1e-6) -> None:
    assert a.columns == b.columns, f"columns differ: {a.columns} vs {b.columns}"
    assert a.shape == b.shape, f"shape differs: {a.shape} vs {b.shape}"
    assert_frame_equal(
        a,
        b,
        check_dtypes=False,
        check_row_order=True,
        check_column_order=True,
        abs_tol=tol,
        rel_tol=tol,
    )


# ─────────────────────────────────────────────────────────────────────────────
# sample.xpt — v5 file with mixed types including dates/datetimes/times
# ─────────────────────────────────────────────────────────────────────────────

def test_sample_xpt_shape():
    df = read_prs(XPT_DIR / "sample.xpt")
    assert df.shape == (5, 7)


def test_sample_xpt_columns():
    df = read_prs(XPT_DIR / "sample.xpt")
    assert df.columns == ["MYCHAR", "MYNUM", "MYDATE", "DTIME", "MYLABL", "MYORD", "MYTIME"]


def test_sample_xpt_numeric_values():
    df = read_prs(XPT_DIR / "sample.xpt")
    prs_df = pyreadstat.read_xport(str(XPT_DIR / "sample.xpt"))[0]
    pl_from_prs_ref = pl.from_pandas(prs_df)

    prs_num = df.select("MYNUM").to_series().to_list()
    ref_num = pl_from_prs_ref["MYNUM"].to_list()
    for a, b in zip(prs_num, ref_num):
        if a is None and b is None:
            continue
        assert abs(a - b) < 1e-9, f"MYNUM mismatch: {a} vs {b}"


def test_sample_xpt_string_col():
    df = read_prs(XPT_DIR / "sample.xpt")
    chars = df["MYCHAR"].to_list()
    assert chars == ["a", "b", "c", "d", "e"]


def test_sample_xpt_date_col():
    df = read_prs(XPT_DIR / "sample.xpt")
    assert df["MYDATE"].dtype == pl.Date
    # first row: 2018-05-06
    from datetime import date
    assert df["MYDATE"][0] == date(2018, 5, 6)


def test_sample_xpt_datetime_col():
    df = read_prs(XPT_DIR / "sample.xpt")
    assert df["DTIME"].dtype == pl.Datetime("us")
    from datetime import datetime
    assert df["DTIME"][0] == datetime(2018, 5, 6, 10, 10, 10)


def test_sample_xpt_time_col():
    df = read_prs(XPT_DIR / "sample.xpt")
    assert df["MYTIME"].dtype == pl.Time
    from datetime import time
    assert df["MYTIME"][0] == time(10, 10, 10)


def test_sample_xpt_vs_pyreadstat():
    path = XPT_DIR / "sample.xpt"
    prs_df = read_prs(path)
    ref = read_pyreadstat(path)
    approx_equal(prs_df, ref)


# ─────────────────────────────────────────────────────────────────────────────
# sas.xpt5 — simple v5 with one numeric column
# ─────────────────────────────────────────────────────────────────────────────

def test_xpt5_vs_pyreadstat():
    path = XPT_DIR / "sas.xpt5"
    prs_df = read_prs(path)
    ref_pd, _ = pyreadstat.read_xport(str(path))
    ref = pl.from_pandas(ref_pd)

    assert prs_df.shape == ref.shape
    assert prs_df.columns == ref.columns
    prs_vals = prs_df.to_series().to_list()
    ref_vals = ref.to_series().to_list()
    for a, b in zip(prs_vals, ref_vals):
        assert abs(a - b) < 1e-9, f"value mismatch: {a} vs {b}"


# ─────────────────────────────────────────────────────────────────────────────
# sas.xpt8 — v8 with long name (lowercase "i" vs uppercase "I" in v5)
# ─────────────────────────────────────────────────────────────────────────────

def test_xpt8_shape():
    df = read_prs(XPT_DIR / "sas.xpt8")
    assert df.shape == (10, 1)


def test_xpt8_long_name():
    df = read_prs(XPT_DIR / "sas.xpt8")
    # v8 uses longname field — pyreadstat should agree
    path = XPT_DIR / "sas.xpt8"
    ref_pd, _ = pyreadstat.read_xport(str(path))
    ref = pl.from_pandas(ref_pd)
    assert df.columns == ref.columns


def test_xpt8_vs_pyreadstat():
    path = XPT_DIR / "sas.xpt8"
    prs_df = read_prs(path)
    ref_pd, _ = pyreadstat.read_xport(str(path))
    ref = pl.from_pandas(ref_pd)
    assert prs_df.shape == ref.shape
    prs_vals = prs_df.to_series().to_list()
    ref_vals = ref.to_series().to_list()
    for a, b in zip(prs_vals, ref_vals):
        assert abs(a - b) < 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# dates_xpt_v8.xpt — v8 with dates/datetimes/times and missing values
# ─────────────────────────────────────────────────────────────────────────────

def test_dates_v8_shape():
    df = read_prs(XPT_DIR / "dates_xpt_v8.xpt")
    assert df.shape[1] == 9
    assert df.shape[0] > 0


def test_dates_v8_types():
    df = read_prs(XPT_DIR / "dates_xpt_v8.xpt")
    assert df["dt"].dtype == pl.Datetime("us")
    assert df["dates"].dtype == pl.Date
    assert df["times"].dtype == pl.Time


def test_dates_v8_missing_values():
    df = read_prs(XPT_DIR / "dates_xpt_v8.xpt")
    # 'missings' column has a pattern of nulls
    nulls = df["missings"].is_null().sum()
    assert nulls > 0


def test_dates_v8_numeric_vs_pyreadstat():
    path = XPT_DIR / "dates_xpt_v8.xpt"
    prs_df = read_prs(path)
    ref_pd, _ = pyreadstat.read_xport(str(path))
    ref = pl.from_pandas(ref_pd)

    # Compare numeric 'seconds' column (no date conversion ambiguity)
    prs_secs = prs_df["seconds"].to_list()
    ref_secs = ref["seconds"].to_list()
    for a, b in zip(prs_secs, ref_secs):
        if a is None and b is None:
            continue
        if a is None or b is None:
            assert False, f"null mismatch: prs={a} ref={b}"
        assert abs(a - b) < 1e-6, f"seconds mismatch: {a} vs {b}"


def test_dates_v8_vs_pyreadstat():
    path = XPT_DIR / "dates_xpt_v8.xpt"
    prs_df = read_prs(path)
    ref = read_pyreadstat(path)
    approx_equal(prs_df, ref)


# ─────────────────────────────────────────────────────────────────────────────
# ACQ_G.xpt — real-world NHANES file (smoke test only, no pyreadstat comparison
# since it may be large)
# ─────────────────────────────────────────────────────────────────────────────

def test_acq_g_reads():
    path = XPT_DIR / "ACQ_G.xpt"
    if not path.exists():
        pytest.skip("ACQ_G.xpt not present")
    df = read_prs(path)
    assert df.shape[0] > 0
    assert df.shape[1] > 0


def test_acq_g_vs_pyreadstat():
    path = XPT_DIR / "ACQ_G.xpt"
    if not path.exists():
        pytest.skip("ACQ_G.xpt not present")
    prs_df = read_prs(path)
    ref = read_pyreadstat(path)
    approx_equal(prs_df, ref)


# ─────────────────────────────────────────────────────────────────────────────
# Metadata JSON
# ─────────────────────────────────────────────────────────────────────────────

def test_metadata_json_sample():
    import json
    md = get_metadata(XPT_DIR / "sample.xpt")
    assert md["xpt_version"] == 5
    assert md["table_name"] == "SAMPLE"
    assert md["column_count"] == 7
    names = [c["name"] for c in md["columns"]]
    assert names == ["MYCHAR", "MYNUM", "MYDATE", "DTIME", "MYLABL", "MYORD", "MYTIME"]


def test_metadata_json_v8():
    md = get_metadata(XPT_DIR / "sas.xpt8")
    assert md["xpt_version"] == 8


# ─────────────────────────────────────────────────────────────────────────────
# scan_readstat (lazy) API
# ─────────────────────────────────────────────────────────────────────────────

def test_scan_readstat_lazy():
    lf = prs.scan_readstat(str(XPT_DIR / "sample.xpt"))
    df = lf.collect()
    assert df.shape == (5, 7)


def test_scan_readstat_with_columns():
    lf = prs.scan_readstat(str(XPT_DIR / "sample.xpt"))
    df = lf.select(["MYCHAR", "MYNUM"]).collect()
    assert df.columns == ["MYCHAR", "MYNUM"]
    assert df.shape[0] == 5


# ─────────────────────────────────────────────────────────────────────────────
# Parallel read — 1M-row file
# ─────────────────────────────────────────────────────────────────────────────

BIG_XPT = TOO_BIG_DIR / "numeric_1000000_2.xpt"


def test_big_xpt_parallel_matches_serial():
    if not BIG_XPT.exists():
        pytest.skip(f"Missing fixture: {BIG_XPT}")
    df_serial = prs.scan_readstat(str(BIG_XPT), threads=1).collect()
    df_parallel = prs.scan_readstat(str(BIG_XPT), preserve_order=True).collect()
    assert df_serial.shape == df_parallel.shape
    assert_frame_equal(df_serial, df_parallel, check_dtypes=True)


def test_big_xpt_parallel_matches_pyreadstat():
    if not BIG_XPT.exists():
        pytest.skip(f"Missing fixture: {BIG_XPT}")
    df = prs.scan_readstat(str(BIG_XPT), preserve_order=True).collect()
    ref_pd, _ = pyreadstat.read_xport(str(BIG_XPT))
    ref = pl.from_pandas(ref_pd)
    assert df.shape == (1_000_000, 2)
    assert_frame_equal(df, ref, check_dtypes=False, abs_tol=1e-6)
