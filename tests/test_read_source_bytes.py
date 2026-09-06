"""Reading from in-memory bytes (`data=...`) must match reading the same
file by path, for every format the package supports.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_readstat as prs

REPO_ROOT = Path(__file__).resolve().parents[1]
RS_TESTS_ROOT = REPO_ROOT / "crates/polars_readstat_rs/tests"

SAS_PATH = RS_TESTS_ROOT / "sas/data/data_pandas/test1.sas7bdat"
STATA_PATH = RS_TESTS_ROOT / "stata/data/stata15.dta"
SPSS_PATH = RS_TESTS_ROOT / "spss/data/sample.sav"
XPT_PATH = RS_TESTS_ROOT / "sas/data/xpt/sample.xpt"
POR_PATH = RS_TESTS_ROOT / "spss/data/sample.por"
CATALOG_PATH = RS_TESTS_ROOT / "sas/data/data_gov/formats.sas7bcat"


def _require(path: Path) -> Path:
    if not path.exists():
        pytest.skip(f"fixture not found: {path}")
    return path


@pytest.mark.parametrize(
    "path",
    [SAS_PATH, STATA_PATH, SPSS_PATH, XPT_PATH],
    ids=["sas7bdat", "dta", "sav", "xpt"],
)
def test_scan_readstat_bytes_matches_path(path: Path) -> None:
    path = _require(path)
    data = path.read_bytes()

    df_path = prs.scan_readstat(str(path)).collect()
    df_bytes = prs.scan_readstat(str(path), data=data).collect()

    assert_frame_equal(df_path, df_bytes, check_row_order=False)


@pytest.mark.parametrize(
    "path",
    [SAS_PATH, STATA_PATH, SPSS_PATH],
    ids=["sas7bdat", "dta", "sav"],
)
def test_read_readstat_bytes_matches_path(path: Path) -> None:
    path = _require(path)
    data = path.read_bytes()

    df_path = prs.read_readstat(str(path))
    df_bytes = prs.read_readstat(str(path), data=data)

    assert_frame_equal(df_path, df_bytes, check_row_order=False)


@pytest.mark.parametrize(
    "path",
    [SAS_PATH, STATA_PATH, SPSS_PATH, XPT_PATH],
    ids=["sas7bdat", "dta", "sav", "xpt"],
)
def test_scan_readstat_reader_bytes_schema_and_metadata(path: Path) -> None:
    """ScanReadstat(..., data=...) — schema and metadata dict must match the
    path-based reader too (this is the up-front, before-any-row-is-read
    path Polars' IO plugin registration depends on)."""
    path = _require(path)
    data = path.read_bytes()

    reader_path = prs.ScanReadstat(str(path))
    reader_bytes = prs.ScanReadstat(str(path), data=data)

    assert reader_bytes.schema == reader_path.schema
    assert reader_bytes.metadata.get("row_count") == reader_path.metadata.get("row_count")


def test_scan_readstat_por_bytes_matches_path() -> None:
    path = _require(POR_PATH)
    data = path.read_bytes()

    df_path = prs.scan_readstat(str(path)).collect()
    df_bytes = prs.scan_readstat(str(path), data=data).collect()

    assert_frame_equal(df_path, df_bytes, check_row_order=False)


def _catalog_normalized(entries: dict) -> list:
    # NaN (the catch-all missing-value key) is neither orderable nor equal
    # to itself; normalize each key to a (type_tag, value) pair so sorting
    # never compares across types and NaN compares equal to itself.
    def norm(code):
        if isinstance(code, float):
            return ("nan", None) if code != code else ("float", code)
        return ("str", code)

    return sorted((norm(code), label) for code, label in entries.items())


def test_read_sas7bcat_bytes_matches_path() -> None:
    path = _require(CATALOG_PATH)
    data = path.read_bytes()

    catalog_path = prs.read_sas7bcat(str(path))
    catalog_bytes = prs.read_sas7bcat(str(path), data=data)

    assert catalog_path.keys() == catalog_bytes.keys()
    for fmt_name, entries_path in catalog_path.items():
        entries_bytes = catalog_bytes[fmt_name]
        assert _catalog_normalized(entries_path) == _catalog_normalized(entries_bytes), (
            f"catalog entries diverged for format {fmt_name!r}"
        )


@pytest.mark.parametrize(
    "path",
    [SAS_PATH, STATA_PATH, SPSS_PATH],
    ids=["sas7bdat", "dta", "sav"],
)
def test_scan_readstat_bytes_with_compress(path: Path) -> None:
    """The compress= probe-read path has its own bytes-aware code path —
    exercise it explicitly rather than relying on default (compress=None)
    coverage above."""
    path = _require(path)
    data = path.read_bytes()

    df_path = prs.scan_readstat(str(path), compress=True).collect()
    df_bytes = prs.scan_readstat(str(path), data=data, compress=True).collect()

    assert_frame_equal(df_path, df_bytes, check_row_order=False)
