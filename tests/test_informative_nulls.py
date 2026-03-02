"""Tests for the informative_nulls feature across SAS, Stata, and SPSS formats.

Each test group verifies:
1. Reading without informative_nulls produces the baseline schema (no indicator cols).
2. Reading with informative_nulls adds indicator String columns.
3. At least some indicator cells are non-null (user-declared missings were found).
4. Both scan_readstat (lazy) and read_readstat (eager) paths work.
5. All three modes work: separate_column, struct, merged_string.
6. Column selection via LazyFrame.select([...]) works.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import polars_readstat as prs

RS_TESTS_ROOT = (Path(__file__).resolve().parents[2] / "polars_readstat_rs/tests").resolve()


# ─────────────────────────── fixtures ───────────────────────────────────────


@pytest.fixture(scope="session")
def sas_info_nulls(rs_tests_root: Path) -> Path:
    path = rs_tests_root / "sas/data/info_nulls_test_data.sas7bdat"
    if not path.exists():
        pytest.skip(f"Missing SAS informative-nulls fixture: {path}")
    return path


@pytest.fixture(scope="session")
def stata_missing(rs_tests_root: Path) -> Path:
    # stata8_115.dta has 78 confirmed user-defined missing cells
    path = rs_tests_root / "stata/data/stata8_115.dta"
    if not path.exists():
        pytest.skip(f"Missing Stata fixture: {path}")
    return path


@pytest.fixture(scope="session")
def spss_simple_alltypes(rs_tests_root: Path) -> Path:
    # discrete missing {7.0, 8.0, 99.0} on x, range missing on z
    path = rs_tests_root / "spss/data/simple_alltypes.sav"
    if not path.exists():
        pytest.skip(f"Missing SPSS fixture: {path}")
    return path


@pytest.fixture(scope="session")
def spss_labelled_num_na(rs_tests_root: Path) -> Path:
    # VAR00002 has discrete missing value 9.0
    path = rs_tests_root / "spss/data/labelled-num-na.sav"
    if not path.exists():
        pytest.skip(f"Missing SPSS fixture: {path}")
    return path


# ─────────────────────────── helpers ────────────────────────────────────────


def _indicator_cols(df: pl.DataFrame, suffix: str = "_null") -> list[str]:
    return [c for c in df.columns if c.endswith(suffix)]


def _total_non_null(df: pl.DataFrame, cols: list[str]) -> int:
    return sum(df[c].drop_nulls().len() for c in cols)


# ─────────────────────────── InformativeNullOpts dataclass ──────────────────


def test_informative_null_opts_defaults() -> None:
    opts = prs.InformativeNullOpts()
    assert opts.columns == "all"
    assert opts.mode == "separate_column"
    assert opts.suffix == "_null"
    assert opts.use_value_labels is True


def test_informative_null_opts_to_dict() -> None:
    opts = prs.InformativeNullOpts(columns=["x", "y"], mode="struct", suffix="_miss", use_value_labels=False)
    d = opts.to_dict()
    assert d["columns"] == ["x", "y"]
    assert d["mode"] == "struct"
    assert d["suffix"] == "_miss"
    assert d["use_value_labels"] is False


def test_informative_null_opts_accepts_dict(sas_info_nulls: Path) -> None:
    # Plain dict is accepted everywhere InformativeNullOpts is accepted
    opts_dict = {"columns": "all", "mode": "separate_column"}
    df = prs.read_readstat(str(sas_info_nulls), informative_nulls=opts_dict)
    assert len(_indicator_cols(df)) > 0, "dict-style opts should work identically to InformativeNullOpts"


# ─────────────────────────── SAS ────────────────────────────────────────────


def test_sas_baseline_has_no_indicator_cols(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(str(sas_info_nulls))
    assert _indicator_cols(df) == [], "baseline read should not have _null columns"


def test_sas_informative_nulls_adds_indicator_cols_read(sas_info_nulls: Path) -> None:
    base = prs.read_readstat(str(sas_info_nulls))
    df = prs.read_readstat(str(sas_info_nulls), informative_nulls={"columns": "all"})

    inds = _indicator_cols(df)
    assert len(inds) > 0, "expected _null indicator columns"
    assert df.width > base.width, "indicator path should widen the DataFrame"


def test_sas_informative_nulls_adds_indicator_cols_scan(sas_info_nulls: Path) -> None:
    base = prs.scan_readstat(str(sas_info_nulls), preserve_order=True).collect()
    df = prs.scan_readstat(
        str(sas_info_nulls),
        preserve_order=True,
        informative_nulls=prs.InformativeNullOpts(),
    ).collect()

    inds = _indicator_cols(df)
    assert len(inds) > 0
    assert df.width > base.width


def test_sas_indicator_cols_are_string_type(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(str(sas_info_nulls), informative_nulls={"columns": "all"})
    for col in _indicator_cols(df):
        assert df[col].dtype == pl.String, f"{col} should be String"


def test_sas_indicator_values_have_sas_missing_format(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(str(sas_info_nulls), informative_nulls={"columns": "all"})
    for col in _indicator_cols(df):
        non_null = df[col].drop_nulls()
        for val in non_null:
            assert val.startswith(".") and len(val) == 2, (
                f"unexpected SAS indicator value '{val}' in column '{col}'"
            )


def test_sas_indicator_cols_have_non_null_values(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(str(sas_info_nulls), informative_nulls={"columns": "all"})
    inds = _indicator_cols(df)
    assert _total_non_null(df, inds) > 0, "expected at least one non-null indicator"


def test_sas_read_and_scan_agree(sas_info_nulls: Path) -> None:
    opts = {"columns": "all"}
    df_read = prs.read_readstat(str(sas_info_nulls), informative_nulls=opts)
    df_scan = prs.scan_readstat(
        str(sas_info_nulls), preserve_order=True, informative_nulls=opts
    ).collect()

    assert df_read.columns == df_scan.columns
    assert df_read.shape == df_scan.shape
    assert df_read.equals(df_scan)


def test_sas_merged_string_mode(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(
        str(sas_info_nulls),
        informative_nulls={"columns": "all", "mode": "merged_string"},
    )
    # No _null suffix columns in merged_string mode
    assert _indicator_cols(df) == [], "merged_string mode should not produce _null columns"
    # Tracked numeric columns become String
    base = prs.read_readstat(str(sas_info_nulls))
    numeric_base_cols = [c for c in base.columns if base[c].dtype == pl.Float64]
    for col in numeric_base_cols:
        assert df[col].dtype == pl.String, f"merged_string: {col} should be String"


def test_sas_struct_mode(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(
        str(sas_info_nulls),
        informative_nulls={"columns": "all", "mode": "struct"},
    )
    # No _null suffix columns; tracked columns become Struct
    assert _indicator_cols(df) == [], "struct mode should not produce _null columns"
    struct_cols = [c for c in df.columns if df[c].dtype == pl.Struct]
    assert len(struct_cols) > 0, "struct mode should produce Struct-type columns"


def test_sas_column_selection(sas_info_nulls: Path) -> None:
    base = prs.read_readstat(str(sas_info_nulls))
    if base.width < 2:
        pytest.skip("need at least 2 columns for column selection test")

    tracked_col = base.columns[0]
    df = prs.read_readstat(
        str(sas_info_nulls),
        informative_nulls={"columns": [tracked_col]},
    )
    inds = _indicator_cols(df)
    assert len(inds) == 1, f"expected exactly 1 indicator column, got {inds}"
    assert inds[0] == f"{tracked_col}_null"


def test_sas_custom_suffix(sas_info_nulls: Path) -> None:
    df = prs.read_readstat(
        str(sas_info_nulls),
        informative_nulls={"columns": "all", "suffix": "_missing"},
    )
    inds = _indicator_cols(df, suffix="_missing")
    assert len(inds) > 0, "expected _missing suffix indicator columns"
    assert _indicator_cols(df, suffix="_null") == [], "should not have default _null suffix"


def test_sas_scanreadstat_informative_nulls(sas_info_nulls: Path) -> None:
    reader = prs.ScanReadstat(
        str(sas_info_nulls),
        informative_nulls=prs.InformativeNullOpts(columns="all"),
    )
    schema = reader.schema
    ind_cols = [c for c in schema if c.endswith("_null")]
    assert len(ind_cols) > 0, "ScanReadstat.schema should include indicator columns"

    df = reader.df.collect()
    assert _total_non_null(df, ind_cols) > 0


# ─────────────────────────── Stata ──────────────────────────────────────────


def test_stata_baseline_has_no_indicator_cols(stata_missing: Path) -> None:
    df = prs.read_readstat(str(stata_missing))
    assert _indicator_cols(df) == []


def test_stata_informative_nulls_adds_indicator_cols(stata_missing: Path) -> None:
    base = prs.read_readstat(str(stata_missing))
    df = prs.read_readstat(str(stata_missing), informative_nulls={"columns": "all"})

    inds = _indicator_cols(df)
    assert len(inds) > 0
    assert df.width > base.width


def test_stata_indicator_cols_are_string_type(stata_missing: Path) -> None:
    df = prs.read_readstat(str(stata_missing), informative_nulls={"columns": "all"})
    for col in _indicator_cols(df):
        assert df[col].dtype == pl.String, f"{col} should be String"


def test_stata_indicator_cols_have_non_null_values(stata_missing: Path) -> None:
    df = prs.read_readstat(str(stata_missing), informative_nulls={"columns": "all"})
    inds = _indicator_cols(df)
    assert _total_non_null(df, inds) > 0, "stata8_115.dta should have user-missing indicators"


def test_stata_read_and_scan_agree(stata_missing: Path) -> None:
    opts = {"columns": "all"}
    df_read = prs.read_readstat(str(stata_missing), informative_nulls=opts)
    df_scan = prs.scan_readstat(
        str(stata_missing), preserve_order=True, informative_nulls=opts
    ).collect()

    assert df_read.columns == df_scan.columns
    assert df_read.shape == df_scan.shape
    assert df_read.equals(df_scan)


def test_stata_merged_string_mode(stata_missing: Path) -> None:
    df = prs.read_readstat(
        str(stata_missing),
        informative_nulls={"columns": "all", "mode": "merged_string"},
    )
    assert _indicator_cols(df) == []


def test_stata_struct_mode(stata_missing: Path) -> None:
    df = prs.read_readstat(
        str(stata_missing),
        informative_nulls={"columns": "all", "mode": "struct"},
    )
    assert _indicator_cols(df) == []
    struct_cols = [c for c in df.columns if df[c].dtype == pl.Struct]
    assert len(struct_cols) > 0


# ─────────────────────────── SPSS ───────────────────────────────────────────


def test_spss_simple_alltypes_baseline_has_no_indicator_cols(spss_simple_alltypes: Path) -> None:
    df = prs.read_readstat(str(spss_simple_alltypes))
    assert _indicator_cols(df) == []


def test_spss_simple_alltypes_informative_nulls_adds_cols(spss_simple_alltypes: Path) -> None:
    base = prs.read_readstat(str(spss_simple_alltypes))
    df = prs.read_readstat(str(spss_simple_alltypes), informative_nulls={"columns": "all"})

    inds = _indicator_cols(df)
    assert len(inds) > 0
    assert df.width > base.width


def test_spss_simple_alltypes_indicator_has_non_null_values(spss_simple_alltypes: Path) -> None:
    df = prs.read_readstat(str(spss_simple_alltypes), informative_nulls={"columns": "all"})
    inds = _indicator_cols(df)
    assert _total_non_null(df, inds) > 0


def test_spss_labelled_num_na_has_indicator_for_var00002(spss_labelled_num_na: Path) -> None:
    df = prs.read_readstat(str(spss_labelled_num_na), informative_nulls={"columns": "all"})
    inds = _indicator_cols(df)
    assert len(inds) > 0, "labelled-num-na.sav should have indicator columns for VAR00002"
    assert _total_non_null(df, inds) > 0


def test_spss_read_and_scan_agree(spss_simple_alltypes: Path) -> None:
    opts = {"columns": "all"}
    df_read = prs.read_readstat(str(spss_simple_alltypes), informative_nulls=opts)
    df_scan = prs.scan_readstat(
        str(spss_simple_alltypes), preserve_order=True, informative_nulls=opts
    ).collect()

    assert df_read.columns == df_scan.columns
    assert df_read.shape == df_scan.shape
    assert df_read.equals(df_scan)


def test_spss_merged_string_mode(spss_simple_alltypes: Path) -> None:
    df = prs.read_readstat(
        str(spss_simple_alltypes),
        informative_nulls={"columns": "all", "mode": "merged_string"},
    )
    assert _indicator_cols(df) == []


def test_spss_struct_mode(spss_simple_alltypes: Path) -> None:
    df = prs.read_readstat(
        str(spss_simple_alltypes),
        informative_nulls={"columns": "all", "mode": "struct"},
    )
    assert _indicator_cols(df) == []
    struct_cols = [c for c in df.columns if df[c].dtype == pl.Struct]
    assert len(struct_cols) > 0


# ─────────────────────────── error handling ──────────────────────────────────


def test_bad_mode_raises(sas_info_nulls: Path) -> None:
    with pytest.raises(Exception, match="mode"):
        prs.read_readstat(
            str(sas_info_nulls),
            informative_nulls={"columns": "all", "mode": "bad_mode"},
        )


def test_bad_columns_string_raises(sas_info_nulls: Path) -> None:
    with pytest.raises(Exception, match="columns"):
        prs.read_readstat(
            str(sas_info_nulls),
            informative_nulls={"columns": "some_invalid_string"},
        )
