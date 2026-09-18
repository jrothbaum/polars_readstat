from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import polars_readstat as prs

pyreadstat = pytest.importorskip("pyreadstat")


def test_missing_ranges_metadata_capture(tmp_path: Path) -> None:
    """Discrete + range missing values are written, then show up in metadata_df."""
    df = pl.DataFrame({"id": [1, 2, 3, 4, 5], "age": [25.0, -1.0, 40.0, 999.0, 30.0]})
    out = tmp_path / "capture.sav"
    prs.write_readstat(df, str(out), missing_ranges={"age": [{"lo": -1, "hi": 0}, 999]})

    reader = prs.ScanReadstat(str(out))
    mdf = reader.metadata_df.select("name", "missing_discrete", "missing_range_lo", "missing_range_hi")
    row = mdf.filter(pl.col("name") == "age").to_dicts()[0]
    assert row["missing_discrete"] == ["999"]
    assert row["missing_range_lo"] == -1.0
    assert row["missing_range_hi"] == 0.0

    id_row = mdf.filter(pl.col("name") == "id").to_dicts()[0]
    assert id_row["missing_discrete"] is None
    assert id_row["missing_range_lo"] is None

    # Declared missing values are nulled on a plain read.
    data = reader.df.collect()
    assert data["age"].to_list() == [25.0, None, 40.0, None, 30.0]

    # And pyreadstat agrees on the file-level definition.
    _pdf, meta = pyreadstat.read_sav(str(out), user_missing=True)
    ranges = meta.missing_ranges["age"]
    assert {"lo": -1.0, "hi": 0.0} in ranges
    assert any(r["lo"] == r["hi"] == 999.0 for r in ranges)


def test_missing_ranges_discrete_only(tmp_path: Path) -> None:
    df = pl.DataFrame({"q": [1.0, 2.0, 98.0, 99.0]})
    out = tmp_path / "discrete.sav"
    prs.write_readstat(df, str(out), missing_ranges={"q": [98, 99]})

    reader = prs.ScanReadstat(str(out))
    assert reader.df.collect()["q"].to_list() == [1.0, 2.0, None, None]

    _pdf, meta = pyreadstat.read_sav(str(out), user_missing=True)
    ranges = meta.missing_ranges["q"]
    assert any(r["lo"] == r["hi"] == 98.0 for r in ranges)
    assert any(r["lo"] == r["hi"] == 99.0 for r in ranges)


@pytest.mark.parametrize(
    "bad_ranges",
    [
        {"q": [1, 2, 3, 4]},  # too many discrete
        {"q": [{"lo": 0, "hi": 1}, 2, 3]},  # range + 2 discrete
        {"q": [{"lo": 0, "hi": 1}, {"lo": 2, "hi": 3}]},  # two ranges
    ],
)
def test_missing_ranges_validation_errors(tmp_path: Path, bad_ranges: dict) -> None:
    df = pl.DataFrame({"q": [1.0, 2.0, 3.0]})
    out = tmp_path / "bad.sav"
    with pytest.raises(ValueError):
        prs.write_readstat(df, str(out), missing_ranges=bad_ranges)


def test_informative_nulls_struct_mode_roundtrip(tmp_path: Path) -> None:
    """scan(informative_nulls=struct) -> write(merge_informative_nulls=True) round-trips
    both discrete and range user-missing values, including unlabeled range values that
    previously fell back to a generic 'MISSING' indicator text."""
    df = pl.DataFrame({"id": [1, 2, 3, 4, 5], "age": [25.0, -1.0, 40.0, 999.0, 30.0]})
    src = tmp_path / "src.sav"
    prs.write_readstat(df, str(src), missing_ranges={"age": [{"lo": -1, "hi": 0}, 999]})

    reader = prs.ScanReadstat(str(src), informative_nulls=prs.InformativeNullOpts(mode="struct"))
    scanned = reader.df.collect()
    assert scanned.schema["age"] == pl.Struct({"age": pl.Float64, "null_indicator": pl.String})

    dst = tmp_path / "dst.sav"
    prs.write_readstat(scanned, str(dst), metadata=reader.metadata_df, merge_informative_nulls=True)

    pdf, meta = pyreadstat.read_sav(str(dst), user_missing=True)
    assert pdf["age"].tolist() == [25.0, -1.0, 40.0, 999.0, 30.0]
    ranges = meta.missing_ranges["age"]
    assert {"lo": -1.0, "hi": 0.0} in ranges
    assert any(r["lo"] == r["hi"] == 999.0 for r in ranges)

    # A plain re-read (no informative_nulls) nulls the declared missing values again.
    reread = prs.ScanReadstat(str(dst)).df.collect()
    assert reread["age"].to_list() == [25.0, None, 40.0, None, 30.0]


def test_informative_nulls_separate_column_mode_roundtrip(tmp_path: Path) -> None:
    """SeparateColumn-mode pairing is captured in metadata_df's informative_null_indicator
    column at scan time and used automatically at write time — never inferred from names."""
    df = pl.DataFrame({"id": [1, 2, 3, 4], "q": [1.0, 2.0, 98.0, 99.0]})
    src = tmp_path / "src2.sav"
    prs.write_readstat(
        df, str(src),
        missing_ranges={"q": [98, 99]},
        value_labels={"q": {98: "Refused", 99: "DK"}},
    )

    reader = prs.ScanReadstat(str(src), informative_nulls=prs.InformativeNullOpts(mode="separate_column"))
    scanned = reader.df.collect()
    assert "q_null" in scanned.columns

    indicator_row = reader.metadata_df.filter(pl.col("name") == "q").to_dicts()[0]
    assert indicator_row["informative_null_indicator"] == "q_null"

    dst = tmp_path / "dst2.sav"
    prs.write_readstat(scanned, str(dst), metadata=reader.metadata_df, merge_informative_nulls=True)

    assert "q_null" not in prs.ScanReadstat(str(dst)).schema

    pdf, meta = pyreadstat.read_sav(str(dst), user_missing=True)
    assert pdf["q"].tolist() == [1.0, 2.0, 98.0, 99.0]
    ranges = meta.missing_ranges["q"]
    assert any(r["lo"] == r["hi"] == 98.0 for r in ranges)
    assert any(r["lo"] == r["hi"] == 99.0 for r in ranges)


def test_informative_null_pairs_explicit(tmp_path: Path) -> None:
    """Hand-built value/indicator column pairs can be merged without a metadata_df,
    via the explicit informative_null_pairs mapping."""
    df = pl.DataFrame({
        "q": [1.0, 2.0, None, None],
        "q_reason": [None, None, "98", "99"],
    })
    out = tmp_path / "explicit.sav"
    prs.write_readstat(
        df, str(out),
        missing_ranges={"q": [98, 99]},
        informative_null_pairs={"q": "q_reason"},
    )

    assert "q_reason" not in prs.ScanReadstat(str(out)).schema
    pdf, meta = pyreadstat.read_sav(str(out), user_missing=True)
    assert pdf["q"].tolist() == [1.0, 2.0, 98.0, 99.0]
