from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pd = pytest.importorskip("pandas")
pyreadstat = pytest.importorskip("pyreadstat")

def _base_frames() -> tuple[pl.DataFrame, "pd.DataFrame"]:
    data = {
        "int_col": [1, -2, 3, 4],
        "float_col": [1.25, -2.5, 3.75, 4.0],
        "str_col": ["alpha", "beta", "gamma", "delta"],
    }
    return pl.DataFrame(data), pd.DataFrame(data)


def _assert_matches_base(base: pl.DataFrame, incoming: pl.DataFrame) -> None:
    assert base.columns == incoming.columns
    assert base.shape == incoming.shape
    assert_frame_equal(
        base,
        incoming,
        check_dtypes=False,
        check_row_order=True,
        check_column_order=True,
        atol=1e-6,
        rtol=1e-6,
    )


@pytest.mark.parametrize(
    "writer, extension",
    [
        ("pandas_dta", "dta"),
        ("pyreadstat_dta", "dta"),
        ("pyreadstat_sav", "sav"),
    ],
)
def test_external_writers_polars_readstat_reads(
    package_module,
    tmp_path: Path,
    writer: str,
    extension: str,
) -> None:
    base_pl, base_pd = _base_frames()
    out_path = tmp_path / f"external_writer.{writer}.{extension}"

    if writer == "pandas_dta":
        base_pd.to_stata(str(out_path), write_index=False)
    elif writer == "pyreadstat_dta":
        pyreadstat.write_dta(base_pd, str(out_path))
    elif writer == "pyreadstat_sav":
        pyreadstat.write_sav(base_pd, str(out_path))
    else:
        raise AssertionError(f"Unknown writer: {writer}")

    read_df = package_module.read_readstat(str(out_path))
    _assert_matches_base(base_pl, read_df)


@pytest.mark.parametrize(
    "reader, extension",
    [
        ("pandas_dta", "dta"),
        ("pyreadstat_dta", "dta"),
        ("pandas_sav", "sav"),
        ("pyreadstat_sav", "sav"),
    ],
)
def test_polars_readstat_writes_external_reads(
    package_module,
    tmp_path: Path,
    reader: str,
    extension: str,
) -> None:
    base_pl, _ = _base_frames()
    out_path = tmp_path / f"polars_readstat_writer.{reader}.{extension}"

    package_module.write_readstat(base_pl, str(out_path), format=extension)

    if reader == "pandas_dta":
        read_df = pd.read_stata(str(out_path), convert_categoricals=False)
    elif reader == "pyreadstat_dta":
        read_df, _ = pyreadstat.read_dta(str(out_path))
    elif reader == "pandas_sav":
        read_df = pd.read_spss(str(out_path))
    elif reader == "pyreadstat_sav":
        read_df, _ = pyreadstat.read_sav(str(out_path))
    else:
        raise AssertionError(f"Unknown reader: {reader}")

    incoming = pl.from_pandas(read_df, include_index=False)
    _assert_matches_base(base_pl, incoming)
