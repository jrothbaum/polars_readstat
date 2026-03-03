from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

SAS_DATE_OFFSET_DAYS = 3653
SECONDS_PER_DAY = 86_400


def _expected_sas_csv_payload(df: pl.DataFrame) -> pl.DataFrame:
    # Match Python-side write normalization.
    df = df.with_columns(
        pl.col(pl.UInt8).cast(pl.Int16),
        pl.col(pl.UInt16).cast(pl.Int32),
    )

    exprs: list[pl.Expr] = []
    for name, dtype in df.schema.items():
        if dtype == pl.Date:
            exprs.append((pl.col(name).cast(pl.Int32) + SAS_DATE_OFFSET_DAYS).alias(name))
            continue

        if dtype.base_type() == pl.Datetime:
            if dtype.time_unit == "ms":
                secs = pl.col(name).cast(pl.Int64).floordiv(1_000)
            elif dtype.time_unit == "us":
                secs = pl.col(name).cast(pl.Int64).floordiv(1_000_000)
            elif dtype.time_unit == "ns":
                secs = pl.col(name).cast(pl.Int64).floordiv(1_000_000_000)
            else:
                raise AssertionError(f"Unsupported Datetime unit: {dtype.time_unit}")
            exprs.append((secs + SAS_DATE_OFFSET_DAYS * SECONDS_PER_DAY).alias(name))
            continue

        if dtype == pl.Time:
            exprs.append(pl.col(name).cast(pl.Int64).floordiv(1_000_000_000).alias(name))
            continue

        exprs.append(pl.col(name))

    return df.select(exprs)


def test_write_sas_csv_import_outputs_bundle(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"num": [1, 2, 3], "name": ["a", "bb", "ccc"]})
    out_stem = tmp_path / "sas_bundle"

    csv_path, sas_path = package_module.write_sas_csv_import(
        df,
        str(out_stem),
        dataset_name="demo",
        value_labels={"num": {1: "one", 2: "two"}},
        variable_labels={"num": "Numeric Label"},
    )

    csv_file = Path(csv_path)
    sas_file = Path(sas_path)

    assert csv_file.exists(), "CSV output was not written"
    assert sas_file.exists(), "SAS script output was not written"

    sas_text = sas_file.read_text(encoding="utf-8")
    assert "data demo;" in sas_text
    assert "proc format;" in sas_text
    assert "Numeric Label" in sas_text


def test_write_readstat_sas_points_to_explicit_api(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    out_path = tmp_path / "out.sas"

    with pytest.raises(NotImplementedError, match="write_sas_csv_import"):
        package_module.write_readstat(df, str(out_path), format="sas")


def test_write_sas_csv_import_csv_matches_source_with_expected_transforms(
    package_module,
    sas_fixture: Path,
    tmp_path: Path,
) -> None:
    source = package_module.scan_readstat(
        str(sas_fixture),
        preserve_order=True,
        missing_string_as_null=False,
        value_labels_as_strings=False,
    ).collect(engine="streaming")

    csv_path, _ = package_module.write_sas_csv_import(
        source,
        str(tmp_path / "sas_bundle_from_fixture"),
        dataset_name="fixture_data",
    )

    actual_csv = pl.read_csv(csv_path)
    expected_csv = _expected_sas_csv_payload(source)

    assert_frame_equal(
        expected_csv,
        actual_csv,
        check_dtypes=False,
        check_row_order=True,
        check_column_order=True,
        atol=1e-6,
        rtol=1e-6,
    )
