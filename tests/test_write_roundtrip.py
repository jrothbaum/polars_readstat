from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

ROUNDTRIP_SOURCE_FILES = [
    "sas/data/data/file1.sas7bdat",
    "sas/data/data_pandas/test1.sas7bdat",
    "stata/data/sample.dta",
    "stata/data/sample_pyreadstat.dta",
    "spss/data/sample.sav",
    "spss/data/simple_alltypes.sav",
]

WRITE_FORMATS = ["dta", "sav"]

SIGNED_INT_DTYPES = {pl.Int8, pl.Int16, pl.Int32, pl.Int64}
UNSIGNED_INT_DTYPES = {pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
FLOAT_DTYPES = {pl.Float32, pl.Float64}


def _normalize_for_compare(df: pl.DataFrame) -> pl.DataFrame:
    exprs: list[pl.Expr] = []
    for name, dtype in df.schema.items():
        if dtype in SIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Int64))
        elif dtype in UNSIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.UInt64))
        elif dtype in FLOAT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Float64))
        elif dtype == pl.String:
            # Ignore writer-dependent trailing NUL padding.
            exprs.append(pl.col(name).str.replace_all(r"\x00+$", ""))
        else:
            exprs.append(pl.col(name))
    return df.select(exprs)


@pytest.mark.parametrize("source_rel", ROUNDTRIP_SOURCE_FILES)
@pytest.mark.parametrize("target_fmt", WRITE_FORMATS)
def test_write_roundtrip_many_files(
    package_module,
    rs_tests_root: Path,
    tmp_path: Path,
    source_rel: str,
    target_fmt: str,
) -> None:
    source_path = rs_tests_root / source_rel
    if not source_path.exists():
        pytest.skip(f"Missing fixture: {source_path}")

    df = package_module.scan_readstat(
        str(source_path),
        preserve_order=True,
        missing_string_as_null=False,
        value_labels_as_strings=False,
    ).collect(engine="streaming")

    if df.width == 0:
        pytest.skip(f"Skipping zero-column fixture: {source_path}")

    out_path = tmp_path / f"{source_path.stem}.roundtrip.{target_fmt}"
    package_module.write_readstat(df, str(out_path), format=target_fmt)

    roundtrip = package_module.scan_readstat(
        str(out_path),
        preserve_order=True,
        missing_string_as_null=False,
        value_labels_as_strings=False,
    ).collect(engine="streaming")

    assert df.shape == roundtrip.shape, f"Shape mismatch for {source_rel} -> {target_fmt}"
    assert df.columns == roundtrip.columns, f"Column mismatch for {source_rel} -> {target_fmt}"

    assert_frame_equal(
        _normalize_for_compare(df),
        _normalize_for_compare(roundtrip),
        check_dtypes=False,
        check_row_order=True,
        check_column_order=True,
        atol=1e-6,
        rtol=1e-6,
    )
