from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

INT_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


def test_write_stata_compress_respects_stata_integer_bounds(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame(
        {
            "inside_bounds_float": [-2_147_483_647.0, 0.0, 2_147_483_620.0],
            "outside_bounds_float": [-2_147_483_648.0, 1.0, 2_147_483_647.0],
        }
    )
    out_path = tmp_path / "stata_bounds_compress.dta"
    package_module.write_readstat(df, str(out_path), format="dta", compress=True)

    roundtrip = package_module.read_readstat(str(out_path))

    assert roundtrip.schema["inside_bounds_float"] in INT_DTYPES
    assert roundtrip.schema["outside_bounds_float"] == pl.Float64


@pytest.mark.parametrize("target_fmt", ["dta", "sav"])
def test_read_compress_roundtrip_for_float_and_string_columns(
    package_module,
    tmp_path: Path,
    target_fmt: str,
) -> None:
    df = pl.DataFrame(
        {
            "float_intlike": [1.0, 2.0, 120.0, None],
            "numeric_string": ["1", "2", "120", None],
        }
    )

    out_path = tmp_path / f"read_compress_case.{target_fmt}"
    if target_fmt == "dta":
        package_module.write_readstat(df, str(out_path), format=target_fmt, compress=False)
    else:
        package_module.write_readstat(df, str(out_path), format=target_fmt)

    compressed = package_module.read_readstat(
        str(out_path),
        compress={
            "enabled": True,
            "compress_numeric": True,
            "string_to_numeric": True,
        },
    )

    assert compressed.schema["float_intlike"] in INT_DTYPES
    assert compressed.schema["numeric_string"] in INT_DTYPES


def test_scan_compress_honors_infer_compress_length_limit(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"num_as_string": ["1", "2", "not_a_number"]})
    out_path = tmp_path / "infer_limit.sav"
    package_module.write_readstat(df, str(out_path), format="sav")

    limited_probe = package_module.scan_readstat(
        str(out_path),
        compress={
            "enabled": True,
            "compress_numeric": True,
            "string_to_numeric": True,
            "infer_compress_length": 2,
        },
        preserve_order=True,
    ).collect(engine="streaming")

    full_probe = package_module.scan_readstat(
        str(out_path),
        compress={
            "enabled": True,
            "compress_numeric": True,
            "string_to_numeric": True,
        },
        preserve_order=True,
    ).collect(engine="streaming")

    assert limited_probe.schema["num_as_string"] in INT_DTYPES
    assert limited_probe["num_as_string"].to_list() == [1, 2, None]
    assert full_probe.schema["num_as_string"] == pl.String


def test_compress_options_are_opt_in_via_enabled_flag(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame(
        {
            "float_intlike": [1.0, 2.0, 120.0, None],
            "numeric_string": ["1", "2", "120", None],
        }
    )
    out_path = tmp_path / "compress_opt_in.sav"
    package_module.write_readstat(df, str(out_path), format="sav")

    not_enabled = package_module.read_readstat(
        str(out_path),
        compress={
            "compress_numeric": True,
            "string_to_numeric": True,
        },
    )

    assert not_enabled.schema["float_intlike"] == pl.Float64
    assert not_enabled.schema["numeric_string"] == pl.String


def test_scan_accepts_compress_and_exposes_compressed_schema_pre_scan(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame(
        {
            "float_intlike": [1.0, 2.0, 120.0, None],
            "numeric_string": ["1", "2", "120", None],
        }
    )
    out_path = tmp_path / "scan_pre_schema.sav"
    package_module.write_readstat(df, str(out_path), format="sav")

    lf = package_module.scan_readstat(
        str(out_path),
        compress={
            "enabled": True,
            "compress_numeric": True,
            "string_to_numeric": True,
        },
        preserve_order=True,
    )

    schema = lf.collect_schema()
    assert schema["float_intlike"] in INT_DTYPES
    assert schema["numeric_string"] in INT_DTYPES
