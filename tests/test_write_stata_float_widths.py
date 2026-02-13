from __future__ import annotations

from pathlib import Path

import polars as pl


def test_write_stata_preserves_float32(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"f32": pl.Series("f32", [1.25, 2.5], dtype=pl.Float32)})
    out_path = tmp_path / "float32.dta"

    package_module.write_readstat(df, str(out_path), format="dta")

    roundtrip = package_module.read_readstat(str(out_path))
    assert roundtrip.schema["f32"] == pl.Float32


def test_write_stata_preserves_float64(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"f64": pl.Series("f64", [1.25, 2.5], dtype=pl.Float64)})
    out_path = tmp_path / "float64.dta"

    package_module.write_readstat(df, str(out_path), format="dta")

    roundtrip = package_module.read_readstat(str(out_path))
    assert roundtrip.schema["f64"] == pl.Float64
