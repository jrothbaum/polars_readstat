from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


def test_sink_readstat_stata_streaming_roundtrip(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["a", "bb", "ccc", "dddd", "eeeee"],
        }
    )
    lf = df.lazy().select(["id", "name"])
    out_path = tmp_path / "sink_streaming.dta"

    package_module.sink_readstat(
        lf,
        str(out_path),
        format="dta",
        batch_size=2,
    )

    roundtrip = package_module.read_readstat(str(out_path))
    assert roundtrip["id"].to_list() == [1, 2, 3, 4, 5]
    assert roundtrip["name"].to_list() == ["a", "bb", "ccc", "dddd", "eeeee"]


def test_sink_readstat_spss_warns_not_streamed(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"x": [1, 2, 3], "s": ["a", "b", "c"]})
    lf = df.lazy()
    out_path = tmp_path / "sink_non_streaming.sav"

    with pytest.warns(RuntimeWarning, match="not streamed"):
        package_module.sink_readstat(lf, str(out_path), format="sav")

    roundtrip = package_module.read_readstat(str(out_path))
    assert roundtrip.shape == df.shape


def test_sink_readstat_stata_handles_strl_required_data_path(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"txt": ["a ", "b"]})
    lf = df.lazy()
    out_path = tmp_path / "sink_reject_strl.dta"

    # If Rust streaming is active this should fail (strL required).
    # If Python/Rust LazyFrame versions are mismatched, sink_readstat falls back
    # to materialized write and this succeeds.
    try:
        package_module.sink_readstat(lf, str(out_path), format="dta")
    except ValueError as err:
        assert "strL" in str(err)
        return

    roundtrip = package_module.read_readstat(str(out_path))
    assert roundtrip["txt"].to_list() == ["a ", "b"]
