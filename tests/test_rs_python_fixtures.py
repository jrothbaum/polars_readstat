from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


@pytest.mark.parametrize(
    "fixture_name, expected_shape",
    [
        ("sas_fixture", (10, 100)),
        ("stata_fixture", (30, 1)),
        ("spss_fixture", (5, 7)),
    ],
)
def test_scan_and_read_match_known_rs_fixtures(
    request: pytest.FixtureRequest,
    package_module,
    fixture_name: str,
    expected_shape: tuple[int, int],
) -> None:
    path: Path = request.getfixturevalue(fixture_name)

    df_scan = package_module.scan_readstat(str(path), preserve_order=True).collect()
    df_read = package_module.read_readstat(str(path))

    assert df_scan.shape == expected_shape
    assert df_read.shape == expected_shape
    assert df_scan.columns == df_read.columns
    assert df_scan.equals(df_read)


@pytest.mark.parametrize("fixture_name", ["sas_fixture", "stata_fixture", "spss_fixture"])
def test_scanreadstat_exposes_schema_and_metadata(
    request: pytest.FixtureRequest,
    package_module,
    fixture_name: str,
) -> None:
    path: Path = request.getfixturevalue(fixture_name)

    reader = package_module.ScanReadstat(path=str(path), preserve_order=True)
    schema = reader.schema
    metadata = reader.metadata
    head_df = reader.df.head(3).collect()

    assert isinstance(schema, (dict, pl.Schema))
    assert isinstance(metadata, dict)
    assert len(schema) > 0
    assert metadata
    assert head_df.height <= 3
    assert head_df.width == len(schema)


def test_columns_projection_uses_rs_sas_fixture(sas_fixture: Path, package_module) -> None:
    base = package_module.read_readstat(str(sas_fixture))
    cols = base.columns[:3]

    df = package_module.scan_readstat(
        str(sas_fixture),
        columns=cols,
        preserve_order=True,
    ).collect()

    assert df.columns == cols
    assert df.shape[0] == base.shape[0]
    assert df.shape[1] == len(cols)
