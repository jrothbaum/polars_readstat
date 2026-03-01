from __future__ import annotations

from pathlib import Path

import pytest


BIG_FIXTURES = [
    (
        Path(
            "/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/sas/data/too_big/numeric_1000000_2.sas7bdat"
        ),
        50_000,
    ),
    (
        Path(
            "/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/spss/data/too_big/GSS2024.sav"
        ),
        50_000,
    ),
    (
        Path(
            "/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/stata/data/too_big/usa_00009.dta"
        ),
        50_000,
    ),
]


@pytest.mark.parametrize("path,batch_size", BIG_FIXTURES)
def test_preserve_order_row_index_and_sort_big_files(
    package_module,
    path: Path,
    batch_size: int,
) -> None:
    if not path.exists():
        pytest.skip(f"Missing fixture: {path}")

    row_col = "__row_idx"
    head_rows = batch_size * 3

    df_row = (
        package_module.scan_readstat(
            str(path),
            threads=4,
            batch_size=batch_size,
            preserve_order={"mode": "row_index", "row_index_name": row_col},
        )
        .head(head_rows)
        .collect()
    )

    assert row_col in df_row.columns
    series = df_row[row_col]
    assert series.n_unique() == df_row.height
    assert series.min() == 0
    assert series.max() == df_row.height - 1

    df_sorted = (
        package_module.scan_readstat(
            str(path),
            threads=4,
            batch_size=batch_size,
            preserve_order={"mode": "sort", "row_index_name": row_col},
        )
        .head(head_rows)
        .collect()
    )

    assert row_col not in df_sorted.columns

    df_buffered = (
        package_module.scan_readstat(
            str(path),
            threads=4,
            batch_size=batch_size,
            preserve_order=True,
        )
        .head(head_rows)
        .collect()
    )
    assert df_sorted.equals(df_buffered)
