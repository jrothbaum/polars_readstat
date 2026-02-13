from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


def test_write_readstat_rejects_unknown_extension(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    out_path = tmp_path / "bad.random_ext"

    with pytest.raises(ValueError, match="Unsupported output format"):
        package_module.write_readstat(df, str(out_path))


def test_write_stata_rejects_unknown_extension(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    out_path = tmp_path / "bad.random_ext"

    with pytest.raises(ValueError, match="unknown file extension"):
        package_module.write_stata(df, str(out_path), None, None, None, None)


def test_write_spss_rejects_unknown_extension(
    package_module,
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    out_path = tmp_path / "bad.random_ext"

    with pytest.raises(ValueError, match="unknown file extension"):
        package_module.write_spss(df, str(out_path), None, None)
