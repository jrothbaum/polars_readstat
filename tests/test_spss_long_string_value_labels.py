from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Any

import polars_readstat as prs


def _run_generator(tmp_path: Path, variant: str) -> Path:
    script = Path(__file__).with_name("generate_long_string_value_labels.py")
    out_path = tmp_path / f"long_string_value_labels_{variant}.sav"

    subprocess.run(
        ["uv", "run", str(script), "--out", str(out_path), "--variant", variant],
        check=True,
    )
    return out_path


def _get_var(metadata: dict[str, Any], name: str) -> dict[str, Any] | None:
    vars_list = metadata.get("variables") or metadata.get("columns") or []
    name_lower = name.lower()
    for var in vars_list:
        var_name = str(var.get("name", "")).lower()
        if var_name == name_lower:
            return var
    return None


def _assert_value_label(metadata: dict[str, Any], name: str) -> None:
    var = _get_var(metadata, name)
    assert var is not None, f"missing variable {name} in metadata"
    assert var.get("value_label"), f"missing value_label for {name}"


def test_spss_long_string_value_labels_basic(tmp_path: Path) -> None:
    out_path = _run_generator(tmp_path, "basic")

    df = prs.scan_readstat(str(out_path)).collect()
    reader = prs.ScanReadstat(path=str(out_path))
    metadata = reader.metadata
    df2 = reader.df.collect()

    assert df.height == 3
    assert df2.shape == df.shape
    assert metadata
    _assert_value_label(metadata, "longstr")


def test_spss_long_string_value_labels_multiple_entries(tmp_path: Path) -> None:
    out_path = _run_generator(tmp_path, "multi")

    df = prs.scan_readstat(str(out_path)).collect()
    metadata = prs.ScanReadstat(path=str(out_path)).metadata

    assert df.height == 5
    assert df.width >= 2
    _assert_value_label(metadata, "longstr")


def test_spss_long_string_value_labels_multiple_vars(tmp_path: Path) -> None:
    out_path = _run_generator(tmp_path, "multi-vars")

    df = prs.scan_readstat(str(out_path)).collect()
    metadata = prs.ScanReadstat(path=str(out_path)).metadata

    assert df.height == 3
    assert df.width >= 3
    _assert_value_label(metadata, "longstr_a")
    _assert_value_label(metadata, "longstr_b")


def test_spss_long_string_value_labels_unicode(tmp_path: Path) -> None:
    out_path = _run_generator(tmp_path, "unicode")

    df = prs.scan_readstat(str(out_path)).collect()
    metadata = prs.ScanReadstat(path=str(out_path)).metadata

    assert df.height == 3
    assert df.width >= 2
    _assert_value_label(metadata, "longstr")
