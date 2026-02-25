from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import polars_readstat as prs


def _col_by_name(metadata: dict, name: str) -> dict:
    columns = metadata.get("columns") or metadata.get("variables") or []
    for col in columns:
        if col.get("name") == name:
            return col
    raise KeyError(f"Column {name!r} not found in metadata")


def _norm_value_labels(labels: dict) -> dict[str, str]:
    """Normalize value label keys to strings for cross-format comparison."""
    return {str(k): str(v) for k, v in labels.items()}


DF = pl.DataFrame({"sex": [1, 2, 1], "score": [10, 20, 30]})
VALUE_LABELS = {"sex": {1: "Male", 2: "Female"}}
VARIABLE_LABELS = {"sex": "Sex of respondent", "score": "Test score"}


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_write_value_labels_roundtrip(tmp_path: Path, fmt: str) -> None:
    out_path = tmp_path / f"labels.{fmt}"
    prs.write_readstat(DF, str(out_path), value_labels=VALUE_LABELS)

    md = prs.ScanReadstat(str(out_path)).metadata
    col = _col_by_name(md, "sex")
    assert col.get("value_labels") is not None, "value_labels missing from metadata"
    assert _norm_value_labels(col["value_labels"]) == {"1": "Male", "2": "Female"}


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_write_variable_labels_roundtrip(tmp_path: Path, fmt: str) -> None:
    out_path = tmp_path / f"varlabels.{fmt}"
    prs.write_readstat(DF, str(out_path), variable_labels=VARIABLE_LABELS)

    md = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(md, "sex")
    score_col = _col_by_name(md, "score")
    assert sex_col.get("label") == "Sex of respondent"
    assert score_col.get("label") == "Test score"


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_write_value_and_variable_labels_roundtrip(tmp_path: Path, fmt: str) -> None:
    out_path = tmp_path / f"both_labels.{fmt}"
    prs.write_readstat(
        DF, str(out_path), value_labels=VALUE_LABELS, variable_labels=VARIABLE_LABELS
    )

    md = prs.ScanReadstat(str(out_path)).metadata
    col = _col_by_name(md, "sex")
    assert col.get("label") == "Sex of respondent"
    assert col.get("value_labels") is not None
    assert _norm_value_labels(col["value_labels"]) == {"1": "Male", "2": "Female"}
