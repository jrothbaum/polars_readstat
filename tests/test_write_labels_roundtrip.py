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


def test_write_spss_variable_metadata_roundtrip(tmp_path: Path) -> None:
    out_path = tmp_path / "spss_variable_metadata.sav"
    prs.write_readstat(
        DF,
        str(out_path),
        variable_measure={"sex": "nominal", "score": "scale"},
        variable_alignment={"sex": "center"},
        variable_display_width={"sex": 11},
        variable_format={"score": "F10.3"},
    )

    md = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(md, "sex")
    score_col = _col_by_name(md, "score")

    assert sex_col.get("measure") == "Nominal"
    assert sex_col.get("alignment") == "Center"
    assert sex_col.get("display_width") == 11
    assert score_col.get("format_width") == 10
    assert score_col.get("decimal_places") == 3


def test_spss_variable_metadata_to_write_kwargs(tmp_path: Path) -> None:
    source_path = tmp_path / "source_metadata.sav"
    out_path = tmp_path / "converted_metadata.sav"
    prs.write_readstat(
        DF,
        str(source_path),
        format="sav",
        value_labels=VALUE_LABELS,
        variable_labels=VARIABLE_LABELS,
        variable_measure={"sex": "nominal", "score": "scale"},
        variable_display_width={"sex": 12, "score": 10},
        variable_alignment={"sex": "center", "score": "right"},
        variable_format={"score": "F10.3"},
    )

    metadata = prs.ScanReadstat(str(source_path)).metadata
    write_kwargs = prs.spss_variable_metadata_to_write_kwargs(metadata["variables"])
    prs.write_readstat(DF, str(out_path), format="sav", **write_kwargs)

    roundtrip = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(roundtrip, "sex")
    score_col = _col_by_name(roundtrip, "score")

    assert sex_col.get("label") == "Sex of respondent"
    assert _norm_value_labels(sex_col["value_labels"]) == {"1": "Male", "2": "Female"}
    assert sex_col.get("measure") == "Nominal"
    assert sex_col.get("display_width") == 12
    assert sex_col.get("alignment") == "Center"
    assert score_col.get("label") == "Test score"
    assert score_col.get("format_width") == 10
    assert score_col.get("decimal_places") == 3


def test_write_stata_variable_format_roundtrip(tmp_path: Path) -> None:
    out_path = tmp_path / "stata_variable_format.dta"
    prs.write_readstat(
        DF,
        str(out_path),
        format="dta",
        variable_format={"score": "%12.2f"},
    )

    md = prs.ScanReadstat(str(out_path)).metadata
    score_col = _col_by_name(md, "score")
    assert score_col.get("format") == "%12.2f"


def test_stata_variable_metadata_to_write_kwargs(tmp_path: Path) -> None:
    source_path = tmp_path / "source_metadata.dta"
    out_path = tmp_path / "converted_metadata.dta"
    prs.write_readstat(
        DF,
        str(source_path),
        format="dta",
        value_labels=VALUE_LABELS,
        variable_labels=VARIABLE_LABELS,
        variable_format={"score": "%12.2f"},
    )

    metadata = prs.ScanReadstat(str(source_path)).metadata
    write_kwargs = prs.stata_variable_metadata_to_write_kwargs(metadata["variables"])
    prs.write_readstat(DF, str(out_path), format="dta", **write_kwargs)

    roundtrip = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(roundtrip, "sex")
    score_col = _col_by_name(roundtrip, "score")

    assert sex_col.get("label") == "Sex of respondent"
    assert _norm_value_labels(sex_col["value_labels"]) == {"1": "Male", "2": "Female"}
    assert score_col.get("label") == "Test score"
    assert score_col.get("format") == "%12.2f"
