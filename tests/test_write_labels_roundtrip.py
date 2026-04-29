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


def test_write_readstat_metadata_param_stata(tmp_path: Path) -> None:
    """write_readstat(metadata=...) roundtrips Stata metadata via the doc example pattern."""
    source_path = tmp_path / "source.dta"
    out_path = tmp_path / "out.dta"
    prs.write_readstat(
        DF,
        str(source_path),
        value_labels=VALUE_LABELS,
        variable_labels=VARIABLE_LABELS,
        variable_format={"score": "%12.2f"},
    )

    reader = prs.ScanReadstat(str(source_path))
    df = reader.df.collect()
    prs.write_readstat(df, str(out_path), metadata=reader.metadata)

    roundtrip = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(roundtrip, "sex")
    score_col = _col_by_name(roundtrip, "score")
    assert _norm_value_labels(sex_col["value_labels"]) == {"1": "Male", "2": "Female"}
    assert sex_col.get("label") == "Sex of respondent"
    assert score_col.get("label") == "Test score"
    assert score_col.get("format") == "%12.2f"


def test_write_readstat_metadata_param_spss(tmp_path: Path) -> None:
    """write_readstat(metadata=...) roundtrips SPSS metadata via the doc example pattern."""
    source_path = tmp_path / "source.sav"
    out_path = tmp_path / "out.sav"
    prs.write_readstat(
        DF,
        str(source_path),
        value_labels=VALUE_LABELS,
        variable_labels=VARIABLE_LABELS,
        variable_measure={"sex": "nominal"},
        variable_display_width={"sex": 12},
        variable_alignment={"sex": "center"},
        variable_format={"score": "F10.3"},
    )

    reader = prs.ScanReadstat(str(source_path))
    df = reader.df.collect()
    prs.write_readstat(df, str(out_path), metadata=reader.metadata)

    roundtrip = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(roundtrip, "sex")
    score_col = _col_by_name(roundtrip, "score")
    assert _norm_value_labels(sex_col["value_labels"]) == {"1": "Male", "2": "Female"}
    assert sex_col.get("label") == "Sex of respondent"
    assert sex_col.get("measure") == "Nominal"
    assert sex_col.get("display_width") == 12
    assert sex_col.get("alignment") == "Center"
    assert score_col.get("format_width") == 10
    assert score_col.get("decimal_places") == 3


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_write_metadata_drop_variable_roundtrip(tmp_path: Path, fmt: str) -> None:
    """Read a file, drop a variable, write with the original full metadata, then verify
    that the retained variable's metadata roundtrips and the dropped variable is absent."""
    source_path = tmp_path / f"source.{fmt}"
    out_path = tmp_path / f"out_subset.{fmt}"

    # Write a source file with rich metadata on both columns
    spss_extra = dict(
        variable_measure={"sex": "nominal", "score": "scale"},
        variable_display_width={"sex": 12, "score": 10},
        variable_alignment={"sex": "center", "score": "right"},
    ) if fmt == "sav" else {}
    prs.write_readstat(
        DF,
        str(source_path),
        value_labels=VALUE_LABELS,
        variable_labels=VARIABLE_LABELS,
        variable_format={"score": "%12.2f"} if fmt == "dta" else {"score": "F10.3"},
        **spss_extra,
    )

    # Read it back, drop "sex", then write with the unmodified full metadata
    reader = prs.ScanReadstat(str(source_path))
    df_subset = reader.df.select(["score"]).collect()
    prs.write_readstat(df_subset, str(out_path), metadata=reader.metadata)

    roundtrip_md = prs.ScanReadstat(str(out_path)).metadata
    variables = roundtrip_md.get("variables") or roundtrip_md.get("columns") or []
    variable_names = [v["name"] for v in variables]

    assert variable_names == ["score"], f"expected only 'score', got {variable_names}"
    score_col = _col_by_name(roundtrip_md, "score")
    assert score_col.get("label") == "Test score"
    if fmt == "dta":
        assert score_col.get("format") == "%12.2f"
    else:
        assert score_col.get("format_width") == 10
        assert score_col.get("decimal_places") == 3


def test_write_readstat_explicit_kwargs_override_metadata(tmp_path: Path) -> None:
    """Explicit kwargs take precedence over metadata-derived values."""
    source_path = tmp_path / "source.dta"
    out_path = tmp_path / "out.dta"
    prs.write_readstat(DF, str(source_path), variable_labels={"sex": "Original label"})

    reader = prs.ScanReadstat(str(source_path))
    df = reader.df.collect()
    prs.write_readstat(df, str(out_path), metadata=reader.metadata, variable_labels={"sex": "Overridden label"})

    roundtrip = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(roundtrip, "sex")
    assert sex_col.get("label") == "Overridden label"


def test_write_stata_combined_params(tmp_path: Path) -> None:
    """Matches the combined Stata example in the write docs."""
    out_path = tmp_path / "stata_combined.dta"
    prs.write_readstat(
        DF,
        str(out_path),
        value_labels=VALUE_LABELS,
        variable_labels=VARIABLE_LABELS,
        variable_format={"score": "%12.2f"},
        compress=True,
    )

    md = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(md, "sex")
    score_col = _col_by_name(md, "score")

    assert _norm_value_labels(sex_col["value_labels"]) == {"1": "Male", "2": "Female"}
    assert sex_col.get("label") == "Sex of respondent"
    assert score_col.get("label") == "Test score"
    assert score_col.get("format") == "%12.2f"


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
