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
    prs.write_readstat(DF, str(out_path), format="sav", metadata=metadata)

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
    prs.write_readstat(DF, str(out_path), format="dta", metadata=metadata)

    roundtrip = prs.ScanReadstat(str(out_path)).metadata
    sex_col = _col_by_name(roundtrip, "sex")
    score_col = _col_by_name(roundtrip, "score")

    assert sex_col.get("label") == "Sex of respondent"
    assert _norm_value_labels(sex_col["value_labels"]) == {"1": "Male", "2": "Female"}
    assert score_col.get("label") == "Test score"
    assert score_col.get("format") == "%12.2f"


# ─────────────────────────────────────────────────────────────────────────────
# metadata_df roundtrip tests
# ─────────────────────────────────────────────────────────────────────────────


def _write_metadata_df_source(tmp_path: Path, fmt: str) -> tuple[Path, str, str]:
    source = tmp_path / f"source.{fmt}"
    if fmt == "dta":
        prs.write_readstat(
            DF,
            source,
            format="dta",
            value_labels=VALUE_LABELS,
            variable_labels=VARIABLE_LABELS,
            variable_format={"score": "%12.2f"},
        )
        return source, "sex", "score"
    if fmt == "sav":
        prs.write_readstat(
            DF,
            source,
            format="sav",
            value_labels=VALUE_LABELS,
            variable_labels=VARIABLE_LABELS,
            variable_measure={"sex": "nominal"},
            variable_alignment={"sex": "center"},
            variable_display_width={"sex": 12},
            variable_format={"score": "F10.3"},
        )
        return source, "sex", "score"
    if fmt == "xpt":
        prs.write_xpt(
            DF,
            source,
            variable_labels=VARIABLE_LABELS,
            variable_format={"score": "F8.0"},
        )
        return source, "sex", "score"
    if fmt == "por":
        prs.write_por(DF, source, variable_labels={"sex": "Sex of respondent"})
        return source, "SEX", "SCORE"
    raise AssertionError(f"unknown format {fmt}")


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_metadata_df_roundtrip(tmp_path: Path, fmt: str) -> None:
    """metadata_df survives a write → read → write → read roundtrip."""
    source = tmp_path / f"source.{fmt}"
    out = tmp_path / f"out.{fmt}"

    spss_extra = dict(
        variable_measure={"sex": "nominal"},
        variable_alignment={"sex": "center"},
        variable_display_width={"sex": 12},
        variable_format={"score": "F10.3"},
    ) if fmt == "sav" else dict(variable_format={"score": "%12.2f"})
    prs.write_readstat(DF, str(source), value_labels=VALUE_LABELS, variable_labels=VARIABLE_LABELS, **spss_extra)

    reader = prs.ScanReadstat(str(source))
    mdf = reader.metadata_df

    assert isinstance(mdf, pl.DataFrame)
    assert "name" in mdf.columns
    assert "label" in mdf.columns
    assert "value_label_codes" in mdf.columns
    assert "value_label_labels" in mdf.columns

    prs.write_readstat(reader.df.collect(), str(out), metadata=mdf)

    roundtrip = prs.ScanReadstat(str(out)).metadata
    sex = _col_by_name(roundtrip, "sex")
    score = _col_by_name(roundtrip, "score")

    assert sex.get("label") == "Sex of respondent"
    assert _norm_value_labels(sex["value_labels"]) == {"1": "Male", "2": "Female"}
    assert score.get("label") == "Test score"
    if fmt == "dta":
        assert score.get("format") == "%12.2f"
    else:
        assert score.get("format_width") == 10
        assert score.get("decimal_places") == 3


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_metadata_df_kwargs_override(tmp_path: Path, fmt: str) -> None:
    """Explicit kwargs take precedence over metadata_df values (coalesce)."""
    source = tmp_path / f"source.{fmt}"
    out = tmp_path / f"out.{fmt}"

    prs.write_readstat(DF, str(source), variable_labels={"sex": "Original", "score": "Original score"})

    reader = prs.ScanReadstat(str(source))
    mdf = reader.metadata_df
    df = reader.df.collect()

    prs.write_readstat(df, str(out), metadata=mdf, variable_labels={"sex": "Overridden"})

    roundtrip = prs.ScanReadstat(str(out)).metadata
    sex = _col_by_name(roundtrip, "sex")
    score = _col_by_name(roundtrip, "score")

    assert sex.get("label") == "Overridden"
    assert score.get("label") == "Original score"


@pytest.mark.parametrize("fmt", ["dta", "sav"])
def test_metadata_df_column_subset(tmp_path: Path, fmt: str) -> None:
    """Passing a full metadata_df but writing only a subset of columns works correctly."""
    source = tmp_path / f"source.{fmt}"
    out = tmp_path / f"out.{fmt}"

    prs.write_readstat(DF, str(source), variable_labels=VARIABLE_LABELS)

    reader = prs.ScanReadstat(str(source))
    mdf = reader.metadata_df
    df_subset = reader.df.select(["score"]).collect()

    prs.write_readstat(df_subset, str(out), metadata=mdf)

    roundtrip = prs.ScanReadstat(str(out)).metadata
    variables = roundtrip.get("variables") or roundtrip.get("columns") or []
    assert [v["name"] for v in variables] == ["score"]
    assert _col_by_name(roundtrip, "score").get("label") == "Test score"


def test_metadata_df_value_label_codes_are_strings(tmp_path: Path) -> None:
    """value_label_codes in metadata_df are always strings (for cross-format compatibility)."""
    source = tmp_path / "source.dta"
    prs.write_readstat(DF, str(source), value_labels=VALUE_LABELS)

    mdf = prs.ScanReadstat(str(source)).metadata_df
    row = mdf.filter(pl.col("name") == "sex")
    codes = row["value_label_codes"][0]
    labels = row["value_label_labels"][0]

    assert set(codes) == {"1", "2"}
    assert set(labels) == {"Male", "Female"}
    assert len(codes) == len(labels) == 2


@pytest.mark.parametrize("fmt", ["dta", "sav", "xpt", "por"])
def test_metadata_df_edits_are_used(tmp_path: Path, fmt: str) -> None:
    source, sex_name, _ = _write_metadata_df_source(tmp_path, fmt)
    reader = prs.ScanReadstat(source)
    mdf = reader.metadata_df.with_columns(
        pl.when(pl.col("name") == sex_name)
        .then(pl.lit("Edited sex label"))
        .otherwise(pl.col("label"))
        .alias("label")
    )

    out = tmp_path / f"edited.{fmt}"
    prs.write_readstat(reader.df.collect(), out, metadata=mdf)

    sex = _col_by_name(prs.ScanReadstat(out).metadata, sex_name)
    assert sex.get("label") == "Edited sex label"


@pytest.mark.parametrize("fmt", ["dta", "sav", "xpt", "por"])
def test_metadata_df_explicit_label_kwargs_override(tmp_path: Path, fmt: str) -> None:
    source, sex_name, _ = _write_metadata_df_source(tmp_path, fmt)
    reader = prs.ScanReadstat(source)
    mdf = reader.metadata_df.with_columns(
        pl.when(pl.col("name") == sex_name)
        .then(pl.lit("Metadata label"))
        .otherwise(pl.col("label"))
        .alias("label")
    )

    out = tmp_path / f"override.{fmt}"
    prs.write_readstat(
        reader.df.collect(),
        out,
        metadata=mdf,
        variable_labels={sex_name: "Explicit label"},
    )

    sex = _col_by_name(prs.ScanReadstat(out).metadata, sex_name)
    assert sex.get("label") == "Explicit label"


def test_write_readstat_por_accepts_metadata_df(tmp_path: Path) -> None:
    source = tmp_path / "source.por"
    prs.write_por(DF, source, variable_labels={"sex": "Sex of respondent"})

    out = tmp_path / "out.por"
    reader = prs.ScanReadstat(source)
    mdf = reader.metadata_df
    prs.write_readstat(reader.df.collect(), out, metadata=mdf)

    sex = _col_by_name(prs.ScanReadstat(out).metadata, "SEX")
    assert sex.get("label") == "Sex of respondent"


def test_write_readstat_xpt_accepts_metadata_df(tmp_path: Path) -> None:
    source = tmp_path / "source.xpt"
    prs.write_xpt(
        DF,
        source,
        variable_labels={"sex": "Sex of respondent", "score": "Test score"},
        variable_format={"score": "F8.0"},
    )

    out = tmp_path / "out.xpt"
    reader = prs.ScanReadstat(source)
    mdf = reader.metadata_df
    prs.write_readstat(reader.df.collect(), out, metadata=mdf)

    metadata = prs.ScanReadstat(out).metadata
    assert _col_by_name(metadata, "sex").get("label") == "Sex of respondent"
    assert _col_by_name(metadata, "score").get("format") == "F8"


def test_spss_write_string_value_label_keys(tmp_path: Path) -> None:
    """write_readstat accepts string keys matching reader.metadata output."""
    source = tmp_path / "source.sav"
    out1 = tmp_path / "out1.sav"
    out2 = tmp_path / "out2.sav"

    prs.write_readstat(DF, str(source), value_labels=VALUE_LABELS)
    reader = prs.ScanReadstat(str(source))
    string_key_vl = reader.metadata["variables"][0]["value_labels"]  # {"1": "Male", "2": "Female"}
    df = reader.df.collect()

    prs.write_readstat(df, str(out1), value_labels={"sex": string_key_vl})
    prs.write_readstat(df, str(out2), value_labels={"sex": string_key_vl})

    for path in (out1, out2):
        rt = prs.ScanReadstat(str(path)).metadata
        sex = _col_by_name(rt, "sex")
        assert _norm_value_labels(sex["value_labels"]) == {"1": "Male", "2": "Female"}
