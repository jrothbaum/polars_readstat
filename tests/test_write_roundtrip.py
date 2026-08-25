from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

REPO_ROOT = Path(__file__).resolve().parents[1]
XPT_DIR = REPO_ROOT / "crates/polars_readstat_rs/tests/sas/data/xpt"

import polars_readstat as prs

ROUNDTRIP_SOURCE_FILES = [
    "sas/data/data/file1.sas7bdat",
    "sas/data/data_pandas/test1.sas7bdat",
    "stata/data/sample.dta",
    "stata/data/sample_pyreadstat.dta",
    "spss/data/sample.sav",
    "spss/data/simple_alltypes.sav",
]

WRITE_FORMATS = ["dta", "sav"]

SIGNED_INT_DTYPES = {pl.Int8, pl.Int16, pl.Int32, pl.Int64}
UNSIGNED_INT_DTYPES = {pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
FLOAT_DTYPES = {pl.Float32, pl.Float64}


def _normalize_for_compare(df: pl.DataFrame) -> pl.DataFrame:
    exprs: list[pl.Expr] = []
    for name, dtype in df.schema.items():
        if dtype in SIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Int64))
        elif dtype in UNSIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.UInt64))
        elif dtype in FLOAT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Float64))
        elif dtype == pl.String:
            # Ignore writer-dependent trailing NUL padding.
            exprs.append(pl.col(name).str.replace_all(r"\x00+$", ""))
        else:
            exprs.append(pl.col(name))
    return df.select(exprs)


@pytest.mark.parametrize("source_rel", ROUNDTRIP_SOURCE_FILES)
@pytest.mark.parametrize("target_fmt", WRITE_FORMATS)
def test_write_roundtrip_many_files(
    package_module,
    rs_tests_root: Path,
    tmp_path: Path,
    source_rel: str,
    target_fmt: str,
) -> None:
    source_path = rs_tests_root / source_rel
    if not source_path.exists():
        pytest.skip(f"Missing fixture: {source_path}")

    df = package_module.scan_readstat(
        str(source_path),
        preserve_order=True,
        missing_string_as_null=False,
        value_labels_as_strings=False,
    ).collect(engine="streaming")

    if df.width == 0:
        pytest.skip(f"Skipping zero-column fixture: {source_path}")

    out_path = tmp_path / f"{source_path.stem}.roundtrip.{target_fmt}"
    package_module.write_readstat(df, str(out_path), format=target_fmt)

    roundtrip = package_module.scan_readstat(
        str(out_path),
        preserve_order=True,
        missing_string_as_null=False,
        value_labels_as_strings=False,
    ).collect(engine="streaming")

    assert df.shape == roundtrip.shape, f"Shape mismatch for {source_rel} -> {target_fmt}"
    assert df.columns == roundtrip.columns, f"Column mismatch for {source_rel} -> {target_fmt}"

    assert_frame_equal(
        _normalize_for_compare(df),
        _normalize_for_compare(roundtrip),
        check_dtypes=False,
        check_row_order=True,
        check_column_order=True,
        abs_tol=1e-6,
        rel_tol=1e-6,
    )


def test_spss_date_format_type_preserved(
    package_module, rs_tests_root: Path, tmp_path: Path
) -> None:
    """format_type survives a metadata-preserving SPSS write/re-read roundtrip.

    datetime.sav has date (ADATE=23), datetime (DATETIME=22), and time (TIME=21)
    columns. Before the fix, _spss_variable_format_from_metadata returned None
    for any format_type other than A(1) or F(5), so date subtypes were silently
    dropped and written back as generic DATE(20).
    """
    source = rs_tests_root / "spss/data/datetime.sav"
    if not source.exists():
        pytest.skip(f"Missing fixture: {source}")

    reader = package_module.ScanReadstat(str(source))
    orig_types = {v["name"]: v["format_type"] for v in reader.metadata["variables"]}
    df = reader.df.collect()

    out = tmp_path / "datetime_roundtrip.sav"
    package_module.write_readstat(df, str(out), metadata=reader.metadata)

    reader2 = package_module.ScanReadstat(str(out))
    rt_types = {v["name"]: v["format_type"] for v in reader2.metadata["variables"]}
    df2 = reader2.df.collect()

    assert_frame_equal(df, df2)

    for name, expected in orig_types.items():
        assert rt_types[name] == expected, (
            f"format_type mismatch for '{name}': expected {expected}, got {rt_types[name]}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# XPT write roundtrip tests
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_xpt(df: pl.DataFrame) -> pl.DataFrame:
    """Cast all columns to a comparable form after XPT roundtrip."""
    exprs = []
    for name, dtype in df.schema.items():
        if dtype in FLOAT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Float64))
        elif dtype in SIGNED_INT_DTYPES | UNSIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Float64))
        elif dtype == pl.String:
            exprs.append(pl.col(name).str.strip_chars_end("\x00").str.strip_chars_end(" "))
        else:
            exprs.append(pl.col(name))
    return df.select(exprs)


@pytest.mark.parametrize("xpt_file,version", [
    ("sample.xpt", 5),
    ("sas.xpt8", 8),
])
def test_xpt_write_roundtrip(tmp_path: Path, xpt_file: str, version: int) -> None:
    """Write an XPT file, read it back, and compare."""
    src = XPT_DIR / xpt_file
    if not src.exists():
        pytest.skip(f"Missing fixture: {src}")

    orig = prs.scan_readstat(str(src), preserve_order=True).collect()
    if orig.width == 0:
        pytest.skip("zero-column fixture")

    out = tmp_path / f"roundtrip_v{version}.xpt"
    prs.write_xpt(orig, str(out), version=version)

    rt = prs.scan_readstat(str(out), preserve_order=True).collect()

    assert orig.shape == rt.shape
    assert orig.columns == rt.columns
    assert_frame_equal(
        _normalize_xpt(orig),
        _normalize_xpt(rt),
        check_dtypes=False,
        check_row_order=True,
        abs_tol=1e-6,
        rel_tol=1e-6,
    )


def test_xpt_write_synthetic(tmp_path: Path) -> None:
    """Write a synthetic DataFrame with mixed types and verify each column."""
    df = pl.DataFrame({
        "id": [1.0, 2.0, 3.0, None],
        "score": [1.5, -2.25, 0.0, 100.0],
        "name": ["Alice", "Bob", "Charlie", None],
        "dt": pl.Series([
            "2020-01-01", "1960-01-01", "2000-06-15", None
        ]).cast(pl.Date),
    })

    out = tmp_path / "synthetic.xpt"
    prs.write_xpt(
        df,
        str(out),
        version=8,
        table_name="TESTDS",
        file_label="Test dataset",
        variable_labels={"id": "Record ID", "score": "Test score", "name": "Full name"},
    )

    rt = prs.scan_readstat(str(out), preserve_order=True).collect()

    assert rt.shape == df.shape
    assert rt.columns == df.columns

    # Numeric columns come back as Float64 from XPT
    assert_frame_equal(
        rt.select("id", "score").cast(pl.Float64),
        df.select("id", "score").cast(pl.Float64),
        check_dtypes=False,
        abs_tol=1e-9,
    )

    # String column — XPT has no null strings; null becomes empty string after roundtrip.
    orig_names = df["name"].cast(pl.String).fill_null("")
    rt_names = rt["name"].str.strip_chars_end(" ").str.strip_chars_end("\x00").fill_null("")
    assert_frame_equal(
        orig_names.to_frame(),
        rt_names.to_frame(),
        check_dtypes=False,
    )

    # Date column roundtrips through SAS date epoch
    assert_frame_equal(
        df.select(pl.col("dt").cast(pl.Date)),
        rt.select(pl.col("dt").cast(pl.Date)),
        check_dtypes=False,
    )


def test_xpt_write_metadata_labels(tmp_path: Path) -> None:
    """Variable labels survive a write/re-read roundtrip."""
    df = pl.DataFrame({"x": [1.0, 2.0], "y": ["a", "b"]})
    labels = {"x": "X variable", "y": "Y variable"}

    out = tmp_path / "labels.xpt"
    prs.write_xpt(df, str(out), variable_labels=labels)

    meta = prs.ScanReadstat(str(out)).metadata
    # XPT metadata uses "columns" (not "variables")
    returned = {v["name"]: v.get("label") or "" for v in meta["columns"]}
    assert returned["x"] == labels["x"]
    assert returned["y"] == labels["y"]


def test_xpt_write_readstat_metadata_param(tmp_path: Path) -> None:
    """write_readstat with metadata= preserves XPT labels, formats, table/file names."""
    src = XPT_DIR / "dates_xpt_v8.xpt"
    if not src.exists():
        pytest.skip(f"Missing fixture: {src}")

    reader = prs.ScanReadstat(str(src))
    df = reader.df.collect()
    meta = reader.metadata

    out = tmp_path / "meta_rt.xpt"
    prs.write_readstat(df, str(out), metadata=meta)

    meta2 = prs.ScanReadstat(str(out)).metadata
    assert meta2["xpt_version"] == meta["xpt_version"]
    assert meta2["table_name"] == meta["table_name"]
    assert meta2["file_label"] == meta["file_label"]

    orig = {c["name"]: c for c in meta["columns"]}
    for col in meta2["columns"]:
        name = col["name"]
        assert col["label"] == orig[name]["label"], f"label mismatch for {name}"
        assert col["format"] == orig[name]["format"], f"format mismatch for {name}"


def test_xpt_write_bad_extension(tmp_path: Path) -> None:
    """write_xpt rejects paths that don't end in .xpt."""
    df = pl.DataFrame({"x": [1.0]})
    with pytest.raises(Exception):
        prs.write_xpt(df, str(tmp_path / "out.sav"))


# ─────────────────────────────────────────────────────────────────────────────
# POR read / write roundtrip tests
# ─────────────────────────────────────────────────────────────────────────────

POR_DIR = REPO_ROOT / "crates/polars_readstat_rs/tests/spss/data"


def _normalize_por(df: pl.DataFrame) -> pl.DataFrame:
    """Cast all columns to a stable form for POR roundtrip comparison."""
    exprs = []
    for name, dtype in df.schema.items():
        if dtype in FLOAT_DTYPES | SIGNED_INT_DTYPES | UNSIGNED_INT_DTYPES:
            exprs.append(pl.col(name).cast(pl.Float64))
        elif dtype == pl.String:
            # POR has no null strings; nulls become empty or single space
            exprs.append(pl.col(name).str.strip_chars_end(" ").fill_null(""))
        else:
            exprs.append(pl.col(name))
    return df.select(exprs)


def test_por_read_sample(package_module, rs_tests_root: Path) -> None:
    """sample.por can be read via scan_readstat."""
    src = rs_tests_root / "spss/data/sample.por"
    if not src.exists():
        pytest.skip(f"Missing fixture: {src}")

    df = package_module.scan_readstat(str(src), preserve_order=True).collect()
    assert df.height > 0
    assert df.width > 0


def test_por_write_roundtrip(tmp_path: Path) -> None:
    """Write a synthetic DataFrame to POR and read it back."""
    df = pl.DataFrame({
        "ID": [1.0, 2.0, 3.0, None],
        "SCORE": [1.5, -2.25, 0.0, 100.0],
        "NAME": ["Alice", "Bob", "Charlie", ""],
    })

    out = tmp_path / "synthetic.por"
    prs.write_por(df, str(out), file_label="Test POR", variable_labels={"ID": "Record ID"})
    assert out.exists()

    rt = prs.scan_readstat(str(out), preserve_order=True).collect()

    assert rt.shape == df.shape
    assert rt.columns == df.columns

    # Numeric columns roundtrip as Float64
    assert_frame_equal(
        rt.select("ID", "SCORE").cast(pl.Float64),
        df.select("ID", "SCORE").cast(pl.Float64),
        check_dtypes=False,
        abs_tol=1e-6,
    )

    # String column: empty string survives (POR encodes missing/empty as " " or "")
    rt_names = rt["NAME"].str.strip_chars_end(" ").fill_null("")
    orig_names = df["NAME"].fill_null("")
    assert_frame_equal(rt_names.to_frame(), orig_names.to_frame(), check_dtypes=False)


def test_por_write_bad_extension(tmp_path: Path) -> None:
    """write_por rejects paths that don't end in .por."""
    df = pl.DataFrame({"x": [1.0]})
    with pytest.raises(Exception):
        prs.write_por(df, str(tmp_path / "out.sav"))


def test_por_sample_roundtrip(package_module, rs_tests_root: Path, tmp_path: Path) -> None:
    """Read sample.por → write back → re-read and compare numeric/string columns."""
    src = rs_tests_root / "spss/data/sample.por"
    if not src.exists():
        pytest.skip(f"Missing fixture: {src}")

    orig = package_module.scan_readstat(str(src), preserve_order=True).collect()
    if orig.width == 0:
        pytest.skip("zero-column fixture")

    out = tmp_path / "sample_rt.por"
    prs.write_por(orig, str(out))

    rt = package_module.scan_readstat(str(out), preserve_order=True).collect()

    assert orig.shape == rt.shape
    assert orig.columns == rt.columns

    assert_frame_equal(
        _normalize_por(orig),
        _normalize_por(rt),
        check_dtypes=False,
        check_row_order=True,
        abs_tol=1e-6,
        rel_tol=1e-6,
    )


# ── String width: declared < actual max ──────────────────────────────────────

@pytest.mark.parametrize("fmt", ["sav", "dta", "xpt"])
def test_write_string_width_undersized_metadata(tmp_path: Path, fmt: str) -> None:
    """When metadata declares a narrower string width than the data contains,
    the writer should use max(declared, actual) so no strings are truncated."""
    short = "hi"            # 2 bytes
    long_ = "hello_world_x" # 13 bytes — longer than the "declared" width below

    df_short = pl.DataFrame({"id": [1.0], "label": [short]})
    df_long  = pl.DataFrame({"id": [1.0, 2.0], "label": [short, long_]})

    # Write the short-string version to establish a "metadata" baseline.
    src = tmp_path / f"short.{fmt}"
    prs.write_readstat(df_short, str(src))

    # Read it back so we have metadata that declares a narrow string width.
    reader = prs.ScanReadstat(str(src))
    metadata = reader.metadata_df

    # Now write the long-string version using that narrow metadata.
    # For all formats, pass the metadata_df from the narrow-width file so the
    # string_width_bytes path (not a hardcoded kwarg) is exercised.
    out = tmp_path / f"long.{fmt}"
    prs.write_readstat(df_long, str(out), metadata=metadata)

    result = prs.scan_readstat(str(out)).collect()

    assert result["label"].to_list() == [short, long_], (
        f"{fmt}: string was truncated — expected {[short, long_]!r}, "
        f"got {result['label'].to_list()!r}"
    )


# ── String width: declared > actual max ──────────────────────────────────────

@pytest.mark.parametrize("fmt", ["dta"])
def test_write_string_width_oversized_metadata_stays_compact(tmp_path: Path, fmt: str) -> None:
    """For Stata, metadata with a wider declared string width than the data
    needs should not cause file bloat — the actual data width is used."""
    wide_value = "x" * 500   # 500 bytes — establishes a wide declared width
    short_value = "hello"    # 5 bytes — the actual data we want to write compactly

    df_wide  = pl.DataFrame({"id": [1.0], "label": [wide_value]})
    df_short = pl.DataFrame({"id": [1.0, 2.0], "label": [short_value, short_value]})

    src = tmp_path / f"wide.{fmt}"
    prs.write_readstat(df_wide, str(src))
    wide_size = src.stat().st_size

    metadata = prs.ScanReadstat(str(src)).metadata_df

    out = tmp_path / f"short.{fmt}"
    prs.write_readstat(df_short, str(out), metadata=metadata)
    out_size = out.stat().st_size

    baseline = tmp_path / f"baseline.{fmt}"
    prs.write_readstat(df_short, str(baseline))
    baseline_size = baseline.stat().st_size

    result = prs.scan_readstat(str(out)).collect()
    assert result["label"].to_list() == [short_value, short_value], (
        f"{fmt}: data corrupted after compact write"
    )

    assert out_size <= baseline_size * 1.1 + 64, (
        f"{fmt}: oversized metadata caused file bloat — "
        f"baseline={baseline_size}, with_metadata={out_size}, wide_src={wide_size}"
    )


def test_write_spss_metadata_roundtrip_compact_by_default(tmp_path: Path) -> None:
    """By default, passing metadata does not inflate string widths — data width is used."""
    wide_value = "x" * 500
    short_value = "hello"

    df_wide  = pl.DataFrame({"id": [1.0], "label": [wide_value]})
    df_short = pl.DataFrame({"id": [1.0, 2.0], "label": [short_value, short_value]})

    src = tmp_path / "wide.sav"
    prs.write_readstat(df_wide, str(src))
    metadata = prs.ScanReadstat(str(src)).metadata_df

    out = tmp_path / "short.sav"
    prs.write_readstat(df_short, str(out), metadata=metadata)

    result = prs.scan_readstat(str(out)).collect()
    assert result["label"].to_list() == [short_value, short_value]

    out_meta = prs.ScanReadstat(str(out)).metadata_df
    roundtrip_width = out_meta.filter(pl.col("name") == "label")["string_width_bytes"][0]
    assert roundtrip_width == len(short_value), (
        f"expected compact width {len(short_value)}, got {roundtrip_width}"
    )


def test_write_spss_string_widths_preserved_on_roundtrip(tmp_path: Path) -> None:
    """With preserve_string_widths=True, declared widths from metadata survive roundtrip."""
    wide_value = "x" * 500
    short_value = "hello"

    df_wide  = pl.DataFrame({"id": [1.0], "label": [wide_value]})
    df_short = pl.DataFrame({"id": [1.0, 2.0], "label": [short_value, short_value]})

    src = tmp_path / "wide.sav"
    prs.write_readstat(df_wide, str(src))

    metadata = prs.ScanReadstat(str(src)).metadata_df
    declared_width = metadata.filter(pl.col("name") == "label")["string_width_bytes"][0]
    assert declared_width == 500, f"expected declared width 500, got {declared_width}"

    out = tmp_path / "short.sav"
    prs.write_readstat(df_short, str(out), metadata=metadata, preserve_string_widths=True)

    result = prs.scan_readstat(str(out)).collect()
    assert result["label"].to_list() == [short_value, short_value]

    out_meta = prs.ScanReadstat(str(out)).metadata_df
    roundtrip_width = out_meta.filter(pl.col("name") == "label")["string_width_bytes"][0]
    assert roundtrip_width == declared_width, (
        f"declared string width was not preserved: expected {declared_width}, got {roundtrip_width}"
    )


def test_write_spss_string_widths_explicit_dict(tmp_path: Path) -> None:
    """Explicit string_widths dict sets the declared minimum width independently of metadata,
    and columns not named in the dict (here "id") must still be written."""
    df = pl.DataFrame({"id": [1.0], "comments": ["hello"]})

    out = tmp_path / "out.sav"
    prs.write_readstat(df, str(out), string_widths={"comments": 1024})

    meta = prs.ScanReadstat(str(out)).metadata_df
    declared = meta.filter(pl.col("name") == "comments")["string_width_bytes"][0]
    assert declared == 1024, f"expected declared width 1024, got {declared}"

    result = prs.scan_readstat(str(out)).collect()
    assert result.columns == ["id", "comments"]
    assert result["id"].to_list() == [1.0]
    assert result["comments"].to_list() == ["hello"]


def test_write_spss_partial_metadata_kwargs_keep_other_columns(tmp_path: Path) -> None:
    """A metadata kwarg naming only some columns (e.g. variable_labels for one
    column) must not drop the DataFrame's other columns from the written file."""
    df = pl.DataFrame({"id": [1.0, 2.0, 3.0], "name": ["a", "b", "c"], "extra": [10, 20, 30]})

    out = tmp_path / "out.sav"
    prs.write_readstat(df, str(out), variable_labels={"name": "Full Name"})

    result = prs.scan_readstat(str(out)).collect()
    assert result.columns == ["id", "name", "extra"]
    assert result["id"].to_list() == [1.0, 2.0, 3.0]
    assert result["extra"].to_list() == [10.0, 20.0, 30.0]


def test_write_stata_partial_metadata_kwargs_keep_other_columns(tmp_path: Path) -> None:
    """Same bug as above but for the Stata writer: variable_labels naming only
    one column must not drop the DataFrame's other columns from the .dta file."""
    df = pl.DataFrame({"id": [1.0, 2.0, 3.0], "name": ["a", "b", "c"], "extra": [10, 20, 30]})

    out = tmp_path / "out.dta"
    prs.write_readstat(df, str(out), variable_labels={"name": "Full Name"})

    result = prs.scan_readstat(str(out)).collect()
    assert result.columns == ["id", "name", "extra"]
    assert result["id"].to_list() == [1.0, 2.0, 3.0]
    assert result["extra"].to_list() == [10.0, 20.0, 30.0]


def test_write_spss_compressed_roundtrip(tmp_path: Path) -> None:
    """compressed=True writes standard SAV bytecode compression and roundtrips correctly."""
    n = 200
    df = pl.DataFrame({
        "id": [float(i) for i in range(n)],
        "comments": ["short"] * n,
    })

    uncompressed = tmp_path / "uncompressed.sav"
    compressed = tmp_path / "compressed.sav"
    prs.write_readstat(df, str(uncompressed), string_widths={"comments": 1024}, compressed=False)
    prs.write_readstat(df, str(compressed), string_widths={"comments": 1024}, compressed=True)

    # Declared width survives compression (this is the point of the feature).
    meta = prs.ScanReadstat(str(compressed)).metadata_df
    declared = meta.filter(pl.col("name") == "comments")["string_width_bytes"][0]
    assert declared == 1024

    result = prs.scan_readstat(str(compressed)).collect()
    assert_frame_equal(result, df)

    assert compressed.stat().st_size < uncompressed.stat().st_size / 2


def test_write_spss_zsav_extension_compresses_by_default(tmp_path: Path) -> None:
    """Writing to a .zsav path enables compression automatically without an explicit flag."""
    n = 200
    df = pl.DataFrame({
        "id": [float(i) for i in range(n)],
        "comments": ["short"] * n,
    })

    sav_path = tmp_path / "plain.sav"
    zsav_path = tmp_path / "auto.zsav"
    prs.write_readstat(df, str(sav_path), string_widths={"comments": 1024})
    prs.write_readstat(df, str(zsav_path), string_widths={"comments": 1024})

    assert zsav_path.stat().st_size < sav_path.stat().st_size / 2
    result = prs.scan_readstat(str(zsav_path)).collect()
    assert_frame_equal(result, df)


def test_write_spss_string_widths_data_wins_if_larger(tmp_path: Path) -> None:
    """When actual data exceeds string_widths, the data width wins (no truncation)."""
    long_value = "x" * 200
    df = pl.DataFrame({"text": [long_value]})

    out = tmp_path / "out.sav"
    prs.write_readstat(df, str(out), string_widths={"text": 10})

    result = prs.scan_readstat(str(out)).collect()
    assert result["text"][0] == long_value

    meta = prs.ScanReadstat(str(out)).metadata_df
    declared = meta.filter(pl.col("name") == "text")["string_width_bytes"][0]
    assert declared == 200


def test_write_xpt_storage_widths_character_honored(tmp_path: Path) -> None:
    """XPT storage_widths is honored as a minimum for character columns."""
    df = pl.DataFrame({"label": ["hello"]})

    out = tmp_path / "out.xpt"
    prs.write_readstat(df, str(out), storage_widths={"label": 200})

    meta = prs.ScanReadstat(str(out)).metadata_df
    declared = meta.filter(pl.col("name") == "label")["string_width_bytes"][0]
    assert declared == 200

    result = prs.scan_readstat(str(out)).collect()
    assert result["label"][0] == "hello"


def test_write_xpt_storage_widths_data_wins_if_larger(tmp_path: Path) -> None:
    """XPT: when actual data exceeds storage_widths, the data width wins."""
    long_value = "x" * 100
    df = pl.DataFrame({"text": [long_value]})

    out = tmp_path / "out.xpt"
    prs.write_readstat(df, str(out), storage_widths={"text": 10})

    result = prs.scan_readstat(str(out)).collect()
    assert result["text"][0] == long_value
