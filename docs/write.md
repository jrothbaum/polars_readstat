# Write (Experimental)

Writing support is experimental. Please report issues.

Supported formats:

- Stata: `.dta`
- SPSS: `.sav`, `.zsav`
- SPSS Portable: `.por` (via `write_readstat` or `write_por`)
- SAS Transport: `.xpt` (via `write_xpt`)
- SAS CSV import bundle: `.csv` + `.sas` script via `write_sas_csv_import` (not binary `.sas7bdat`)

```python
from polars_readstat import write_readstat, write_xpt, write_por, write_sas_csv_import

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")
write_readstat(df, "/path/out.por")
write_xpt(df, "/path/out.xpt")
```

## `write_readstat` parameters

| Parameter | Notes |
| --- | --- |
| `format` | Override format detection. Accepted values: `"dta"` or `"stata"` for Stata; `"sav"`, `"zsav"`, or `"spss"` for SPSS; `"por"` or `"spss_por"` for SPSS Portable. Inferred from the file extension if omitted. |
| `metadata` | Metadata from `ScanReadstat(...).metadata` (dict) or `ScanReadstat(...).metadata_df` (Polars DataFrame, faster for very wide files). Extracts variable labels, value labels, and formats automatically. See [Preserving metadata](#preserving-metadata-from-a-source-file). Explicit kwargs take precedence. |

## Stata parameters (`.dta`)

| Parameter | Notes |
| --- | --- |
| `value_labels` | Dict mapping column names to `{coded_value: label}`. |
| `variable_labels` | Dict mapping column names to descriptive label strings. |
| `variable_format` | Dict mapping column names to Stata format strings (e.g. `"%12.2f"`). |
| `compress` | `True` to write a compressed Stata file. |
| `threads` | Number of threads for writing. |

```python
write_readstat(
    df,
    "/path/out.dta",
    value_labels={"sex": {1: "Male", 2: "Female"}},
    variable_labels={"sex": "Sex of respondent", "score": "Test score"},
    variable_format={"score": "%12.2f"},
    compress=True,
)
```

## SPSS parameters (`.sav` / `.zsav`)

| Parameter | Notes |
| --- | --- |
| `value_labels` | Dict mapping column names to `{coded_value: label}`. Accepts `int`, `float`, or numeric strings as keys. |
| `variable_labels` | Dict mapping column names to descriptive label strings. |
| `variable_format` | Dict mapping column names to SPSS format strings (e.g. `"F10.2"`, `"A20"`), or to a dict with keys `format_type`, `width`, and `decimals` for numeric codes. |
| `variable_measure` | Dict mapping column names to measurement level: `"nominal"`, `"ordinal"`, or `"scale"`. |
| `variable_display_width` | Dict mapping column names to display width (int). |
| `variable_alignment` | Dict mapping column names to alignment: `"left"`, `"right"`, or `"center"`. |
| `string_widths` | Dict mapping column names to a minimum declared string width in bytes (e.g. `{"COMMENTS": 1024}`). The actual data width is used if larger, so no truncation is possible. Results in a larger file when the declared width exceeds the data, unless `compressed=True`. Always honoured regardless of `preserve_string_widths`. |
| `preserve_string_widths` | `True` to honour declared string widths from `metadata=` on roundtrip. Default `False` (compact — string columns are sized to the actual data). Has no effect without `metadata=`. See [String width and roundtrip fidelity](#string-width-and-roundtrip-fidelity). |
| `compressed` | `True`/`False` to force standard SAV compression on or off. If omitted, compression is enabled automatically when the output path ends in `.zsav` and disabled for `.sav`. See [Compression](#compression). |

```python
write_readstat(
    df,
    "/path/out.sav",
    value_labels={"sex": {1: "Male", 2: "Female"}},
    variable_labels={"sex": "Sex of respondent"},
    variable_measure={"sex": "nominal"},
    variable_display_width={"sex": 10},
    variable_alignment={"sex": "left"},
    variable_format={"score": "F10.2"},
)
```

### Compression

SPSS supports a standard run-length "bytecode" compression scheme (the same one SPSS itself, PSPP, and `pyreadstat`/`haven` read). It's independent of declared string width: unused padding in a wide string column and system-missing numeric values collapse to a single byte each on disk, so a column declared `A1024` can keep that width in the metadata without inflating the file to match.

```python
# Explicit
write_readstat(df, "/path/out.sav", compressed=True)

# Implicit — .zsav enables compression automatically
write_readstat(df, "/path/out.zsav")
```

This pairs naturally with wide declared string widths — compression is what makes `preserve_string_widths=True` or `string_widths=` affordable on real files:

```python
write_readstat(
    df, "out.zsav",
    metadata=reader.metadata_df,
    preserve_string_widths=True,  # declared widths survive the roundtrip
    # compressed=True is implied by the .zsav extension here
)
```

Note that this is standard SAV compression (compression code 1), not the zlib-block ZSAV format some tools also produce — `.zsav` is used here only as the conventional trigger for "compress this file." True zlib-block ZSAV writing isn't supported yet, but files written this way remain readable as `.sav`/`.zsav` by this library, `pyreadstat`, and SPSS itself.

## SAS Transport (`write_xpt`)

```python
from polars_readstat import write_xpt

write_xpt(df, "/path/out.xpt", version=8, table_name="MYDATA", file_label="My dataset",
          variable_labels={"id": "Record ID"})
```

Parameters: `version` (5 or 8, default 5), `table_name`, `file_label`, `variable_labels`, `metadata`. Variable names are uppercased and truncated to 8 characters.

`storage_widths` accepts a dict mapping column names to a minimum storage width. For numeric columns the value is clamped to 3–8 bytes (controls precision). For character columns it sets a minimum declared width, with the actual data width used if larger — same tradeoff as `string_widths` for SPSS.

## SPSS Portable (`write_por`)

```python
from polars_readstat import write_por

write_por(df, "/path/out.por", file_label="My dataset", variable_labels={"ID": "Record ID"})
```

Parameters: `file_label`, `variable_labels`. Also callable as `write_readstat(df, "out.por")`. Variable names are uppercased and truncated to 8 characters.

## Preserving metadata from a source file

`write_readstat` accepts a `metadata=` argument that carries over variable labels, value labels, formats, and SPSS-specific attributes (measure, alignment, display width). Only variables present in the DataFrame being written are included, so this works correctly when writing a column subset. Explicit kwargs always override anything derived from `metadata=`.

Pass `reader.metadata` (a dict) for convenience:

```python
from polars_readstat import ScanReadstat, write_readstat

reader = ScanReadstat("source.sav")
df = reader.df.collect()

write_readstat(df, "out.sav", metadata=reader.metadata)
```

Pass `reader.metadata_df` (a Polars DataFrame) for better performance on wide files — Rust reads Arrow arrays directly with no JSON serialization:

```python
reader = ScanReadstat("source.sav")
df = reader.df.collect()

write_readstat(df, "out.sav", metadata=reader.metadata_df)
```

Both forms support kwargs overrides that take precedence over the base metadata:

```python
write_readstat(
    df, "out.sav",
    metadata=reader.metadata_df,
    variable_labels={"my_col": "Overridden label"},  # takes precedence
)
```

### String width and roundtrip fidelity

By default, SPSS string columns are written at the width of the widest value in the data, keeping files compact. Declared widths from `metadata=` are ignored for storage.

If you need to preserve the original declared widths (e.g. a survey file where open-ended fields are specified as A1024 regardless of the current wave's data), pass `preserve_string_widths=True`. This results in larger files when the declared width exceeds the actual data, but the variable specification survives the roundtrip intact:

```python
# Compact (default) — labels/formats preserved, string widths shrunk to data
write_readstat(df, "out.sav", metadata=reader.metadata_df)

# Full fidelity — declared string widths preserved, file may be larger
write_readstat(df, "out.sav", metadata=reader.metadata_df, preserve_string_widths=True)

# Selective — compact roundtrip but specific columns explicitly widened
write_readstat(df, "out.sav", metadata=reader.metadata_df, string_widths={"COMMENTS": 1024})
```

`string_widths` is always honoured regardless of `preserve_string_widths`, and the actual data width always wins if it is larger than the declared width — no truncation is possible.

### `metadata_df` schema

`reader.metadata_df` is a standard Polars DataFrame with one row per variable:

| Column | Type | Description |
| --- | --- | --- |
| `name` | `String` | Variable name |
| `label` | `String` (nullable) | Variable label |
| `value_label_codes` | `List[String]` (nullable) | Coded values as strings |
| `value_label_labels` | `List[String]` (nullable) | Corresponding display labels |
| `format` | `String` (nullable) | Format string (Stata: e.g. `"%12.2f"`; null for SPSS) |
| `format_type` | `Int32` (nullable) | SPSS numeric format type code; null for Stata/SAS |
| `format_width` | `Int32` (nullable) | Format width |
| `format_decimals` | `Int32` (nullable) | Decimal places |
| `measure` | `String` (nullable) | SPSS measurement level (`"nominal"`, `"ordinal"`, `"scale"`) |
| `display_width` | `Int32` (nullable) | SPSS display width |
| `alignment` | `String` (nullable) | SPSS alignment (`"left"`, `"right"`, `"center"`) |
| `string_width_bytes` | `Int32` (nullable) | Declared string storage width in bytes; used when `preserve_string_widths=True` or via `string_widths=` |

Because it is an ordinary DataFrame, you can inspect, filter, or edit it before passing it to `write_readstat`:

```python
reader = ScanReadstat("source.sav")
mdf = reader.metadata_df

# Inspect
print(mdf.filter(pl.col("label").is_not_null()))

# Edit: override a label before writing
mdf = mdf.with_columns(
    pl.when(pl.col("name") == "income")
    .then(pl.lit("Annual income (USD)"))
    .otherwise(pl.col("label"))
    .alias("label")
)

write_readstat(df, "out.sav", metadata=mdf)
```

Notes:

- `write_readstat(..., format="sas")` is intentionally unsupported because it implies binary `.sas7bdat` output.
- Use `write_sas_csv_import(...)` to generate a SAS-ingestible bundle (`.csv` + `.sas` import script).
