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

## SAS Transport (`write_xpt`)

```python
from polars_readstat import write_xpt

write_xpt(df, "/path/out.xpt", version=8, table_name="MYDATA", file_label="My dataset",
          variable_labels={"id": "Record ID"})
```

Parameters: `version` (5 or 8, default 5), `table_name`, `file_label`, `variable_labels`, `metadata`. Variable names are uppercased and truncated to 8 characters.

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
