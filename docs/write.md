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
| `metadata` | Metadata dict from `ScanReadstat(...).metadata`. Extracts variable labels, value labels, and formats automatically. See [Preserving metadata](#preserving-metadata-from-a-source-file). Explicit kwargs take precedence. |

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
| `value_labels` | Dict mapping column names to `{coded_value: label}`. |
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

Pass `metadata=reader.metadata` to carry over variable labels, value labels, formats, and SPSS-specific attributes when writing. Only variables present in the DataFrame being written are used, so this works correctly even when writing a column subset.

Note: since Polars DataFrames do not carry file metadata, use `ScanReadstat` to access when reading the table so it is available when writing.

```python
from polars_readstat import ScanReadstat, write_readstat

# Stata
reader = ScanReadstat("source.dta")
df = reader.df.collect()
write_readstat(df, "out.dta", metadata=reader.metadata)

# SPSS
reader = ScanReadstat("source.sav")
df = reader.df.collect()
write_readstat(df, "out.sav", metadata=reader.metadata)
```

`metadata` extracts variable labels, value labels, and formats for each format. For SPSS it also extracts `variable_measure`, `variable_display_width`, and `variable_alignment`. Any explicitly passed kwargs take precedence.

Notes:

- `write_readstat(..., format="sas")` is intentionally unsupported because it implies binary `.sas7bdat` output.
- Use `write_sas_csv_import(...)` to generate a SAS-ingestible bundle (`.csv` + `.sas` import script).
