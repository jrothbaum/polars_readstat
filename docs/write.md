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
| `metadata` | Metadata from `ScanReadstat(...).metadata` (dict) or `ScanReadstat(...).metadata_handle` (opaque handle, faster for very wide files). Extracts variable labels, value labels, and formats automatically. See [Preserving metadata](#preserving-metadata-from-a-source-file). Explicit kwargs take precedence. |

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

`write_readstat` accepts a `metadata=` argument that carries over variable labels, value labels, formats, and SPSS-specific attributes (measure, alignment, display width). Only variables present in the DataFrame being written are included, so this works correctly when writing a column subset.

```python
from polars_readstat import ScanReadstat, write_readstat

reader = ScanReadstat("source.sav")
df = reader.df.collect()

write_readstat(df, "out.sav", metadata=reader.metadata)
```

Explicit kwargs always override anything derived from `metadata=`:

```python
write_readstat(
    df, "out.sav",
    metadata=reader.metadata,
    variable_labels={"my_col": "Overridden label"},  # takes precedence
)
```

Notes:

- `write_readstat(..., format="sas")` is intentionally unsupported because it implies binary `.sas7bdat` output.
- Use `write_sas_csv_import(...)` to generate a SAS-ingestible bundle (`.csv` + `.sas` import script).

## Roundtripping very wide files (thousands of variables)

For files with a large number of variables, `metadata=reader.metadata` can be slow because it serializes all variable records to JSON, parses them into Python dicts, and crosses the PyO3 boundary.

Use `reader.metadata_handle` instead — an opaque Rust object that bypasses all Python serialization and builds the writer directly in Rust:

```python
reader = ScanReadstat("source.sav")
df = reader.df.collect()

# No JSON round-trip — same speed as writing with no metadata
write_readstat(df, "out.sav", metadata=reader.metadata_handle)
```

`metadata_handle` cannot be inspected from Python; it exists solely as a passthrough to the writer. Explicit kwargs (`value_labels`, `variable_labels`, etc.) still take precedence over anything in the handle.

`reader.metadata_handle` opens the file once and caches the result. If you access `reader.metadata` (the dict) afterward, it is derived from the same cached handle at no extra I/O cost.
