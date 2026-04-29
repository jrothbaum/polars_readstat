# Write (Experimental)

Writing support is experimental and compatibility varies across tools. Stata roundtrip tests are included; SPSS roundtrip coverage is limited.

Supported formats:

- Stata: `.dta`
- SPSS: `.sav`, `.zsav`
- SAS CSV import bundle: `.csv` + `.sas` script via `write_sas_csv_import` (not binary `.sas7bdat`)

```python
from polars_readstat import write_readstat, write_sas_csv_import

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")
```

The output format is inferred from the file extension. Pass `format=` to override explicitly (e.g. `format="dta"`).

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
| `variable_format` | Dict mapping column names to SPSS format strings (e.g. `"F10.2"`, `"A20"`). |
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

`metadata` extracts `value_labels`, `variable_labels`, and `variable_format` for both formats. For SPSS it also extracts `variable_measure`, `variable_display_width`, and `variable_alignment`. Any explicitly passed kwargs take precedence over what is derived from metadata.

Notes:

- `write_readstat(..., format="sas")` is intentionally unsupported because it implies binary `.sas7bdat` output.
- Use `write_sas_csv_import(...)` to generate a SAS-ingestible bundle (`.csv` + `.sas` import script).
