# Write (Experimental)

Writing support is experimental and compatibility varies across tools. Stata roundtrip tests are included; SPSS roundtrip coverage is limited.

Supported formats:

- Stata: `.dta`
- SPSS: `.sav`, `.zsav`

SAS writing is not supported.

```python
from polars_readstat import write_readstat

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")

write_readstat(
    df,
    "/path/out.dta",
    value_labels={"sex": {1: "Male", 2: "Female"}},
    variable_labels={"sex": "Sex of respondent", "age": "Age in years"},
    compress=True,
    threads=8,
)
```

Key parameters:

| Parameter | Formats | Notes |
| --- | --- | --- |
| `value_labels` | dta, sav, zsav | Dict mapping columns to `{coded_value: label}`. |
| `variable_labels` | dta, sav, zsav | Dict mapping columns to descriptive labels. |
| `compress` | dta | Write compressed Stata file (`.dta`). No effect for SPSS (`.sav`). |
| `threads` | dta | Number of threads for writing. |
