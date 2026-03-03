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

write_readstat(
    df,
    "/path/out.dta",
    value_labels={"sex": {1: "Male", 2: "Female"}},
    variable_labels={"sex": "Sex of respondent", "age": "Age in years"},
    compress=True,
    threads=8,
)

csv_path, sas_script_path = write_sas_csv_import(
    df,
    "/path/out/sas_bundle",
    dataset_name="my_data",
    value_labels={"sex": {1: "Male", 2: "Female"}},
    variable_labels={"sex": "Sex of respondent"},
)
```

Key parameters:

| Parameter | Formats | Notes |
| --- | --- | --- |
| `value_labels` | dta, sav, zsav | Dict mapping columns to `{coded_value: label}`. |
| `variable_labels` | dta, sav, zsav | Dict mapping columns to descriptive labels. |
| `compress` | dta | Write compressed Stata file (`.dta`). No effect for SPSS (`.sav`). |
| `threads` | dta | Number of threads for writing. |

Notes:

- `write_readstat(..., format="sas")` is intentionally unsupported because it implies binary `.sas7bdat` output.
- Use `write_sas_csv_import(...)` to generate a SAS-ingestible bundle (`.csv` + `.sas` import script).
