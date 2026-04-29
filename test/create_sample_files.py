"""
Create sample Stata and SPSS files with all supported metadata options.
Output is written to a temp directory and the path is printed.
"""
import tempfile
from pathlib import Path

import polars as pl
import polars_readstat as prs

df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "sex": [1, 2, 1, 2, 1],
    "age": [25, 34, 45, 28, 52],
    "income": [50000.0, 62000.5, 48000.0, 71000.25, 55000.0],
    "educ": [1, 2, 3, 2, 4],
    "region": [1, 2, 1, 3, 2],
    "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
})

value_labels = {
    "sex": {1: "Male", 2: "Female"},
    "educ": {1: "Less than high school", 2: "High school", 3: "Some college", 4: "College or more"},
    "region": {1: "North", 2: "South", 3: "West"},
}

variable_labels = {
    "id": "Respondent ID",
    "sex": "Sex of respondent",
    "age": "Age in years",
    "income": "Annual income (USD)",
    "educ": "Educational attainment",
    "region": "Geographic region",
    "name": "Respondent name",
}

out_dir = Path(tempfile.mkdtemp(prefix="polars_readstat_sample_"))

# --- Stata ---
stata_path = out_dir / "sample.dta"
prs.write_readstat(
    df,
    str(stata_path),
    value_labels=value_labels,
    variable_labels=variable_labels,
    variable_format={
        "income": "%12.2f",
        "age": "%3.0f",
    },
    compress=True,
)
print(f"Stata:  {stata_path}")

# --- SPSS ---
spss_path = out_dir / "sample.sav"
prs.write_readstat(
    df,
    str(spss_path),
    value_labels=value_labels,
    variable_labels=variable_labels,
    variable_format={
        "income": "F12.2",
        "age": "F3.0",
        "name": "A10",
    },
    variable_measure={
        "id": "scale",
        "sex": "nominal",
        "age": "scale",
        "income": "scale",
        "educ": "ordinal",
        "region": "nominal",
        "name": "nominal",
    },
    variable_display_width={
        "id": 5,
        "sex": 8,
        "age": 5,
        "income": 12,
        "educ": 10,
        "region": 8,
        "name": 12,
    },
    variable_alignment={
        "id": "right",
        "sex": "left",
        "age": "right",
        "income": "right",
        "educ": "left",
        "region": "left",
        "name": "left",
    },
)
print(f"SPSS:   {spss_path}")
print(f"\nOutput directory: {out_dir}")
