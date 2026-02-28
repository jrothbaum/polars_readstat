# polars_readstat
Polars plugin for SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files.

The Python package wraps the Rust core in `polars_readstat_rs` and exposes a Polars-first API. The project includes cross-library parity tests and roundtrip checks to reduce regressions.

The Rust engine is generally faster for many workloads, but performance varies by file shape and options. If you need the legacy C/C++ engine, use version 0.11.1 (see the [prior version](https://github.com/jrothbaum/polars_readstat/tree/250f516a4424fbbe84c931a41cb82b454c5ca205)).

## Why use this?

- In project benchmarks, the new Rust-backed engine is typically faster than pandas/pyreadstat on large SAS/Stata files, especially for subset/filter workloads.
- It avoids the older C/C++ toolchain complexity and ships as standard Python wheels.
- API is Polars-first (`scan_readstat`, `read_readstat`, `write_readstat`).

## Install

```bash
pip install polars-readstat
```

## Docs

Short docs are in `docs/` and built with MkDocs.

```bash
mkdocs serve
```

## Core API

### 1) Lazy scan
```python
import polars as pl
from polars_readstat import scan_readstat

lf = scan_readstat("/path/file.sas7bdat", preserve_order=True)
df = lf.select(["SERIALNO", "AGEP"]).filter(pl.col("AGEP") >= 18).collect()
```

Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `preserve_order` | `False` | Return rows in original file order. Set `True` when order matters; may be slower with multiple threads. |
| `missing_string_as_null` | `False` | Convert empty strings to `null`. |
| `value_labels_as_strings` | `False` | For labeled numeric columns (Stata/SPSS), return the string label instead of the numeric code. |
| `schema_overrides` | `None` | Dict mapping column names to Polars types (e.g. `{"id": pl.Int64}`). Useful when the file header reports a narrower type than the data requires. |
| `batch_size` | `100_000` | Number of rows per internal chunk during collect. |
| `informative_nulls` | `None` | Capture user-defined missing value indicators. See below. |

### 2) Informative Nulls

SAS, Stata, and SPSS files support user-defined missing value codes (SAS `.A`–`.Z`, Stata `.a`–`.z`, SPSS discrete/range missings). By default these are read as `null`. The `informative_nulls` option captures the missing-value indicator alongside the data value.

```python
from polars_readstat import scan_readstat, read_readstat, InformativeNullOpts

# Track all eligible columns; add a "<col>_null" String indicator column after each
lf = scan_readstat("file.dta", informative_nulls=InformativeNullOpts(columns="all"))
df = read_readstat("file.sas7bdat", informative_nulls={"columns": "all"})
```

**Three output modes** (set via the `mode` parameter):

| Mode | Description |
|---|---|
| `"separate_column"` (default) | Adds a parallel `String` column `<col><suffix>` after each tracked column |
| `"struct"` | Wraps each `(value, indicator)` pair into a `Struct` column |
| `"merged_string"` | Merges into a single `String` column (value as string, or the indicator code) |

```python
from polars_readstat import InformativeNullOpts

opts = InformativeNullOpts(
    columns=["income", "age"],      # or "all"
    mode="separate_column",         # "separate_column", "struct", or "merged_string"
    suffix="_missing",              # indicator column suffix (separate_column mode only)
    use_value_labels=True,          # use value label for indicator string when defined
)
```

`informative_nulls` accepts either an `InformativeNullOpts` dataclass or a plain dict. It is supported on `scan_readstat`, `read_readstat`, and `ScanReadstat`.

### 3) Eager read
```python
from polars_readstat import read_readstat

df = read_readstat("/path/file.dta")
```

### 4) Metadata + schema
```python
from polars_readstat import ScanReadstat

reader = ScanReadstat(path="/path/file.sav")
schema = reader.schema      # polars.Schema
metadata = reader.metadata  # dict with file info and per-column details
lf = reader.df              # LazyFrame — same as calling scan_readstat(path)
```

`metadata` is a dict with a `columns` list. Each column entry includes:
- `"name"` — column name
- `"label"` — variable label (description), if present
- `"value_labels"` — dict mapping coded values to label strings, if present

### 5) Write (Stata/SPSS) - ***EXPERIMENTAL***
Writing support is experimental and compatibility varies across tools. Stata roundtrip tests are included; SPSS roundtrip coverage is limited. Please report issues.

```python
from polars_readstat import write_readstat

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")

# With value labels and variable labels (both formats)
write_readstat(
    df,
    "/path/out.dta",
    value_labels={"sex": {1: "Male", 2: "Female"}},
    variable_labels={"sex": "Sex of respondent", "age": "Age in years"},
)

# Stata-only options
write_readstat(df, "/path/out.dta", compress=True, threads=8)
```

`write_readstat` supports Stata (`dta`) and SPSS (`sav`). SAS writing is not supported.

| Parameter | Formats | Description |
|-----------|---------|-------------|
| `value_labels` | dta, sav | Dict mapping column names to `{coded_value: label_string}`. |
| `variable_labels` | dta, sav | Dict mapping column names to descriptive label strings. |
| `compress` | dta only | Write compressed Stata file. |
| `threads` | dta only | Number of threads for writing. |

## Tests run

Test coverage includes:
- Cross-library comparisons on the pyreadstat and pandas test data, checking results against `polars-readstat==0.11.1`, [pyreadstat](https://github.com/Roche/pyreadstat), and [pandas](https://github.com/pandas-dev/pandas).
- Stata/SPSS read/write roundtrip tests.
- Large-file read/write benchmark runs on real-world data (results below).

If you want to run the same checks locally, helper scripts and tests are in `scripts/` and `tests/`.

## Benchmark

Benchmarks compare four scenarios: 1) load the full file, 2) load a subset of columns (Subset:True), 3) filter to a subset of rows (Filter: True), 4) load a subset of columns and filter to a subset of rows (Subset:True, Filter: True).

Benchmark context:
- Machine: AMD Ryzen 7 8845HS (16 cores), 14 GiB RAM, Linux Mint 22
- Storage: external SSD
- `polars-readstat` (rust engine v0.12.4) last run: February 24, 2026; comparison library timings for SAS/Stata (v0.11.1) last run August 31, 2025
- Version tested: `polars-readstat` 0.12.4 (new Rust engine) against polars-readstat 0.11.1 (prior C++ and C engines) and pandas and pyreadstat
- Method: wall-clock timings via Python `time.time()`

### Compared to Pandas and Pyreadstat (using read_file_multiprocessing for parallel processing in Pyreadstat)
#### SAS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://github.com/jrothbaum/polars_readstat_rs) | 0.72<br>(2.9×) | 0.04<br>(51.5×) | 1.04<br>(2.9×) | 0.04<br>(52.5×) |
| polars_readstat<br>engine="cpp"<br>(fastest for 0.11.1) | 1.31<br>(1.6×) | 0.09<br>(22.9×) | 1.56<br>(1.9×) | 0.09<br>(23.2×) |
| pandas | 2.07 | 2.06 | 3.03 | 2.09 |
| pyreadstat | 10.75<br>(0.2×) | 0.46<br>(4.5×) | 11.93<br>(0.3×) | 0.50<br>(4.2×) |

#### Stata
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://github.com/jrothbaum/polars_readstat_rs) | 0.17<br>(6.7×) | 0.12<br>(9.8×) | 0.24<br>(4.1×) | 0.11<br>(8.7×) |
| polars_readstat<br>engine="readstat"<br>(the only option for 0.11.1) | 1.80<br>(0.6×) | 0.27<br>(4.4×) | 1.31<br>(0.8×) | 0.29<br>(3.3×) |
| pandas | 1.14 | 1.18 | 0.99 | 0.96 |
| pyreadstat | 7.46<br>(0.2×) | 2.18<br>(0.5×) | 7.66<br>(0.1×) | 2.24<br>(0.4×) |

#### SPSS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://github.com/jrothbaum/polars_readstat_rs) | 0.22<br>(6.6×) | 0.15<br>(9.1×) | 0.25<br>(6.0×) | 0.26<br>(4.5×) |
| pandas | 1.46 | 1.36 | 1.49 | 1.16 |
| pyreadstat | 9.25<br>(0.2×) | 4.85<br>(0.3×) | 9.39<br>(0.2×) | 4.75<br>(0.2×) |

Detailed benchmark notes and dataset descriptions are in `BENCHMARKS.md`.
