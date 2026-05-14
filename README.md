# polars_readstat
Polars plugin for SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files.

The Python package wraps the Rust core in [polars_readstat_rs](https://crates.io/crates/polars-readstat-rs) and exposes a Polars-first API. The project includes cross-library parity tests and roundtrip checks to reduce regressions.

The Rust engine is generally faster for many workloads, but performance varies by file shape and options. If you need the legacy C/C++ engine, use version 0.11.1 (see the [prior version](https://github.com/jrothbaum/polars_readstat/tree/250f516a4424fbbe84c931a41cb82b454c5ca205)).

## Why use this?

- In project benchmarks, the new Rust-backed engine is typically faster than pandas/pyreadstat on large SAS/Stata files, especially for subset/filter workloads.
- It avoids the older C/C++ toolchain complexity and ships as standard Python wheels.
- API is Polars-first (`scan_readstat`, `read_readstat`, `write_readstat`, `write_sas_csv_import`).

## Install

```bash
pip install polars-readstat
```

## Core API

### 1) Lazy scan
```python
import polars as pl
from polars_readstat import scan_readstat

lf = scan_readstat("/path/file.sas7bdat", preserve_order=True)
df = lf.select(["SERIALNO", "AGEP"]).filter(pl.col("AGEP") >= 18).collect()
```

### 2) Getting metadata
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

### 3) Write (Experimental)
Writing support is experimental and compatibility varies across tools. Stata roundtrip tests are included; SPSS roundtrip coverage is limited. Please report issues.

```python
from polars_readstat import write_readstat, write_sas_csv_import

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")
write_sas_csv_import(df, "/path/out/sas_bundle", dataset_name="my_data")
```

`write_readstat` supports Stata (`dta`) and SPSS (`sav`).  
Use `write_sas_csv_import` for SAS-ingestible output (`.csv` + `.sas` import script). Binary `.sas7bdat` writing is not currently supported.

## Docs

View the docs at [https://jrothbaum.github.io/polars_readstat/](https://jrothbaum.github.io/polars_readstat/) for more information on the options you can pass to the scan and write functions.

## Benchmark

Benchmarks compare four scenarios: 1) load the full file, 2) load a subset of columns (Subset:True), 3) filter to a subset of rows (Filter: True), 4) load a subset of columns and filter to a subset of rows (Subset:True, Filter: True).

Benchmark context:
- Machine: AMD Ryzen 7 8845HS (16 cores), 14 GiB RAM, Linux Mint 22
- Storage: external SSD
- Last run: May 14, 2026 — `polars-readstat` v0.17.0 vs pandas and pyreadstat
- Method: wall-clock timings via Python `time.time()`

### Compared to Pandas and Pyreadstat (using read_file_multiprocessing for parallel processing in Pyreadstat)
#### SAS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat | 0.55<br>(3.9×) | 0.07<br>(28.4×) | 1.46<br>(2.0×) | 0.08<br>(39.4×) |
| pandas | 2.16 | 1.99 | 2.93 | 3.15 |
| pyreadstat | 6.76<br>(0.3×) | 1.64<br>(1.2×) | 7.86<br>(0.4×) | 2.18<br>(1.4×) |

#### Stata
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat | 0.16<br>(7.3×) | 0.10<br>(11.7×) | 0.18<br>(7.3×) | 0.09<br>(13.8×) |
| pandas | 1.17 | 1.17 | 1.31 | 1.24 |
| pyreadstat | 5.48<br>(0.2×) | 4.57<br>(0.3×) | 5.67<br>(0.2×) | 7.69<br>(0.2×) |

#### SPSS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat | 1.09<br>(62.5×) | 0.15<br>(3.9×) | 1.10<br>(62.4×) | 0.15<br>(3.9×) |
| pandas | 68.12 | 0.59 | 68.67 | 0.59 |
| pyreadstat | 3.06<br>(22.3×) | 1.15<br>(0.5×) | 7.09<br>(9.7×) | 1.23<br>(0.5×) |

#### zsav
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat | 3.97<br>(5.9×) | 1.04<br>(2.1×) | 4.77<br>(4.7×) | 1.15<br>(2.0×) |
| pandas | 23.47 | 2.20 | 22.40 | 2.29 |

Detailed benchmark notes and dataset descriptions are in `BENCHMARKS.md`.


## Tests run

Test coverage includes:
- Cross-library comparisons on the pyreadstat and pandas test data, checking results against `polars-readstat==0.11.1`, [pyreadstat](https://github.com/Roche/pyreadstat), and [pandas](https://github.com/pandas-dev/pandas).
- Stata/SPSS read/write roundtrip tests.
- Large-file read/write benchmark runs on real-world data (results below).

If you want to run the same checks locally, helper scripts and tests are in `scripts/` and `tests/`.
