# polars_readstat
Polars plugin for SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files.

The Python package wraps the Rust core in [polars_readstat_rs](https://crates.io/crates/polars-readstat-rs) and exposes a Polars-first API. The project includes cross-library parity tests and roundtrip checks to reduce regressions.

## Why use this?

- In project [benchmarks](#benchmark), polars_readstat is typically faster than pandas/pyreadstat (usually at least 4x faster), and can be much, much faster for loading subsets of rows or columns.
- It returns a Polars `LazyFrame`, so many polars operations that reduce the number of columns or rows you need can be pushed into the reader — only the data you actually need is read from disk.
- Lots of customization and options for reading metadata (quickly), handling formats and labels, special null encoding, etc. to flexibly handle many needs when converting stat software data into a polars dataframe
- Fast and low memory streaming of files to parquet, if you just want to get the data out of the original format to something more efficient ([see code below](#sink)).

## Install

```bash
uv add polars-readstat
```

or if you prefer pip:
```bash
pip install polars-readstat
```

## Docs/Core API
View the docs at [https://jrothbaum.github.io/polars_readstat/](https://jrothbaum.github.io/polars_readstat/) for more information than below on the options you can pass to the scan and write functions.

### 1) Lazy scan
```python
import polars as pl
from polars_readstat import scan_readstat

lf = scan_readstat("/path/file.sas7bdat")
#   do something
df = lf.collect()

df = (
    scan_readstat("/path/file.sas7bdat")
    .select(["SERIALNO", "AGEP"])   # column pushdown — only these columns are read
    .head(1_000)                    # row limit is pushed down too
    .filter(pl.col("AGEP") >= 18)   # filters applied to streamed batches to avoid loading full file into memory
    .collect()
)
```

### 2) Getting metadata
```python
from polars_readstat import ScanReadstat

reader = ScanReadstat(path="/path/file.sav")
schema = reader.schema           # polars.Schema
metadata = reader.metadata       # dict with file info and per-column details
lf = reader.df                   # LazyFrame — same as calling scan_readstat(path)
```

`metadata` is a dict with a `variables` (SPSS/Stata) or `columns` (SAS) list. Each entry includes:
- `"name"` — column name
- `"label"` — variable label (description), if present
- `"value_labels"` — dict mapping coded values to label strings, if present

### Polars lazy evaluation

`scan_readstat` returns a `LazyFrame`, so Polars can push operations into the reader before any data is loaded:

**Read only specific columns** — column selection is pushed into the reader; unselected columns are never read from disk:
```python
lf = scan_readstat("file.sav")
df = lf.select(["id", "age", "income"]).collect()
```

**Read the first N rows** — `head()` / `limit()` stops the reader after N rows, so you never load the full file:
```python
df = scan_readstat("file.sas7bdat").head(1000).collect()
```

**Filter rows** — filters are applied in Polars after reading, but still benefit from column pushdown if combined with `.select()`:
```python
df = scan_readstat("file.dta").select(["id", "age"]).filter(pl.col("age") >= 18).collect()
```

The benchmark numbers below reflect these optimizations — the large "Subset: True" speedups come from column pushdown.

### 3) Write
Writing support is experimental and compatibility varies across tools. Stata roundtrip tests are included; SPSS roundtrip coverage is limited. Please report issues.

```python
from polars_readstat import write_readstat, write_sas_csv_import

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")
write_sas_csv_import(df, "/path/out/sas_bundle", dataset_name="my_data")
```

`write_readstat` supports Stata (`dta`) and SPSS (`sav`).  
Use `write_sas_csv_import` for SAS-ingestible output (`.csv` + `.sas` import script). Binary `.sas7bdat` writing is not currently supported.

### 4) <a id="sink"></a>You just want to get the data into parquet (or any other polars-supported file type):

#### Fast, Low-RAM Conversion
```
scan_readstat("/path/file.sas7bdat").sink_parquet("/path/file.parquet")
```
To give context, I converted a very tall sas file (over 8 billion rows) that was 500GB on disk to parquet and never exceeded 1GB of RAM.

#### Fast, Order-Preserving, but High-RAM Conversion
However, that code doesn't guarantee the row order is preserved, if you need that, use add `preserve_order=True` (at the cost of greater RAM usage).
```
scan_readstat("/path/file.sas7bdat",preserve_order=True).sink_parquet("/path/file.parquet")
```

#### Slower, Order-Preserving, Low-RAM Conversion
If you don't care as much about speed, but want the row order preserved and low RAM, set `threads=1` and it will stream in order (somewhat slower) with super low ram usage.
```
scan_readstat("/path/file.sas7bdat",threads=1).sink_parquet("/path/file.parquet")
```


## <a id="benchmark"></a>Benchmark 

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
