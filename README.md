# polars_readstat
Polars plugin for SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files.

The Python package wraps the Rust core in `polars_readstat_rs` and exposes a simple Polars-first API.  I have tried to make sure there are no errors or regressions in this release (tested against 178 test files from pandas, pyreadstat, etc.).  If I missed something, you can find info on the [readme for the prior version](https://github.com/jrothbaum/polars_readstat/tree/250f516a4424fbbe84c931a41cb82b454c5ca205) and install v0.11.1 from pypi.

## Why use this?

- In project benchmarks, the new Rust-backed engine is typically faster than pandas/pyreadstat on large SAS/Stata files, especially for subset/filter workloads.
- It avoids the older C/C++ toolchain complexity and ships as standard Python wheels.
- API is Polars-first (`scan_readstat`, `read_readstat`, `write_readstat`).

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

### 2) Eager read
```python
from polars_readstat import read_readstat

df = read_readstat("/path/file.dta")
```

### 3) Metadata + schema
```python
from polars_readstat import ScanReadstat

reader = ScanReadstat(path="/path/file.sav")
schema = reader.schema
metadata = reader.metadata
```

### 4) Write (Stata/SPSS)
```python
from polars_readstat import write_readstat

write_readstat(df, "/path/out.dta", threads=8)
write_readstat(df, "/path/out.sav")
```

`write_readstat` supports Stata (`dta`) and SPSS (`sav`). SAS writing is not supported.

## Tests run

We’ve tried to test this thoroughly:
- Cross-library comparisons on the pyreadstat and pandas test data, checking results against `polars-readstat==0.11.1`, [pyreadstat](https://github.com/Roche/pyreadstat), and [pandas](https://github.com/pandas-dev/pandas).
- Stata/SPSS read/write roundtrip tests.
- Large-file read/write benchmark runs on real-world data (results below).

If you want to run the same checks locally, helper scripts and tests are in `scripts/` and `tests/`.

## Benchmark

For each file, I compared 4 different scenarios: 1) load the full file, 2) load a subset of columns (Subset:True), 3) filter to a subet of rows (Filter: True), 4) load a subset of columns and filter to a subset of rows (Subset:True, Filter: True).

Benchmark context:
- Machine: AMD Ryzen 7 8845HS (16 cores), 14 GiB RAM, Linux Mint 22
- Storage: external SSD
- Last run: August 31, 2025
- Version tested: `polars-readstat` 0.12 (new Rust engine) against polars-readstat 0.11.1 (prior C++ and C engines) and pandas and pyreadstat
- Method: wall-clock timings via Python `time.time()`

### Compared to Pandas and Pyreadstat (using read_file_multiprocessing for parallel processing in Pyreadstat)
#### SAS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://github.com/jrothbaum/polars_readstat_rs) | 0.90<br>(2.3×) | 0.07<br>(29.4×) | 1.23<br>(2.5×) | 0.07<br>(29.9×) |
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

Detailed benchmark notes and dataset descriptions are in `BENCHMARKS.md`.
