# Benchmarks

This project includes practical, workload-driven benchmark checks comparing:
- `polars_readstat`==0.12.4 (new Rust engine)
- `polars-readstat==0.11.1` legacy engines (`cpp`/`readstat`)
- `pandas`
- `pyreadstat`

These are intended as reproducible engineering checks, not formal microbenchmarks.

## Environment

- CPU: AMD Ryzen 7 8845HS (16 cores)
- RAM: 14 GiB
- OS: Linux Mint 22
- Storage: external SSD
- Last run date: August 31, 2025 (SAS/Stata); February 24–26, 2026 (SPSS)
- Timing method: Python `time.time()` wall-clock timing

## Workloads

Each benchmark compares four scenarios:
1. Full file load
2. Column subset
3. Row filter
4. Column subset + row filter

## Datasets

- Stata (`.dta`)
  - Source: IPUMS 2000 5% sample decennial census
  - Rows used: 10,000,000 (capped to fit laptop memory)
  - Shape: relatively tall and narrow

- SAS (`.sas7bdat`)
  - Source: ACS 5-year Illinois PUMS
  - Rows used: 623,757
  - Shape: shorter and wider

- SPSS (`.sav`)
  - Source: American National Election Studies (ANES) cumulative time-series
  - Rows: 73,745
  - Columns: 1,030
  - File size: ~87 MB

## Notes

- Results can vary by machine, disk speed, CPU scaling, and library versions.
- For reruns, use the helper scripts in `scripts/` and tests in `tests/`.

## Results

#### SAS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://crates.io/crates/polars-readstat-rs) | 0.72<br>(2.9×) | 0.04<br>(51.5×) | 1.04<br>(2.9×) | 0.04<br>(52.5×) |
| polars_readstat<br>engine="cpp"<br>(fastest for 0.11.1) | 1.31<br>(1.6×) | 0.09<br>(22.9×) | 1.56<br>(1.9×) | 0.09<br>(23.2×) |
| pandas | 2.07 | 2.06 | 3.03 | 2.09 |
| pyreadstat | 10.75<br>(0.2×) | 0.46<br>(4.5×) | 11.93<br>(0.3×) | 0.50<br>(4.2×) |

#### Stata
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://crates.io/crates/polars-readstat-rs) | 0.17<br>(6.7×) | 0.12<br>(9.8×) | 0.24<br>(4.1×) | 0.11<br>(8.7×) |
| polars_readstat<br>engine="readstat"<br>(the only option for 0.11.1) | 1.80<br>(0.6×) | 0.27<br>(4.4×) | 1.31<br>(0.8×) | 0.29<br>(3.3×) |
| pandas | 1.14 | 1.18 | 0.99 | 0.96 |
| pyreadstat | 7.46<br>(0.2×) | 2.18<br>(0.5×) | 7.66<br>(0.1×) | 2.24<br>(0.4×) |

#### SPSS
all times in seconds (speedup relative to pandas in parenthesis below each)
| Library | Full File | Subset: True | Filter: True | Subset: True, Filter: True |
|---------|------------------------------|-----------------------------|-----------------------------|----------------------------|
| polars_readstat<br>[New rust engine](https://crates.io/crates/polars-readstat-rs) | 0.22<br>(6.6×) | 0.15<br>(9.1×) | 0.25<br>(6.0×) | 0.26<br>(4.5×) |
| pandas | 1.46 | 1.36 | 1.49 | 1.16 |
| pyreadstat | 9.25<br>(0.2×) | 4.85<br>(0.3×) | 9.39<br>(0.2×) | 4.75<br>(0.2×) |
