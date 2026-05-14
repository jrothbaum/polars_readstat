# Benchmarks

This project includes practical, workload-driven benchmark checks comparing:
- `polars_readstat` v0.17.0
- `pandas`
- `pyreadstat` (using `read_file_multiprocessing` for parallel reads)

These are intended as reproducible engineering checks, not formal microbenchmarks.

## Environment

- CPU: AMD Ryzen 7 8845HS (16 cores)
- RAM: 14 GiB
- OS: Linux Mint 22
- Storage: external SSD
- Last run date: May 14, 2026
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
  - Rows: 10,000,000 (capped to fit laptop memory)
  - Shape: relatively tall and narrow

- SAS (`.sas7bdat`)
  - Source: ACS 5-year Illinois PUMS
  - Rows: 623,757
  - Shape: shorter and wider

- SPSS (`.sav`)
  - Source: American National Election Studies (ANES) cumulative time-series
  - Rows: 73,745
  - Columns: 1,030
  - File size: ~87 MB

- zsav (`.zsav`)
  - Source: ACS 5-year Illinois PUMS (same data as SAS above, SPSS compressed format)
  - Rows: 623,757
  - Note: pyreadstat excluded — exhausts RAM on this file

## Notes

- Results can vary by machine, disk speed, CPU scaling, and library versions.
- For reruns, use the helper scripts in `scripts/` and tests in `tests/`.

## Results

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
