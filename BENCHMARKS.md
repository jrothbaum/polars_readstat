# Benchmarks

This project includes practical, workload-driven benchmark checks comparing:
- `polars_readstat`==0.12.0 (new Rust engine)
- `polars-readstat==0.11.1` legacy engines (`cpp`/`readstat`)
- `pandas`
- `pyreadstat`

These are intended as reproducible engineering checks, not formal microbenchmarks.

## Environment

- CPU: AMD Ryzen 7 8845HS (16 cores)
- RAM: 14 GiB
- OS: Linux Mint 22
- Storage: external SSD
- Last run date: August 31, 2025
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

## Notes

- Results can vary by machine, disk speed, CPU scaling, and library versions.
- Use the benchmark tables in `README.md` as directional guidance.
- For reruns, use the helper scripts in `scripts/` and tests in `tests/`.
