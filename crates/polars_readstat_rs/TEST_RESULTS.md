# Test Results (Quick Summary)

This repo contains Rust readers for **SAS**, **Stata**, and **SPSS** with Polars support.

## Relative Timings (100k rows, polars_readstat vs Rust)

- **SAS** (`psam_p17.sas7bdat`): Python ~1.471s vs Rust ~0.160s
- **Stata** (`usa_00009.dta`): Python ~2.476s vs Rust ~0.282s
- **SPSS** (`ess_data.sav`): Python ~1.473s vs Rust ~1.125s

## Status

- **SAS**: reader works, AnonymousScan works, Arrow export works
  - Threads: ✅ (`with_n_threads`)
  - Chunk size: ✅ (`with_chunk_size`)
- **Stata**: reader works, AnonymousScan works, Arrow export works
  - Threads: ✅ (`with_n_threads`)
  - Chunk size: ✅ (`with_chunk_size`)
- **SPSS**: reader works, AnonymousScan works, Arrow export works
  - Threads: ✅ (`with_n_threads`)
  - Chunk size: ✅ (`with_chunk_size`)

## Validation (Correctness)

### SAS vs ReadStat (Python)

- **Files:** 181 files in `tests/sas/data` (skips >1GB)
- **Checked:** 30,724 values (first 10 rows, all columns)
- **Result:** 0 mismatches
- Notes:
  - `zero_variables.sas7bdat` skipped (polars_readstat error: no columns)

### Stata vs ReadStat (Python)

- **File:** `tests/stata/data/usa_00009.dta`
- **Checked:** 1,600,000 values (first 100,000 rows, 16 columns)
- **Result:** 0 mismatches

### SPSS vs ReadStat (Python)

- **Files:** 19 files in `tests/spss/data`
- **Checked:** 15,707 values (first 10 rows, all columns)
- **Result:** 0 mismatches
- Notes:
  - `datetime.sav` skipped (polars_readstat missing `pyarrow`, pyreadstat date range error)
  - Column-name case can differ (e.g., `VAR` vs `var`)
  - Some SPSS files contain extra columns not present in ReadStat

## Benchmarks (Performance)

### SAS

- `uv run tests/sas/bench_vs_python.py --file tests/sas/data/psam_p17.sas7bdat --rows 100000 --repeat 2`
- Result (mean): Python 1.471s, Rust 0.160s

### Stata

- `uv run tests/stata/bench_vs_python.py --file tests/stata/data/usa_00009.dta --rows 100000 --repeat 2`
- Result (mean): Python 2.476s, Rust 0.282s

### SPSS

- `uv run tests/spss/bench_vs_python.py --file tests/spss/data/ess_data.sav --rows 100000 --repeat 2 --readstat-default`
- Result (mean): Python 1.473s, Rust 1.125s

## How to Reproduce

```bash
# SAS validation
uv run tests/sas/compare_to_python.py

# Stata validation (100k rows)
uv run tests/stata/compare_to_python.py --file tests/stata/data/usa_00009.dta --rows 100000

# SPSS validation
uv run tests/spss/compare_to_python.py

# SAS benchmark
uv run tests/sas/bench_vs_python.py --file tests/sas/data/psam_p17.sas7bdat --rows 100000 --repeat 2

# Stata benchmark
uv run tests/stata/bench_vs_python.py --file tests/stata/data/usa_00009.dta --rows 100000 --repeat 2

# SPSS benchmark
uv run tests/spss/bench_vs_python.py --file tests/spss/data/ess_data.sav --rows 100000 --repeat 2 --readstat-default
```
