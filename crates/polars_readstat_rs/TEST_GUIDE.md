# Testing Guide for SAS7BDAT Reader

This guide explains how to run tests and benchmarks for the SAS7BDAT reader library.

## Test Suite Overview

The test suite consists of three main components:

1. **Integration Tests** - Verify the reader works with all test files
2. **Regression Tests** - Ensure specific data values remain correct
3. **Benchmarks** - Track performance over time to catch regressions

## Quick Start

```bash
# Run all tests
cargo test

# Run all benchmarks
cargo bench

# Run specific test suite
cargo test --test integration_tests
cargo test --test regression_tests

# Run specific benchmark
cargo bench --bench read_benchmarks
cargo bench --bench schema_benchmarks
```

## Test Organization

```
tests/
├── common/
│   └── mod.rs              # Shared test utilities
├── integration_tests.rs     # Tests for all 178 test files
└── regression_tests.rs      # Tests for specific data values

benches/
├── read_benchmarks.rs       # Performance benchmarks for reading
└── schema_benchmarks.rs     # Performance benchmarks for schema inference
```

## Running Tests

### All Tests

```bash
# Run all tests (unit + integration + regression)
cargo test

# Run with output visible (doesn't capture println!)
cargo test -- --nocapture

# Run tests in parallel with specific thread count
cargo test -- --test-threads=4
```

### Integration Tests

Tests that verify the reader works with all test files:

```bash
# Run all integration tests
cargo test --test integration_tests

# Run specific integration test
cargo test --test integration_tests test_all_files_can_be_opened
cargo test --test integration_tests test_compressed_files
cargo test --test integration_tests test_batch_reading_matches_full_read

# See which tests are available
cargo test --test integration_tests -- --list
```

**Integration tests include:**
- `test_all_files_can_be_opened` - Verify all 178 files can be opened
- `test_all_files_can_read_metadata` - Check metadata access
- `test_all_files_can_read_data` - Read all files completely
- `test_compressed_files` - Specifically test compressed files
- `test_batch_reading_matches_full_read` - Verify batch consistency
- `test_numeric_files` - Test numeric-only files
- `test_parallel_reading` - Test multi-threaded reading

### Regression Tests

Tests that verify specific data values to catch regressions:

```bash
# Run all regression tests
cargo test --test regression_tests

# Run specific regression test
cargo test --test regression_tests test_test1_data_values
cargo test --test regression_tests test_known_file_dimensions

# See available regression tests
cargo test --test regression_tests -- --list
```

**Regression tests include:**
- `test_test1_data_values` - Verify specific values from test1.sas7bdat
- `test_known_file_dimensions` - Check row/column counts
- `test_numeric_data_integrity` - Verify numeric data is valid
- `test_compressed_data_validity` - Ensure decompression works
- `test_batch_consistency` - Batch reading produces same results
- `test_schema_inference_consistency` - Schema inference doesn't break data
- `test_edge_cases` - Handle edge cases without panics
- `test_metadata_accuracy` - Metadata matches actual data

### Unit Tests

Unit tests are embedded in source files:

```bash
# Run unit tests only
cargo test --lib

# Run tests for specific module
cargo test reader::
cargo test arrow_output::
```

## Running Benchmarks

### Prerequisites

Benchmarks use [Criterion.rs](https://github.com/bheisler/criterion.rs) for statistical analysis.

### All Benchmarks

```bash
# Run all benchmarks
cargo bench

# This runs both read_benchmarks and schema_benchmarks
```

### Read Performance Benchmarks

```bash
# Run only read benchmarks
cargo bench --bench read_benchmarks

# Run specific benchmark group
cargo bench --bench read_benchmarks batch_reading
cargo bench --bench read_benchmarks parallel_reading
```

**Available benchmarks:**
- `open_test1` - File opening overhead
- `read_small_file` - Reading small files (test1)
- `read_large_file` - Reading large files (1M rows)
- `read_compressed` - Decompression performance
- `batch_reading` - Different batch sizes
- `parallel_reading` - Different thread counts
- `metadata_access` - Metadata retrieval speed
- `multiple_files` - Opening/reading multiple files
- `file_types` - Compare numeric vs compressed files

### Schema Inference Benchmarks

```bash
# Run only schema benchmarks
cargo bench --bench schema_benchmarks

# Run specific schema benchmark
cargo bench --bench schema_benchmarks default_vs_inferred
```

**Available benchmarks:**
- `schema_inference` - Time to infer schema
- `default_vs_inferred` - Compare default vs inferred read
- `schema_inference_only` - Just schema inference time
- `schema_cast_overhead` - Type casting overhead
- `arrow_conversion` - Arrow conversion performance
- `streaming_with_schema` - Streaming with schema
- `batch_size_schema` - Batch size impact with schema
- `memory_schema` - Large file memory impact

### Benchmark Comparison

Save a baseline and compare after changes:

```bash
# Save current performance as baseline
cargo bench -- --save-baseline before

# Make your changes...

# Compare against baseline
cargo bench -- --baseline before

# View detailed comparison
open target/criterion/report/index.html
```

### Benchmark Output

Criterion generates detailed reports in `target/criterion/`:
- HTML reports with graphs
- Statistical analysis
- Comparison with previous runs

View the reports:
```bash
# Open HTML report in browser
open target/criterion/report/index.html

# Or on Linux
xdg-open target/criterion/report/index.html
```

## Advanced Testing

### Faster Test Running with cargo-nextest

Install cargo-nextest for faster test execution:

```bash
cargo install cargo-nextest

# Run tests with nextest (much faster)
cargo nextest run

# Run specific test suite
cargo nextest run --test integration_tests
```

### Auto-run Tests on File Changes

Install cargo-watch:

```bash
cargo install cargo-watch

# Auto-run tests when files change
cargo watch -x test

# Auto-run specific test
cargo watch -x "test --test integration_tests"

# Auto-run benchmarks
cargo watch -x bench
```

### Code Coverage

Install cargo-tarpaulin:

```bash
cargo install cargo-tarpaulin

# Generate coverage report
cargo tarpaulin --out Html

# Open coverage report
open tarpaulin-report.html
```

## Continuous Integration

### GitHub Actions Example

Create `.github/workflows/test.yml`:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Run tests
        run: cargo test --all
      - name: Run benchmarks
        run: cargo bench --no-run  # Compile but don't run

  benchmark:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v2
      - name: Benchmark
        run: cargo bench -- --save-baseline pr-${{ github.event.number }}
```

## Test Best Practices

### Before Committing

```bash
# Full test suite
cargo test

# Check formatting
cargo fmt --check

# Check for warnings
cargo clippy

# Run benchmarks to check for regressions
cargo bench
```

### Adding New Tests

1. **Integration test** - Add to `tests/integration_tests.rs` if testing overall functionality
2. **Regression test** - Add to `tests/regression_tests.rs` if verifying specific values
3. **Unit test** - Add to source file's `#[cfg(test)] mod tests` if testing internal logic
4. **Benchmark** - Add to appropriate bench file if measuring performance

### Writing Good Tests

```rust
#[test]
fn test_descriptive_name() {
    // Arrange - set up test data
    let path = test_data_path("test1.sas7bdat");

    // Act - perform the action
    let reader = Sas7bdatReader::open(&path).unwrap();
    let df = reader.read_all().unwrap();

    // Assert - verify the result
    assert_eq!(df.height(), 10);
    assert_eq!(df.width(), 4);
}
```

## Performance Tracking

### Establishing Baselines

When setting up performance tracking:

```bash
# 1. Run benchmarks on main branch
git checkout main
cargo bench -- --save-baseline main

# 2. Switch to feature branch
git checkout feature-branch

# 3. Make changes and benchmark
cargo bench -- --baseline main

# 4. Review differences
# Criterion will show % change from baseline
```

### Interpreting Results

- **Green** (improvement): Your change made it faster
- **Red** (regression): Your change made it slower
- **Yellow** (noise): No significant change

Criterion uses statistical analysis to filter out noise.

## Troubleshooting

### Tests Failing

```bash
# Run with verbose output
cargo test -- --nocapture --test-threads=1

# Run specific failing test
cargo test test_name -- --exact --nocapture
```

### Benchmarks Not Running

```bash
# Ensure criterion is installed
cargo bench --version

# Compile benchmarks without running
cargo bench --no-run

# Check for compilation errors
cargo check --benches
```

### Missing Test Files

If tests fail because test files are missing:

```bash
# Check if test data exists
ls tests/sas/data/

# The test suite expects SAS7BDAT files in tests/sas/data/
# Make sure your test files are in the correct location
```

## Summary

| Command | Purpose |
|---------|---------|
| `cargo test` | Run all tests |
| `cargo test --test integration_tests` | Run integration tests |
| `cargo test --test regression_tests` | Run regression tests |
| `cargo bench` | Run all benchmarks |
| `cargo bench --bench read_benchmarks` | Run read benchmarks |
| `cargo bench --bench schema_benchmarks` | Run schema benchmarks |
| `cargo bench -- --save-baseline name` | Save performance baseline |
| `cargo bench -- --baseline name` | Compare against baseline |
| `cargo nextest run` | Fast test runner |
| `cargo watch -x test` | Auto-run tests on changes |
| `cargo tarpaulin --out Html` | Generate coverage report |

## Next Steps

1. Run `cargo test` to verify all tests pass
2. Run `cargo bench` to establish performance baselines
3. Set up CI/CD with GitHub Actions
4. Add new tests as you develop features
5. Monitor benchmarks to catch performance regressions
