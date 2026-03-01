# polars_readstat_rs

Rust library for reading SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files with Polars.

The crate provides:
- format-specific readers (`Sas7bdatReader`, `StataReader`, `SpssReader`)
- a format-agnostic scan API (`readstat_scan`)
- metadata/schema helpers
- Arrow FFI export helpers
- Stata/SPSS writers

## Performance at a glance

Benchmarks run via the [`polars_readstat`](https://github.com/jrothbaum/polars_readstat) Python bindings (AMD Ryzen 7 8845HS, 16 cores, external SSD), comparing against pandas:

| Format | Full file | Column subset | Row filter | Subset + filter |
|--------|----------:|-------------:|-----------:|----------------:|
| SAS    | 2.9×      | **51.5×**    | 2.9×       | **52.5×**       |
| Stata  | 6.7×      | 9.8×         | 4.1×       | 8.7×            |
| SPSS   | 16×       | 25×          | 15×        | **64×**         |

The largest gains come from column projection: the reader skips parsing unwanted columns entirely rather than reading and discarding them. Speedups will vary with file shape, storage, and hardware.

SPSS reading was significantly improved in v0.3. The library also handles large SPSS files (600MB+) that crash pandas due to memory exhaustion.

SPSS run times (`anes_timeseries_cdf.sav`, 73,745 rows × 1,030 cols, 87 MB — one run per scenario):

```
                  Full file   Subset   Filter   Subset+Filter
polars_readstat    1.30       0.74     1.24     0.28
pandas            21.26      18.17    17.94    17.93
pyreadstat         6.37       1.58     5.49     1.43
```

For full methodology and tables, see [BENCHMARKS.md](https://github.com/jrothbaum/polars_readstat/blob/main/BENCHMARKS.md).

This was nearly completely coded by Claude Code and Codex, but with a very particular setup that I hope makes it less likely to be a mess than other moslty-AI code repository.  It was meant to directly replace the C++ and C code in relatively small, existing codebase ([polars_readstat, v0.11.1](https://github.com/jrothbaum/polars_readstat/releases/tag/v0.11.1)) with the ability to exactly validate the new code's output against the old.  For any given regression, the AI models could be told to refer directly to the spot in the code where the prior implementation did the same operation to try to figure out how to solve the issue.  It could also compare any output to the output produced by other similar tools such as [pandas](https://github.com/pandas-dev/pandas) and [pyreadstat](https://github.com/Roche/pyreadstat/).  I'm sure it's not the most beautiful code, but I'm an economist and I wanted a tool to exist that was faster than what's out there and implemented in Rust (so many build issues with using C++ and C across systems.  So many...) but I didn't want to spend months on figuring out records layouts and encoding of SAS, Stata, and SPSS files.  Hence, my first attempt that just directly plugged into other tools ([polars_readstat, v0.11.1](https://github.com/jrothbaum/polars_readstat/releases/tag/v0.11.1)) and the AI-first version of this.

## Install
```toml
[dependencies]
polars_readstat_rs = "0.2"
```

## Core API

### 1) Read directly to a `DataFrame`
```rust
use polars_readstat_rs::Sas7bdatReader;

let df = Sas7bdatReader::open("file.sas7bdat")?
    .read()
    .finish()?;
```

### 2) ReadBuilder options

All three readers expose the same fluent builder. Options and their defaults:

| Method | Type | Default | Notes |
|---|---|---|---|
| `.with_columns(cols)` | `Vec<String>` | all columns | column projection |
| `.with_offset(n)` | `usize` | `0` | skip first N rows |
| `.with_limit(n)` | `usize` | all rows | read at most N rows |
| `.with_n_threads(n)` | `usize` | rayon default | parallel thread count |
| `.with_chunk_size(n)` | `usize` | auto | rows per parallel chunk |
| `.with_schema(schema)` | `Arc<Schema>` | none | cast columns after read |
| `.sequential()` | — | parallel on | force single-threaded read |
| `.missing_string_as_null(bool)` | `bool` | `true` | treat empty/missing strings as null |
| `.value_labels_as_strings(bool)` | `bool` | `true` | decode value labels as strings *(Stata/SPSS only)* |
| `.informative_nulls(opts)` | `Option<InformativeNullOpts>` | `None` | capture user-defined missing value indicators (see [Informative Nulls](#informative-nulls)) |

```rust
use polars_readstat_rs::StataReader;

let df = StataReader::open("file.dta")?
    .read()
    .with_columns(vec!["serial".to_string(), "age".to_string()])
    .with_offset(1_000)
    .with_limit(5_000)
    .with_n_threads(4)
    .missing_string_as_null(true)
    .value_labels_as_strings(true)
    .finish()?;
```

Use `.finish_profiled()` instead of `.finish()` to get timing breakdowns:

```rust
let (df, profile) = StataReader::open("file.dta")?.read().finish_profiled()?;
println!("total ms: {}", profile.total_ms);
```

SAS has two additional pipeline modes that can improve throughput for compressed files:

```rust
use polars_readstat_rs::Sas7bdatReader;

// pipeline: better for RLE/RDC-compressed SAS files
let df = Sas7bdatReader::open("file.sas7bdat")?
    .read()
    .pipeline()
    .pipeline_chunk_size(50_000)
    .finish()?;
```

### 3) Format-agnostic lazy scan

`ScanOptions` controls parallelism and output behavior for `readstat_scan` and `readstat_batch_iter`.
All fields are `Option`; `None` uses the shown default.

```rust
use polars::prelude::*;
use polars_readstat_rs::{readstat_scan, CompressOptionsLite, ScanOptions};

let opts = ScanOptions {
    threads: Some(4),
    chunk_size: Some(100_000),
    missing_string_as_null: Some(true),  // default: true
    value_labels_as_strings: Some(true), // default: true (Stata/SPSS)
    preserve_order: Some(false),         // default: false; true = deterministic row order when parallel
    informative_nulls: None,             // see Informative Nulls section
    compress_opts: CompressOptionsLite {
        enabled: false,              // post-read type narrowing (disabled by default)
        cols: None,                  // columns to compress; None = all
        compress_numeric: false,     // downcast integers/floats to smallest fitting type
        datetime_to_date: false,     // convert datetimes to dates where possible
        string_to_numeric: false,    // parse numeric strings as numbers
    },
};

let lf = readstat_scan("file.sav", Some(opts), None)?;
let out = lf.select([col("id"), col("income")]).collect()?;
```

### 4) Format-agnostic batch streaming (no full materialization)
```rust
use polars_readstat_rs::{readstat_batch_iter, ReadStatFormat, ScanOptions};

let opts = ScanOptions {
    threads: Some(4),
    chunk_size: Some(100_000),
    preserve_order: Some(true), // keep row order deterministic when parallel
    ..Default::default()
};

let mut iter = readstat_batch_iter(
    "file.sas7bdat",
    Some(opts),
    Some(ReadStatFormat::Sas), // None = auto-detect from extension
    None,                      // column projection (None = all)
    Some(200_000),             // total row limit
    Some(100_000),             // batch size (overrides chunk_size)
)?;

while let Some(batch) = iter.next() {
    let df = batch?;
    // process batch
}
```

`ReadstatBatchStream` wraps the iterator with a `next_batch() -> PolarsResult<Option<DataFrame>>` interface:

```rust
use polars_readstat_rs::ReadstatBatchStream;

let mut stream = ReadstatBatchStream::new("file.dta", None, None, None, None, Some(50_000))?;
while let Some(df) = stream.next_batch()? {
    // process batch
}
```

When scanning with multiple threads, `preserve_order = false` (default) allows batches to be emitted out of order for higher throughput. Set `true` for deterministic row order.

### 5) Metadata and schema
```rust
use polars_readstat_rs::{readstat_metadata_json, readstat_schema};

let metadata_json = readstat_metadata_json("file.dta", None)?;
let schema = readstat_schema("file.dta", None, None)?;
```

### 6) Writing Stata

```rust
use polars_readstat_rs::StataWriter;

// Simple write
StataWriter::new("out.dta").write_df(&df)?;
```

`StataWriter` supports value labels, variable labels, compression, and thread control:

```rust
use polars_readstat_rs::{
    StataWriteColumn, StataWriteSchema, StataWriter, ValueLabelMap, ValueLabels, VariableLabels,
};
use polars::prelude::DataType;
use std::collections::{BTreeMap, HashMap};

let mut status_map: ValueLabelMap = BTreeMap::new();
status_map.insert(1, "Active".to_string());
status_map.insert(2, "Inactive".to_string());
let value_labels: ValueLabels = HashMap::from([("status".to_string(), status_map)]);

let variable_labels: VariableLabels = HashMap::from([
    ("id".to_string(), "Respondent ID".to_string()),
    ("status".to_string(), "Employment Status".to_string()),
]);

StataWriter::new("out.dta")
    .with_value_labels(value_labels)
    .with_variable_labels(variable_labels)
    .with_compress(true)  // downcast numeric types to smallest Stata type
    .with_n_threads(4)
    .write_df(&df)?;
```

For streaming writes when the full DataFrame doesn't fit in memory, provide a schema with `row_count` and pass batches:

```rust
use polars_readstat_rs::{StataWriteColumn, StataWriteSchema, StataWriter};
use polars::prelude::DataType;

let schema = StataWriteSchema {
    columns: vec![
        StataWriteColumn { name: "id".to_string(), dtype: DataType::Int32, string_width_bytes: None },
        StataWriteColumn { name: "name".to_string(), dtype: DataType::String, string_width_bytes: Some(32) },
    ],
    row_count: Some(total_rows),
    value_labels: None,
    variable_labels: None,
};

StataWriter::new("out.dta").write_batches(batch_iter, schema)?;
```

### 7) Writing SPSS
```rust
use polars_readstat_rs::SpssWriter;

// Simple write
SpssWriter::new("out.sav").write_df(&df)?;
```

With schema and labels:

```rust
use polars::prelude::*;
use polars_readstat_rs::{
    SpssValueLabelKey,
    SpssValueLabelMap,
    SpssValueLabels,
    SpssVariableLabels,
    SpssWriteColumn,
    SpssWriteSchema,
    SpssWriter,
};
use std::collections::HashMap;

let df = DataFrame::new(vec![
    Series::new("status".into(), &[1i32, 2, 3]).into_column(),
    Series::new("name".into(), &["alice", "bob", "carol"]).into_column(),
])?;

let schema = SpssWriteSchema {
    columns: vec![
        SpssWriteColumn {
            name: "status".to_string(),
            dtype: DataType::Int32,
            string_width_bytes: None,
        },
        SpssWriteColumn {
            name: "name".to_string(),
            dtype: DataType::String,
            string_width_bytes: Some(16),
        },
    ],
    row_count: Some(df.height()),
    value_labels: None,
    variable_labels: None,
};

let mut status_map: SpssValueLabelMap = HashMap::new();
status_map.insert(SpssValueLabelKey::from_f64(1.0), "one".to_string());
status_map.insert(SpssValueLabelKey::from_f64(2.0), "two".to_string());
status_map.insert(SpssValueLabelKey::from_f64(3.0), "three".to_string());
let value_labels: SpssValueLabels = HashMap::from([("status".to_string(), status_map)]);
let variable_labels: SpssVariableLabels = HashMap::from([
    ("status".to_string(), "Status Label".to_string()),
    ("name".to_string(), "Display Name".to_string()),
]);

SpssWriter::new("out.sav")
    .with_schema(schema)
    .with_value_labels(value_labels)
    .with_variable_labels(variable_labels)
    .write_df(&df)?;
```

SPSS writer behavior and current limits:
- Variable names are validated as non-empty and `<= 64` bytes.
- SPSS short names are generated automatically (ASCII, uppercase, unique, max 8 chars) when needed.
- Strings are fixed-width in bytes and limited to `<= 255` bytes per value.
- Numeric output supports integer/float/bool/date/datetime/time columns (written as SPSS numeric values).
- Value labels are currently supported for numeric variables only.
- String value labels are not currently supported.
- Output encoding is selected automatically: Windows-1252 when possible, otherwise UTF-8 with an SPSS encoding record.

## Informative Nulls

SAS, Stata, and SPSS files support user-defined missing value codes (SAS `.A`–`.Z`, Stata `.a`–`.z`, SPSS discrete/range missings). By default these are read as `null`. The `informative_nulls` option captures the missing-value indicator alongside the data value.

```rust
use polars_readstat_rs::{
    InformativeNullColumns, InformativeNullMode, InformativeNullOpts, StataReader,
};

// Track all eligible columns using the default mode (separate indicator column)
let opts = InformativeNullOpts::new(InformativeNullColumns::All);

let df = StataReader::open("file.dta")?
    .read()
    .informative_nulls(Some(opts))
    .finish()?;
```

**Three output modes:**

| Mode | Description |
|---|---|
| `SeparateColumn { suffix }` (default `"_null"`) | Adds a parallel `String` column `<col><suffix>` after each tracked column |
| `Struct` | Wraps each `(value, indicator)` pair into a `Struct` column |
| `MergedString` | Merges into a single `String` column (value as string, or the indicator code) |

```rust
use polars_readstat_rs::{
    InformativeNullColumns, InformativeNullMode, InformativeNullOpts,
};

let opts = InformativeNullOpts {
    columns: InformativeNullColumns::Selected(vec!["income".into(), "age".into()]),
    mode: InformativeNullMode::SeparateColumn { suffix: "_missing".into() },
    use_value_labels: true, // use the value label for the indicator string when defined
};
```

Works with all three format readers and with `ScanOptions` / `readstat_batch_iter`.

## Arrow export
```rust
use polars_readstat_rs::sas_arrow_output;

let mut schema = sas_arrow_output::read_to_arrow_schema_ffi("file.sas7bdat")?;
let mut stream = sas_arrow_output::read_to_arrow_stream_ffi("file.sas7bdat", Some(100_000))?;
```

See `ARROW_EXPORT.md` for FFI details.

## Basic validation and benchmarks

Compare against Python reference outputs:
```bash
uv run tests/sas/compare_to_python.py
uv run tests/stata/compare_to_python.py --file tests/stata/data/too_big/usa_00009.dta --rows 100000
uv run tests/spss/compare_to_python.py --rows 100000
```

Read performance checks:
```bash
uv run tests/sas/bench_vs_python.py --file tests/sas/data/too_big/psam_p17.sas7bdat --rows 100000 --repeat 2
uv run tests/stata/bench_vs_python.py --file tests/stata/data/too_big/usa_00009.dta --rows 100000 --repeat 2
uv run tests/spss/bench_vs_python.py --file tests/spss/data/too_big/ess_data.sav --rows 100000 --repeat 2
```

Streaming batch benchmark (200k-row cap on `tests/*/data/too_big` files):
```bash
cargo bench --bench readstat_stream_benchmarks
```
