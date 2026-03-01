# Schema Inference and Streaming

The SAS7BDAT reader supports schema inference and schema-based streaming for both Polars and Arrow formats.

## Overview

This enables two-pass reading:
1. **Pass 1 (Scan)**: Infer optimal schema by analyzing all data
2. **Pass 2 (Stream)**: Read data in batches with schema casting applied

**Key feature**: Batches are cast individually as they're read (in parallel), not after concatenation.

## Why Infer Schemas?

SAS stores all numeric data as `float64`, but many columns contain:
- **Integers** (IDs, counts, years)
- **Small integers** (age, status codes)
- **Booleans** (0/1 flags)

Using optimal types provides:
- **Reduced memory usage** (int8 = 1 byte vs float64 = 8 bytes)
- **Better Parquet compression** (integer encoding is more efficient)
- **More accurate schema** for downstream tools (preserves semantic meaning)

## Quick Start

### Arrow Streaming

```rust
use polars_readstat_rs::arrow_output::{infer_arrow_schema, ArrowBatchStream};

// Pass 1: Infer schema
let schema = infer_arrow_schema("data.sas7bdat", 10000, true)?;

// Pass 2: Stream with schema (batches cast as they're read)
let mut stream = ArrowBatchStream::with_schema("data.sas7bdat", 10000, schema)?;

while let Some(batch) = stream.next_batch()? {
    // Each batch uses optimized types (int8/16/32/64, uint8/16/32/64, Boolean)
}
```

### Polars Reading

```rust
use polars_readstat_rs::reader::Sas7bdatReader;
use polars::prelude::*;
use std::sync::Arc;

let reader = Sas7bdatReader::open("data.sas7bdat")?;

// Define custom schema
let schema = Schema::from_iter(vec![
    Field::new("age", DataType::UInt8),
    Field::new("income", DataType::Int32),
]);

// Read with schema (parallel batches are cast individually)
let df = reader.read_all_with_schema(Arc::new(schema))?;
```

## API Reference

### Simple Functions

```rust
// Infer schema only (no data read)
let schema = infer_arrow_schema("data.sas7bdat", 10000, true)?;

// Read with inferred schema (two-pass)
let batch = read_to_arrow_inferred("data.sas7bdat", true)?;

// Read with custom schema
let schema = infer_arrow_schema("data.sas7bdat", 10000, false)?;
let reader = Sas7bdatReader::open("data.sas7bdat")?;
let df = reader.read_all()?;
let batch = dataframe_to_arrow_with_schema(df, schema)?;
```

### Parameters

- **`infer_boolean`**: If `true`, detect 0/1-only columns as `Boolean` type
  - When `false`, these become `UInt8` instead
  - Use `true` for semantic correctness, `false` if you need numeric operations

- **`batch_size`**: Batch size for scanning (only in `infer_arrow_schema`)
  - Larger = faster scan but more memory
  - Default: 10,000 rows

## Type Inference Rules

The inference algorithm scans all values in each numeric column:

1. **Track min, max, integer-ness, boolean-ness**
2. **Determine optimal type:**
   - All values are 0.0 or 1.0 → `Boolean` (if `infer_boolean=true`)
   - All values are integers:
     - All ≥ 0:
       - max ≤ 255 → `UInt8`
       - max ≤ 65,535 → `UInt16`
       - max ≤ 4,294,967,295 → `UInt32`
       - else → `UInt64`
     - Has negatives:
       - -128 ≤ min, max ≤ 127 → `Int8`
       - -32,768 ≤ min, max ≤ 32,767 → `Int16`
       - -2,147,483,648 ≤ min, max ≤ 2,147,483,647 → `Int32`
       - else → `Int64`
   - Has fractional values → `Float64`

## Examples

### Example 1: Basic Inference

```rust
use polars_readstat_rs::arrow_output::{read_to_arrow, read_to_arrow_inferred};

// Default: all numerics as Float64
let batch_default = read_to_arrow("data.sas7bdat")?;
println!("{:?}", batch_default.schema());
// Schema: [age: Float64, income: Float64, active: Float64]

// Inferred: optimal types
let batch_inferred = read_to_arrow_inferred("data.sas7bdat", true)?;
println!("{:?}", batch_inferred.schema());
// Schema: [age: UInt8, income: Int32, active: Boolean]
```

### Example 2: Parquet Export with Optimized Schema

```rust
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;

// Read with inferred schema
let batch = read_to_arrow_inferred("data.sas7bdat", true)?;

// Write optimized Parquet
let file = File::create("output.parquet")?;
let props = WriterProperties::builder()
    .set_compression(parquet::basic::Compression::SNAPPY)
    .build();

let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
writer.write(&batch)?;
writer.close()?;

// Result: smaller file size due to integer encoding
```

### Example 3: Custom Schema Control

```rust
use polars_readstat_rs::arrow_output::{infer_arrow_schema, dataframe_to_arrow_with_schema};
use polars_readstat_rs::reader::Sas7bdatReader;

// Infer without boolean detection
let schema = infer_arrow_schema("data.sas7bdat", 10000, false)?;

// Manually modify schema if needed
// (e.g., force certain columns to Int64)

// Read with custom schema
let reader = Sas7bdatReader::open("data.sas7bdat")?;
let df = reader.read_all()?;
let batch = dataframe_to_arrow_with_schema(df, schema)?;
```

## Streaming Benefits

### Why Stream with Schema?

1. **Memory Efficiency**: Don't need to load entire file at once
2. **Parallel Casting**: Each batch is cast individually in parallel workers
3. **Progressive I/O**: Can write to Parquet/IPC as data streams in
4. **Large File Support**: Works with files too big for memory

### Example: Stream to Parquet

```rust
use polars_readstat_rs::arrow_output::{infer_arrow_schema, ArrowBatchStream};
use parquet::arrow::ArrowWriter;
use std::fs::File;

// Pass 1: Infer schema
let schema = infer_arrow_schema("large.sas7bdat", 10000, true)?;

// Pass 2: Stream to Parquet with inferred schema
let mut stream = ArrowBatchStream::with_schema("large.sas7bdat", 10000, schema.clone())?;
let file = File::create("output.parquet")?;
let mut writer = ArrowWriter::try_new(file, schema, None)?;

while let Some(batch) = stream.next_batch()? {
    writer.write(&batch)?;  // Each batch already optimized
}
writer.close()?;
```

## Performance

**Two-pass reading cost:**
- First pass: scan all data (decompress + parse, ~50% of single-pass)
- Second pass: stream with cast (decompress + parse + cast)
- Total: ~1.5× single-pass cost (scan is cheaper than full read)

**Streaming advantages:**
- Batches cast in parallel (if uncompressed)
- No large memory spike for concatenation
- Can write output progressively

**When to use:**
- Large files with many integer columns
- Exporting to Parquet/IPC (better compression)
- Files too large for memory
- Downstream tools that care about types (SQL databases, BI tools)

**When NOT to use:**
- Small files (overhead not worth it)
- Temporary analysis (types don't matter)
- Only need subset of rows (better to read slice directly)

## Memory Savings

Typical savings depend on column types:

| Column Type     | Default | Inferred | Savings |
|----------------|---------|----------|---------|
| Age (0-120)    | Float64 (8 bytes) | UInt8 (1 byte) | 87.5% |
| ID (0-1M)      | Float64 (8 bytes) | UInt32 (4 bytes) | 50% |
| Flag (0/1)     | Float64 (8 bytes) | Boolean (1 bit) | 93.75% |
| Income (±2B)   | Float64 (8 bytes) | Int32 (4 bytes) | 50% |
| Amount (floats)| Float64 (8 bytes) | Float64 (8 bytes) | 0% |

**Overall savings:** typically 30-60% depending on data characteristics.

## Limitations

1. **Two-pass I/O**: Reads file twice (scan + read)
2. **Lossy casting**: Float64 → Int truncates fractional parts (shouldn't exist if inferred as int)
3. **No overflow checking**: Trusts inferred min/max are accurate
4. **String columns**: Always `Utf8`, no optimization

## Example

See [examples/infer_arrow_schema.rs](examples/infer_arrow_schema.rs) for a complete working example.
