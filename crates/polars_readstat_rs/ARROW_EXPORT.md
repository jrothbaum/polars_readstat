# Arrow RecordBatch Export

The SAS7BDAT reader can export data as Apache Arrow RecordBatches, enabling interoperability with any Arrow-compatible tools.

## Quick Start

```rust
use polars_readstat_rs::arrow_output::read_to_arrow;

// Read entire file as Arrow RecordBatch
let batch = read_to_arrow("data.sas7bdat")?;
println!("Shape: {} rows × {} columns", batch.num_rows(), batch.num_columns());
```

## API Reference

### Simple Functions

```rust
// Read entire file
let batch = read_to_arrow("data.sas7bdat")?;

// Read a slice of rows
let batch = read_batch_to_arrow("data.sas7bdat", offset, count)?;

// Convert Polars DataFrame to Arrow RecordBatch
let df = reader.read_all()?;
let batch = dataframe_to_arrow(df)?;
```

### Streaming Large Files

```rust
use polars_readstat_rs::arrow_output::ArrowBatchStream;

// Stream in 10,000-row batches
let mut stream = ArrowBatchStream::new("large_file.sas7bdat", 10_000)?;

while let Some(batch) = stream.next_batch()? {
    println!("Processing {} rows", batch.num_rows());
    // Process batch...
}
```

## Use Cases

### 1. Convert SAS → Parquet

```rust
use std::fs::File;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

let batch = read_to_arrow("data.sas7bdat")?;
let file = File::create("output.parquet")?;

let props = WriterProperties::builder()
    .set_compression(parquet::basic::Compression::SNAPPY)
    .build();

let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
writer.write(&batch)?;
writer.close()?;
```

### 2. Convert SAS → Arrow IPC/Feather

```rust
use std::fs::File;
use arrow::ipc::writer::FileWriter;

let batch = read_to_arrow("data.sas7bdat")?;
let file = File::create("output.arrow")?;

let mut writer = FileWriter::try_new(file, &batch.schema())?;
writer.write(&batch)?;
writer.finish()?;
```

### 3. Stream Large File to Parquet

```rust
use parquet::arrow::ArrowWriter;

let mut stream = ArrowBatchStream::new("large.sas7bdat", 10_000)?;
let schema = stream.schema()?;

let file = File::create("output.parquet")?;
let mut writer = ArrowWriter::try_new(file, schema, None)?;

while let Some(batch) = stream.next_batch()? {
    writer.write(&batch)?;
}
writer.close()?;
```

### 4. Use with DuckDB (via Arrow)

```python
import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

# After converting to Arrow IPC in Rust
with open('output.arrow', 'rb') as f:
    reader = ipc.open_file(f)
    table = reader.read_all()

# Query with DuckDB
con = duckdb.connect()
result = con.execute("SELECT * FROM table WHERE age > 30").fetchdf()
```

### 5. Use with PyArrow

```python
import pyarrow as pa
import pyarrow.ipc as ipc

# Read Arrow IPC file created from SAS
with open('output.arrow', 'rb') as f:
    reader = ipc.open_file(f)
    table = reader.read_all()

# Convert to Pandas
df = table.to_pandas()

# Or work directly with Arrow
print(f"Shape: {table.num_rows} rows × {table.num_columns} columns")
print(table.schema)
```

## Data Type Mapping

| SAS Type | Polars Type | Arrow Type |
|----------|-------------|------------|
| Numeric  | Float64     | Float64    |
| Character| String      | Utf8       |

## Performance

**Test file:** topical.sas7bdat (84,355 rows × 111 columns)

- **Full file read**: 679ms
- **Stream to Parquet** (9 batches): ~750ms total
- **Arrow overhead**: Minimal (just type conversion)

## Dependencies

```toml
[dependencies]
polars_readstat_rs = "0.1"
arrow = "54.0"

# Optional: for Parquet export
parquet = "54.0"
```

## Example

See [examples/export_to_arrow.rs](examples/export_to_arrow.rs) for a complete working example.

