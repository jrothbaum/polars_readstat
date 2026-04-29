# Read-Side Type Compression

SAS, Stata, and SPSS files often store values wider than needed by downstream
tools. The current public API handles read-side narrowing through
`CompressOptionsLite`, which can be passed in `ScanOptions`.

This is a post-read transform applied to streamed `DataFrame` batches. It is not
the same thing as Stata file compression in the writer.

## Quick Start

```rust
use polars_readstat_rs::{readstat_scan, CompressOptionsLite, ScanOptions};

let opts = ScanOptions {
    compress_opts: CompressOptionsLite {
        enabled: true,
        cols: None,
        compress_numeric: true,
        datetime_to_date: true,
        string_to_numeric: true,
    },
    ..Default::default()
};

let lf = readstat_scan("data.sas7bdat", Some(opts), None)?;
let df = lf.collect()?;
```

## Options

| Field | Default | Description |
|---|---|---|
| `enabled` | `false` | Enable read-side type compression. |
| `cols` | `None` | Restrict compression to selected columns. |
| `compress_numeric` | `false` | Downcast numeric columns where values fit. |
| `datetime_to_date` | `false` | Convert datetime columns to dates where possible. |
| `string_to_numeric` | `false` | Parse numeric strings as numeric columns where safe. |

## Batch Streaming

The same options work with `readstat_batch_iter` and `ReadstatBatchStream`.

```rust
use polars_readstat_rs::{readstat_batch_iter, CompressOptionsLite, ScanOptions};

let opts = ScanOptions {
    chunk_size: Some(100_000),
    compress_opts: CompressOptionsLite {
        enabled: true,
        compress_numeric: true,
        ..Default::default()
    },
    ..Default::default()
};

let mut iter = readstat_batch_iter(
    "data.dta",
    Some(opts),
    None, // auto-detect format
    None, // all columns
    None, // all rows
    Some(100_000),
)?;

while let Some(batch) = iter.next() {
    let df = batch?;
    // process compressed batch
}
```

## Python

The Python package exposes the same behavior as `compress=` on
`scan_readstat()` and `read_readstat()`.

```python
from polars_readstat import scan_readstat

lf = scan_readstat("data.sav", compress=True)
df = lf.collect()
```

For finer control, pass a `CompressOptions` object or a dict with matching
fields.
