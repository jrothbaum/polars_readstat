# Arrow FFI Export

The Rust crate exposes Arrow C Data Interface helpers for SAS, Stata, and SPSS.
These are intended for embedders such as DuckDB extensions that want an
`ArrowArrayStream` or one-shot `ArrowArray` without going through the Python API.

## Modules

```rust
use polars_readstat_rs::{
    sas_arrow_output,
    spss_arrow_output,
    stata_arrow_output,
};
```

Each module exports:

- `read_to_arrow_schema_ffi(...) -> PolarsResult<*mut ArrowSchema>`
- `read_to_arrow_array_ffi(...) -> PolarsResult<(*mut ArrowSchema, *mut ArrowArray)>`
- `read_to_arrow_stream_ffi(...) -> PolarsResult<*mut ArrowArrayStream>`

The returned raw pointers are owned by the caller and must be released through
the normal Arrow C Data Interface release callbacks.

## SAS

```rust
use polars_readstat_rs::sas_arrow_output;
use std::path::Path;

let path = Path::new("file.sas7bdat");

let schema = sas_arrow_output::read_to_arrow_schema_ffi(
    path,
    None, // threads
    true, // missing_string_as_null
    None, // chunk_size
)?;

let stream = sas_arrow_output::read_to_arrow_stream_ffi(
    path,
    None, // threads
    true, // missing_string_as_null
    Some(100_000), // chunk_size
    0,    // row offset
    None, // row limit
)?;
```

## Stata and SPSS

Stata and SPSS also accept `value_labels_as_strings`.

```rust
use polars_readstat_rs::{spss_arrow_output, stata_arrow_output};
use std::path::Path;

let dta_stream = stata_arrow_output::read_to_arrow_stream_ffi(
    Path::new("file.dta"),
    None,        // threads
    true,        // missing_string_as_null
    Some(false), // value_labels_as_strings
    Some(100_000), // chunk_size
    0,           // row offset
    None,        // row limit
)?;

let sav_stream = spss_arrow_output::read_to_arrow_stream_ffi(
    Path::new("file.sav"),
    None,        // threads
    true,        // missing_string_as_null
    Some(false), // value_labels_as_strings
    Some(100_000), // chunk_size
    0,           // row offset
    None,        // row limit
)?;
```

## Notes

- `offset` and `n_rows` are applied to stream output.
- Schema and one-shot array helpers read the full schema/data for the selected
  format and do not take `offset` or `n_rows`.
- Arrow output uses the same decoding behavior as the Polars scan paths.
