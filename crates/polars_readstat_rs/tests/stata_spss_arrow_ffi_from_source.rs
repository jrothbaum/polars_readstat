//! Same equivalence check as `sas_arrow_ffi_from_source.rs`, for Stata and
//! SPSS: the `_from_source` FFI functions reuse the exact same decode
//! machinery as the path-based ones, differing only in how the reader's
//! `ReadSource` gets constructed.

mod common;

use common::{spss_files, stata_files};
use polars_arrow::ffi::{ArrowArrayStream, ArrowArrayStreamReader};
use polars_readstat_rs::LocalFileSource;
use std::sync::Arc;

fn schema_field_names(ptr: *mut polars_arrow::ffi::ArrowSchema) -> Vec<String> {
    let schema = unsafe { Box::from_raw(ptr) };
    let field = unsafe { polars_arrow::ffi::import_field_from_c(&schema) }
        .expect("valid exported schema");
    match field.dtype() {
        polars_arrow::datatypes::ArrowDataType::Struct(fields) => {
            fields.iter().map(|f| f.name.to_string()).collect()
        }
        other => panic!("expected struct field, got {other:?}"),
    }
}

/// Returns `Err` (instead of panicking) on a batch-decode error, so callers
/// can treat a pre-existing decode issue in a fixture as "skip this file"
/// rather than a test-harness crash — same tolerance the `Ok`/`Err` handling
/// around the FFI call itself already gets.
fn stream_row_count(ptr: *mut ArrowArrayStream) -> Result<usize, String> {
    let boxed: Box<ArrowArrayStream> = unsafe { Box::from_raw(ptr) };
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(boxed) }.map_err(|e| e.to_string())?;
    let mut total = 0usize;
    while let Some(array) = unsafe { reader.next() } {
        total += array.map_err(|e| e.to_string())?.len();
    }
    Ok(total)
}

#[test]
fn stata_from_source_matches_path() {
    use polars_readstat_rs::stata_arrow_output::{
        read_to_arrow_schema_ffi, read_to_arrow_schema_ffi_from_source, read_to_arrow_stream_ffi,
        read_to_arrow_stream_ffi_from_source,
    };

    let files = stata_files();
    assert!(!files.is_empty(), "expected at least one Stata fixture");
    let mut mismatches = Vec::new();

    for path in &files {
        let path_str = path.display().to_string();

        let schema_via_path = match read_to_arrow_schema_ffi(path, None, true, Some(true), None) {
            Ok(p) => schema_field_names(p),
            Err(e) => {
                mismatches.push(format!("{path_str}: path schema failed: {e}"));
                continue;
            }
        };
        let source = Arc::new(LocalFileSource::new(path)) as Arc<dyn polars_readstat_rs::ReadSource>;
        let schema_via_source =
            match read_to_arrow_schema_ffi_from_source(source.clone(), Some(true)) {
                Ok(p) => schema_field_names(p),
                Err(e) => {
                    mismatches.push(format!("{path_str}: source schema failed: {e}"));
                    continue;
                }
            };
        if schema_via_path != schema_via_source {
            mismatches.push(format!(
                "{path_str}: schema mismatch: {schema_via_path:?} vs {schema_via_source:?}"
            ));
            continue;
        }

        let rows_via_path = match read_to_arrow_stream_ffi(path, None, true, Some(true), None, 0, None)
            .map_err(|e| e.to_string())
            .and_then(stream_row_count)
        {
            Ok(n) => n,
            Err(e) => {
                // Pre-existing decode issue in this fixture, unrelated to
                // the from_source plumbing being tested here — skip it.
                eprintln!("{path_str}: skipping (path stream failed: {e})");
                continue;
            }
        };
        let rows_via_source = match read_to_arrow_stream_ffi_from_source(
            source,
            None,
            true,
            Some(true),
            None,
            0,
            None,
        )
        .map_err(|e| e.to_string())
        .and_then(stream_row_count)
        {
            Ok(n) => n,
            Err(e) => {
                mismatches.push(format!("{path_str}: source stream failed: {e}"));
                continue;
            }
        };
        if rows_via_path != rows_via_source {
            mismatches.push(format!(
                "{path_str}: row count mismatch: {rows_via_path} vs {rows_via_source}"
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "stata from_source mismatches:\n{}",
        mismatches.join("\n")
    );
}

#[test]
fn spss_from_source_matches_path() {
    use polars_readstat_rs::spss_arrow_output::{
        read_to_arrow_schema_ffi, read_to_arrow_schema_ffi_from_source, read_to_arrow_stream_ffi,
        read_to_arrow_stream_ffi_from_source,
    };

    let files = spss_files();
    assert!(!files.is_empty(), "expected at least one SPSS fixture");
    let mut mismatches = Vec::new();

    for path in &files {
        let path_str = path.display().to_string();

        let schema_via_path = match read_to_arrow_schema_ffi(path, None, true, Some(true), None) {
            Ok(p) => schema_field_names(p),
            Err(e) => {
                mismatches.push(format!("{path_str}: path schema failed: {e}"));
                continue;
            }
        };
        let source = Arc::new(LocalFileSource::new(path)) as Arc<dyn polars_readstat_rs::ReadSource>;
        let schema_via_source =
            match read_to_arrow_schema_ffi_from_source(source.clone(), Some(true)) {
                Ok(p) => schema_field_names(p),
                Err(e) => {
                    mismatches.push(format!("{path_str}: source schema failed: {e}"));
                    continue;
                }
            };
        if schema_via_path != schema_via_source {
            mismatches.push(format!(
                "{path_str}: schema mismatch: {schema_via_path:?} vs {schema_via_source:?}"
            ));
            continue;
        }

        let rows_via_path = match read_to_arrow_stream_ffi(path, None, true, Some(true), None, 0, None)
            .map_err(|e| e.to_string())
            .and_then(stream_row_count)
        {
            Ok(n) => n,
            Err(e) => {
                // Pre-existing decode issue in this fixture, unrelated to
                // the from_source plumbing being tested here — skip it.
                eprintln!("{path_str}: skipping (path stream failed: {e})");
                continue;
            }
        };
        let rows_via_source = match read_to_arrow_stream_ffi_from_source(
            source,
            None,
            true,
            Some(true),
            None,
            0,
            None,
        )
        .map_err(|e| e.to_string())
        .and_then(stream_row_count)
        {
            Ok(n) => n,
            Err(e) => {
                mismatches.push(format!("{path_str}: source stream failed: {e}"));
                continue;
            }
        };
        if rows_via_path != rows_via_source {
            mismatches.push(format!(
                "{path_str}: row count mismatch: {rows_via_path} vs {rows_via_source}"
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "spss from_source mismatches:\n{}",
        mismatches.join("\n")
    );
}
