//! `read_to_arrow_schema_ffi_from_source`/`read_to_arrow_stream_ffi_from_source`
//! reuse the exact same decode machinery as the path-based FFI functions —
//! the only difference is how the `Sas7bdatReader`/`ReadSource` gets
//! constructed. Confirms that equivalence holds (same schema, same row
//! count) across the fixture corpus, for a source explicitly wrapping a
//! local file path (the same `LocalFileSource` the path-based functions
//! build internally).

mod common;

use common::sas_files;
use polars_arrow::ffi::{ArrowArrayStream, ArrowArrayStreamReader};
use polars_readstat_rs::sas_arrow_output::{
    read_to_arrow_schema_ffi, read_to_arrow_schema_ffi_from_source, read_to_arrow_stream_ffi,
    read_to_arrow_stream_ffi_from_source,
};
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

fn stream_row_count(ptr: *mut ArrowArrayStream) -> usize {
    let boxed: Box<ArrowArrayStream> = unsafe { Box::from_raw(ptr) };
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(boxed) }.expect("valid stream");
    let mut total = 0usize;
    while let Some(array) = unsafe { reader.next() } {
        let array = array.expect("batch ok");
        total += array.len();
    }
    total
}

#[test]
fn from_source_matches_path_for_all_sas_files() {
    let files = sas_files();
    assert!(!files.is_empty(), "expected at least one SAS7BDAT fixture");

    let mut mismatches = Vec::new();

    for path in &files {
        let path_str = path.display().to_string();

        let schema_via_path = match read_to_arrow_schema_ffi(path, None, true, None) {
            Ok(p) => schema_field_names(p),
            Err(e) => {
                mismatches.push(format!("{path_str}: path schema failed: {e}"));
                continue;
            }
        };
        let source = Arc::new(LocalFileSource::new(path)) as Arc<dyn polars_readstat_rs::ReadSource>;
        let schema_via_source = match read_to_arrow_schema_ffi_from_source(source.clone()) {
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

        let rows_via_path = match read_to_arrow_stream_ffi(path, None, true, None, 0, None) {
            Ok(p) => stream_row_count(p),
            Err(e) => {
                mismatches.push(format!("{path_str}: path stream failed: {e}"));
                continue;
            }
        };
        let rows_via_source =
            match read_to_arrow_stream_ffi_from_source(source, None, true, None, 0, None) {
                Ok(p) => stream_row_count(p),
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
        "from_source mismatches:\n{}",
        mismatches.join("\n")
    );
}
