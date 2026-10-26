//! XPT and POR read through the Arrow C Data Interface, from a caller-supplied source,
//! compared with the library's own scans of the same files.

use polars::prelude::*;
use polars_arrow::ffi::{ArrowArrayStream, ArrowArrayStreamReader, ArrowSchema};
use polars_readstat_rs::sas_arrow_output::{
    read_xpt_to_arrow_schema_ffi_from_source, read_xpt_to_arrow_stream_ffi_from_source,
};
use polars_readstat_rs::spss_arrow_output::{
    read_por_to_arrow_schema_ffi_from_source, read_por_to_arrow_stream_ffi_from_source,
};
use polars_readstat_rs::{scan_por, LocalFileSource, ReadSource, ScanOptions};
use std::path::PathBuf;
use std::sync::Arc;

fn data(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join(rel)
}

fn source(path: &PathBuf) -> Arc<dyn ReadSource> {
    Arc::new(LocalFileSource::new(path))
}

fn schema_names(ptr: *mut ArrowSchema) -> Vec<String> {
    let schema = unsafe { *Box::from_raw(ptr) };
    let field = unsafe { polars_arrow::ffi::import_field_from_c(&schema) }.unwrap();
    match field.dtype() {
        polars_arrow::datatypes::ArrowDataType::Struct(fields) => fields.iter().map(|f| f.name.to_string()).collect(),
        other => panic!("expected a struct, got {other:?}"),
    }
}

fn stream_rows_and_columns(ptr: *mut ArrowArrayStream) -> (usize, usize) {
    let stream: Box<ArrowArrayStream> = unsafe { Box::from_raw(ptr) };
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(stream) }.expect("valid stream");
    let (mut rows, mut cols) = (0, 0);
    while let Some(array) = unsafe { reader.next() } {
        let array = array.expect("batch");
        rows += array.len();
        let structs = array.as_any().downcast_ref::<polars_arrow::array::StructArray>().unwrap();
        cols = structs.values().len();
    }
    (rows, cols)
}

#[test]
fn xpt_fixtures_match_the_library_scan() {
    for name in ["sample.xpt", "sas.xpt5", "sas.xpt8", "dates_xpt_v8.xpt", "ACQ_G.xpt"] {
        let path = data(&format!("sas/data/xpt/{name}"));
        let expected = polars_readstat_rs::readstat_scan(&path, None, None).unwrap().collect().unwrap();

        let names = schema_names(read_xpt_to_arrow_schema_ffi_from_source(source(&path), None).unwrap());
        assert_eq!(names, expected.get_column_names().iter().map(|s| s.to_string()).collect::<Vec<_>>(), "{name}");

        let ptr = read_xpt_to_arrow_stream_ffi_from_source(
            source(&path), None, false, None, 0, None, true, None, None,
        )
        .unwrap();
        assert_eq!(stream_rows_and_columns(ptr), (expected.height(), expected.width()), "{name}");
    }
}

#[test]
fn xpt_column_subset_row_window_and_row_index() {
    let path = data("sas/data/xpt/ACQ_G.xpt");
    let full = polars_readstat_rs::readstat_scan(&path, None, None).unwrap().collect().unwrap();
    assert!(full.width() >= 3);

    // the last two columns, in reverse order, rows 2..5
    let last = full.width();
    let ptr = read_xpt_to_arrow_stream_ffi_from_source(
        source(&path), None, false, None, 2, Some(3), true, None, Some(vec![last - 1, last - 2]),
    )
    .unwrap();
    assert_eq!(stream_rows_and_columns(ptr), (3, 2));

    // a row-index column alongside a data column
    let schema = schema_names(read_xpt_to_arrow_schema_ffi_from_source(source(&path), Some("rownum")).unwrap());
    assert_eq!(schema.last().map(String::as_str), Some("rownum"));
    let ptr = read_xpt_to_arrow_stream_ffi_from_source(
        source(&path), None, false, None, 0, None, true, Some("rownum".into()), Some(vec![0, schema.len() - 1]),
    )
    .unwrap();
    assert_eq!(stream_rows_and_columns(ptr), (full.height(), 2));
}

#[test]
fn por_fixture_matches_the_library_scan() {
    let path = data("spss/data/sample.por");
    let expected = scan_por(&path, ScanOptions::default()).unwrap().collect().unwrap();

    let names = schema_names(read_por_to_arrow_schema_ffi_from_source(source(&path), None).unwrap());
    assert_eq!(names, expected.get_column_names().iter().map(|s| s.to_string()).collect::<Vec<_>>());

    let ptr = read_por_to_arrow_stream_ffi_from_source(source(&path), false, None, 0, None, None, None).unwrap();
    assert_eq!(stream_rows_and_columns(ptr), (expected.height(), expected.width()));

    // small batches, a row window and a column subset
    let ptr = read_por_to_arrow_stream_ffi_from_source(source(&path), false, Some(2), 1, Some(3), None, Some(vec![1, 0])).unwrap();
    assert_eq!(stream_rows_and_columns(ptr), (3.min(expected.height().saturating_sub(1)), 2));

    // row index
    let ptr = read_por_to_arrow_stream_ffi_from_source(source(&path), false, None, 0, None, Some("rownum".into()), Some(vec![0, expected.width()])).unwrap();
    assert_eq!(stream_rows_and_columns(ptr), (expected.height(), 2));
}
