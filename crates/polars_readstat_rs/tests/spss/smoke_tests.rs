use polars_arrow::ffi::ArrowArrayStreamReader;
use polars_readstat_rs::spss::arrow_output::{read_to_arrow_ffi, read_to_arrow_stream_ffi};
use polars_readstat_rs::SpssReader;
use std::path::PathBuf;

fn test_data_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("spss")
        .join("data")
        .join(filename)
}

#[test]
fn test_read_uncompressed_file() {
    let path = test_data_path("hebrews.sav");
    let reader = SpssReader::open(&path).expect("open");
    let df = reader.read().with_limit(5).finish().expect("read");
    assert!(df.height() > 0);
    assert!(df.width() > 0);
}

#[test]
fn test_read_compressed_file() {
    let path = test_data_path("sample.sav");
    let reader = SpssReader::open(&path).expect("open");
    let df = reader.read().with_limit(5).finish().expect("read");
    assert!(df.height() > 0);
    assert!(df.width() > 0);
}

#[test]
fn test_read_zsav_file() {
    let path = test_data_path("sample.zsav");
    let reader = SpssReader::open(&path).expect("open");
    let df = reader.read().with_limit(5).finish().expect("read");
    assert!(df.height() > 0);
    assert!(df.width() > 0);
}

#[test]
fn test_read_value_labels_as_strings() {
    let path = test_data_path("ordered_category.sav");
    let reader = SpssReader::open(&path).expect("open");
    let df = reader
        .read()
        .value_labels_as_strings(true)
        .with_limit(5)
        .finish()
        .expect("read");
    assert!(df.height() > 0);
    assert!(df.width() > 0);
}

#[test]
fn test_very_long_string_metadata() {
    let path = test_data_path("test_width.sav");
    let reader = SpssReader::open(&path).expect("open");
    let var = reader
        .metadata()
        .variables
        .iter()
        .find(|v| {
            v.short_name.eq_ignore_ascii_case("STARTDAT") || v.name.eq_ignore_ascii_case("STARTDAT")
        })
        .expect("STARTDAT variable");
    assert!(
        var.string_len >= 1024,
        "expected STARTDAT string_len >= 1024"
    );
    assert!(
        var.width * 8 >= var.string_len,
        "expected width bytes to cover string_len"
    );
}

#[test]
fn test_long_string_metadata() {
    let path = test_data_path("tegulu.sav");
    let reader = SpssReader::open(&path).expect("open");
    let var = reader
        .metadata()
        .variables
        .iter()
        .find(|v| {
            v.short_name.eq_ignore_ascii_case("Q16BR9OE") || v.name.eq_ignore_ascii_case("Q16BR9OE")
        })
        .expect("Q16BR9OE variable");
    assert!(var.string_len >= 512, "expected Q16BR9OE string_len >= 512");
    assert!(
        var.width * 8 >= var.string_len,
        "expected width bytes to cover string_len"
    );
}

#[test]
fn test_arrow_export() {
    let path = test_data_path("sample.sav");
    let (schema, array) = read_to_arrow_ffi(&path).expect("arrow export");
    unsafe {
        drop(Box::from_raw(schema));
        drop(Box::from_raw(array));
    }
}

#[test]
fn test_arrow_stream_export() {
    let path = test_data_path("sample.sav");
    let stream =
        read_to_arrow_stream_ffi(&path, None, true, Some(true), None, 2).expect("arrow stream");
    let mut reader =
        unsafe { ArrowArrayStreamReader::try_new(Box::from_raw(stream)) }.expect("stream reader");
    let mut count = 0usize;
    unsafe {
        while let Some(batch) = reader.next() {
            let _batch = batch.expect("batch");
            count += 1;
            if count >= 2 {
                break;
            }
        }
    }
    assert!(count > 0);
}
