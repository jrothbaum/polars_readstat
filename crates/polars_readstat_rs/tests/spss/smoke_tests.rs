use polars::prelude::*;
use polars_arrow::ffi::ArrowArrayStreamReader;
use polars_readstat_rs::spss::arrow_output::{read_to_arrow_ffi, read_to_arrow_stream_ffi};
use polars_readstat_rs::{
    spss, SpssAlignment, SpssMeasure, SpssReader, SpssVariableFormat, SpssWriter,
};
use std::collections::HashMap;
use std::path::PathBuf;

fn test_data_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("spss")
        .join("data")
        .join(filename)
}

fn temp_sav_path(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{prefix}_{}_{}.sav", std::process::id(), nanos));
    path
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
fn test_spss_variable_display_metadata() {
    let path = temp_sav_path("spss_display_metadata");
    let df = df! {
        "status" => &[1i32, 2, 3],
        "name" => &["a", "bb", "ccc"],
    }
    .expect("df");

    SpssWriter::new(&path).write_df(&df).expect("write");

    let reader = SpssReader::open(&path).expect("open");
    let status = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "status")
        .expect("status variable");
    assert_eq!(status.format_width, 8);
    assert_eq!(status.format_decimals, 0);
    assert_eq!(
        status.measure.map(|m| format!("{:?}", m)).as_deref(),
        Some("Scale")
    );
    assert_eq!(
        status.alignment.map(|a| format!("{:?}", a)).as_deref(),
        Some("Right")
    );
    assert_eq!(status.display_width, Some(8));

    let name = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "name")
        .expect("name variable");
    assert_eq!(
        name.measure.map(|m| format!("{:?}", m)).as_deref(),
        Some("Nominal")
    );
    assert_eq!(
        name.alignment.map(|a| format!("{:?}", a)).as_deref(),
        Some("Left")
    );

    let metadata: serde_json::Value =
        serde_json::from_str(&spss::metadata_json(&path).expect("metadata json")).expect("json");
    let variables = metadata["variables"].as_array().expect("variables");
    let status_json = variables
        .iter()
        .find(|v| v["name"] == "status")
        .expect("status json");
    assert_eq!(status_json["decimal_places"], 0);
    assert_eq!(status_json["measure"], "Scale");
    assert_eq!(status_json["alignment"], "Right");
    assert_eq!(status_json["display_width"], 8);
    assert_eq!(status_json["storage_width_bytes"], 8);

    let _ = std::fs::remove_file(path);
}

#[test]
fn test_spss_writer_variable_metadata_overrides() {
    let path = temp_sav_path("spss_writer_variable_metadata");
    let df = df! {
        "rating" => &[1i32, 2, 3],
        "score" => &[1.25f64, 2.5, 3.75],
        "name" => &["a", "bb", "ccc"],
    }
    .expect("df");

    let mut measures = HashMap::new();
    measures.insert("rating".to_string(), SpssMeasure::Ordinal);
    measures.insert("name".to_string(), SpssMeasure::Nominal);

    let mut alignments = HashMap::new();
    alignments.insert("rating".to_string(), SpssAlignment::Center);
    alignments.insert("name".to_string(), SpssAlignment::Right);

    let mut display_widths = HashMap::new();
    display_widths.insert("rating".to_string(), 12);
    display_widths.insert("name".to_string(), 24);

    let mut formats = HashMap::new();
    formats.insert(
        "score".to_string(),
        SpssVariableFormat {
            format_type: None,
            width: Some(10),
            decimals: Some(3),
        },
    );

    SpssWriter::new(&path)
        .with_variable_measures(measures)
        .with_variable_alignments(alignments)
        .with_variable_display_widths(display_widths)
        .with_variable_formats(formats)
        .write_df(&df)
        .expect("write");

    let reader = SpssReader::open(&path).expect("open");
    let rating = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "rating")
        .expect("rating variable");
    assert_eq!(rating.measure, Some(SpssMeasure::Ordinal));
    assert_eq!(rating.alignment, Some(SpssAlignment::Center));
    assert_eq!(rating.display_width, Some(12));

    let score = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "score")
        .expect("score variable");
    assert_eq!(score.format_width, 10);
    assert_eq!(score.format_decimals, 3);

    let name = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "name")
        .expect("name variable");
    assert_eq!(name.alignment, Some(SpssAlignment::Right));
    assert_eq!(name.display_width, Some(24));

    let _ = std::fs::remove_file(path);
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
    let stream = read_to_arrow_stream_ffi(&path, None, true, Some(true), None, 0, None)
        .expect("arrow stream");
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
