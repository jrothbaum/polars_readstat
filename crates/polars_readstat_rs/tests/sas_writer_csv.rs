use polars::prelude::*;
use polars_readstat_rs::{
    SasValueLabelKey, SasValueLabelMap, SasValueLabels, SasVariableLabels, SasWriter,
};
use std::collections::HashMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("{prefix}_{pid}_{nanos}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_sas_writer_csv_bundle() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("num".into(), &[1i32, 2, 3]).into_column(),
        Series::new("str".into(), &["a", "b", "c"]).into_column(),
        Series::new("date".into(), &[0i32, 1, 2])
            .cast(&DataType::Date)
            .unwrap()
            .into_column(),
        Series::new("dt".into(), &[0i64, 1_000, 2_000])
            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            .unwrap()
            .into_column(),
        Series::new("tm".into(), &[0i64, 1_000_000_000, 2_000_000_000])
            .cast(&DataType::Time)
            .unwrap()
            .into_column(),
    ])
    .unwrap();

    let mut vmap: SasValueLabelMap = HashMap::new();
    vmap.insert(SasValueLabelKey::from(1.0), "one".to_string());
    vmap.insert(SasValueLabelKey::from(2.0), "two".to_string());
    let mut vlabels: SasValueLabels = HashMap::new();
    vlabels.insert("num".to_string(), vmap);

    let var_labels = SasVariableLabels::from([
        ("num".to_string(), "Numeric Label".to_string()),
        ("str".to_string(), "String Label".to_string()),
    ]);

    let out_dir = temp_dir("sas_writer_csv");
    let writer = SasWriter::new(&out_dir)
        .with_dataset_name("demo")
        .with_value_labels(vlabels)
        .with_variable_labels(var_labels);

    let (csv_path, sas_path) = writer.write_df(&df).unwrap();
    assert!(csv_path.exists(), "csv not written");
    assert!(sas_path.exists(), "sas script not written");

    let csv = fs::read_to_string(&csv_path).unwrap();
    let sas = fs::read_to_string(&sas_path).unwrap();

    assert!(sas.contains("data demo;"), "missing data step");
    assert!(sas.contains("proc format;"), "missing format block");
    assert!(sas.contains("Numeric Label"), "missing variable label");

    // date: 0,1,2 (days since 1970) -> 3653,3654,3655 (days since 1960)
    assert!(csv.contains("\n1,a,3653,"), "date conversion missing");
    // datetime: 0ms -> 3653*86400 seconds
    assert!(csv.contains(",315619200,"), "datetime conversion missing");

    let _ = fs::remove_file(&csv_path);
    let _ = fs::remove_file(&sas_path);
    let _ = fs::remove_dir_all(&out_dir);
}

#[test]
fn test_sas_writer_boolean_as_int() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("flag".into(), &[true, false, true]).into_column(),
        Series::new("val".into(), &[1i32, 2, 3]).into_column(),
    ])
    .unwrap();

    let out_dir = temp_dir("sas_writer_bool");
    let writer = SasWriter::new(&out_dir).with_dataset_name("booltest");
    let (csv_path, sas_path) = writer.write_df(&df).unwrap();

    let csv = fs::read_to_string(&csv_path).unwrap();
    // Boolean should be written as 0/1, not "true"/"false"
    assert!(csv.contains("1,1") || csv.contains(",1\n"), "boolean true should be 1");
    assert!(!csv.contains("true"), "boolean should not write as 'true'");
    assert!(!csv.contains("false"), "boolean should not write as 'false'");

    let sas = fs::read_to_string(&sas_path).unwrap();
    // Boolean (→ Int8) should get length 3
    assert!(sas.contains("length flag 3"), "boolean column should have length 3");

    let _ = fs::remove_file(&csv_path);
    let _ = fs::remove_file(&sas_path);
    let _ = fs::remove_dir_all(&out_dir);
}

#[test]
fn test_sas_writer_dataset_name_from_path() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("x".into(), &[1i32, 2]).into_column(),
    ])
    .unwrap();

    let out_dir = temp_dir("sas_writer_nametest");
    // Write to a file path with a stem — dataset name should come from the stem
    let file_path = out_dir.join("my_dataset.csv");
    let writer = SasWriter::new(&file_path);
    let (csv_path, sas_path) = writer.write_df(&df).unwrap();

    let sas = fs::read_to_string(&sas_path).unwrap();
    assert!(
        sas.contains("data my_dataset;"),
        "dataset name should be derived from file stem, got:\n{sas}"
    );

    let _ = fs::remove_file(&csv_path);
    let _ = fs::remove_file(&sas_path);
    let _ = fs::remove_dir_all(&out_dir);
}

#[test]
fn test_sas_writer_library_and_delete() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("x".into(), &[1i32, 2]).into_column(),
    ])
    .unwrap();

    let out_dir = temp_dir("sas_writer_libtest");
    let writer = SasWriter::new(&out_dir)
        .with_dataset_name("myds")
        .with_library("PERMLIB")
        .with_delete_csv_on_import(true);
    let (csv_path, sas_path) = writer.write_df(&df).unwrap();

    let sas = fs::read_to_string(&sas_path).unwrap();
    assert!(sas.contains("libname PERMLIB"), "should include libname statement");
    assert!(sas.contains("data PERMLIB.myds;"), "should use library.dataset in DATA step");
    assert!(sas.contains("fdelete"), "should include CSV delete statement");

    let _ = fs::remove_file(&csv_path);
    let _ = fs::remove_file(&sas_path);
    let _ = fs::remove_dir_all(&out_dir);
}

#[test]
fn test_sas_writer_numeric_lengths() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("i8".into(), &[1i8, 2]).into_column(),
        Series::new("i16".into(), &[1i16, 2]).into_column(),
        Series::new("i32".into(), &[1i32, 2]).into_column(),
        Series::new("i64".into(), &[1i64, 2]).into_column(),
        Series::new("f32".into(), &[1.0f32, 2.0]).into_column(),
        Series::new("f64".into(), &[1.0f64, 2.0]).into_column(),
    ])
    .unwrap();

    let out_dir = temp_dir("sas_writer_lengths");
    let writer = SasWriter::new(&out_dir).with_dataset_name("lengths");
    let (csv_path, sas_path) = writer.write_df(&df).unwrap();

    let sas = fs::read_to_string(&sas_path).unwrap();
    assert!(sas.contains("length i8 3"), "Int8 should have length 3");
    assert!(sas.contains("length i16 4"), "Int16 should have length 4");
    assert!(sas.contains("length i32 5"), "Int32 should have length 5");
    assert!(sas.contains("length f32 4"), "Float32 should have length 4");
    // Int64 and Float64 are SAS default (8 bytes), no length statement needed
    assert!(!sas.contains("length i64"), "Int64 should not emit length");
    assert!(!sas.contains("length f64"), "Float64 should not emit length");

    let _ = fs::remove_file(&csv_path);
    let _ = fs::remove_file(&sas_path);
    let _ = fs::remove_dir_all(&out_dir);
}
