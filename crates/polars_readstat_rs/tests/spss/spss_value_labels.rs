use polars::prelude::*;
use polars_readstat_rs::{
    SpssReader, SpssValueLabelKey, SpssValueLabelMap, SpssValueLabels, SpssVariableLabels,
    SpssWriter,
};
use std::collections::HashMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    path.push(format!("{prefix}_{pid}_{nanos}.{ext}"));
    path
}

#[test]
fn test_spss_value_and_variable_labels() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("status".into(), &[1i32, 2, 3]).into_column()
    ])
    .unwrap();

    let mut map: SpssValueLabelMap = HashMap::new();
    map.insert(SpssValueLabelKey::from_f64(1.0), "one".to_string());
    map.insert(SpssValueLabelKey::from_f64(2.0), "two".to_string());
    map.insert(SpssValueLabelKey::from_f64(3.0), "three".to_string());
    let mut labels: SpssValueLabels = HashMap::new();
    labels.insert("status".to_string(), map);

    let var_labels = SpssVariableLabels::from([("status".to_string(), "Status Label".to_string())]);

    let path = temp_path("spss_labels", "sav");
    SpssWriter::new(&path)
        .with_value_labels(labels)
        .with_variable_labels(var_labels)
        .write_df(&df)
        .unwrap();

    let reader = SpssReader::open(&path).unwrap();
    let meta = reader.metadata();
    let var = meta
        .variables
        .iter()
        .find(|v| {
            v.short_name.eq_ignore_ascii_case("status") || v.name.eq_ignore_ascii_case("status")
        })
        .expect("status variable");
    let status_idx = meta.variables.iter().position(|v| v.name == var.name).unwrap();
    let label = meta.metadata_df.column("label").ok()
        .and_then(|c| c.str().ok())
        .and_then(|ca| ca.get(status_idx));
    assert_eq!(label, Some("Status Label"));
    assert!(var.value_label.is_some());

    let out = reader
        .read()
        .value_labels_as_strings(true)
        .finish()
        .unwrap();
    let col_name = var.name.as_str();
    let col = out.column(col_name).unwrap().str().unwrap();
    let vals: Vec<Option<&str>> = col.into_iter().collect();
    assert_eq!(vals, vec![Some("one"), Some("two"), Some("three")]);

    let _ = fs::remove_file(&path);
}

#[test]
fn test_spss_value_label_with_empty_label_text_is_preserved() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("status".into(), &[0i32, 1]).into_column()
    ])
    .unwrap();

    let mut map: SpssValueLabelMap = HashMap::new();
    map.insert(SpssValueLabelKey::from_f64(0.0), "NO TO:".to_string());
    map.insert(SpssValueLabelKey::from_f64(1.0), String::new());
    let mut labels: SpssValueLabels = HashMap::new();
    labels.insert("status".to_string(), map);

    let path = temp_path("spss_empty_label", "sav");
    SpssWriter::new(&path)
        .with_value_labels(labels)
        .write_df(&df)
        .unwrap();

    let reader = SpssReader::open(&path).unwrap();
    let meta = reader.metadata();
    let value_label = meta
        .value_labels
        .first()
        .expect("value label group should be present");
    assert_eq!(value_label.mapping.len(), 2, "empty-label entry should not be dropped");
    let has_empty_entry = value_label
        .mapping
        .iter()
        .any(|(_, label)| label.is_empty());
    assert!(has_empty_entry, "code with empty label text should be preserved, not dropped");

    let _ = fs::remove_file(&path);
}

#[test]
fn test_spss_variable_label_with_empty_text_is_preserved() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("status".into(), &[1i32, 2, 3]).into_column(),
        Series::new("other".into(), &[10i32, 20, 30]).into_column(),
    ])
    .unwrap();

    let var_labels = SpssVariableLabels::from([
        ("status".to_string(), String::new()),
        ("other".to_string(), "Other Label".to_string()),
    ]);

    let path = temp_path("spss_empty_var_label", "sav");
    SpssWriter::new(&path)
        .with_variable_labels(var_labels)
        .write_df(&df)
        .unwrap();

    let reader = SpssReader::open(&path).unwrap();
    let meta = reader.metadata();
    let status_idx = meta.variables.iter().position(|v| v.name == "status").unwrap();
    let label_ca = meta.metadata_df.column("label").ok().and_then(|c| c.str().ok());
    let status_label = label_ca.as_ref().and_then(|ca| ca.get(status_idx));
    assert_eq!(
        status_label,
        Some(""),
        "empty variable label should round-trip as an empty string, not be dropped to null"
    );

    let _ = fs::remove_file(&path);
}
