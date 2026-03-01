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
    assert_eq!(var.label.as_deref(), Some("Status Label"));
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
