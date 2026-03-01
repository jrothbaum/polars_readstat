use polars::prelude::*;
use polars_readstat_rs::{stata, StataReader, StataWriter, ValueLabelMap, ValueLabels, VariableLabels};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
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
fn test_stata_value_labels_roundtrip() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("status".into(), &[1i32, 2, 3]).into_column(),
        Series::new("other".into(), &[10i32, 20, 30]).into_column(),
    ])
    .unwrap();

    let mut mapping: ValueLabelMap = BTreeMap::new();
    mapping.insert(1, "one".to_string());
    mapping.insert(2, "two".to_string());
    mapping.insert(3, "three".to_string());

    let mut labels: ValueLabels = HashMap::new();
    labels.insert("status".to_string(), mapping);

    let path = temp_path("stata_value_labels", "dta");
    StataWriter::new(&path)
        .with_value_labels(labels.clone())
        .with_variable_labels(VariableLabels::from([(
            "status".to_string(),
            "Status Label".to_string(),
        )]))
        .write_df(&df)
        .unwrap();

    let reader = StataReader::open(&path).unwrap();
    let meta = reader.metadata();

    let var = meta.variables.iter().find(|v| v.name == "status").unwrap();
    assert_eq!(var.value_label_name.as_deref(), Some("status"));
    assert_eq!(var.label.as_deref(), Some("Status Label"));

    let label = meta
        .value_labels
        .iter()
        .find(|v| v.name == "status")
        .unwrap();
    assert_eq!(label.mapping.len(), 6);

    let json = stata::metadata_json(&path).unwrap();
    let meta_json: Value = serde_json::from_str(&json).unwrap();
    let vars = meta_json
        .get("variables")
        .and_then(|v| v.as_array())
        .unwrap();
    let var = vars
        .iter()
        .find(|v| v.get("name").and_then(|n| n.as_str()) == Some("status"))
        .unwrap();
    let vlabels = var
        .get("value_labels")
        .and_then(|v| v.as_object())
        .unwrap();
    assert_eq!(vlabels.get("1").and_then(|v| v.as_str()), Some("one"));
    assert_eq!(vlabels.get("2").and_then(|v| v.as_str()), Some("two"));
    assert_eq!(vlabels.get("3").and_then(|v| v.as_str()), Some("three"));

    let df_labeled = reader
        .read()
        .value_labels_as_strings(true)
        .finish()
        .unwrap();
    let status = df_labeled.column("status").unwrap().str().unwrap();
    let values: Vec<Option<&str>> = status.into_iter().collect();
    assert_eq!(values, vec![Some("one"), Some("two"), Some("three")]);

    let _ = fs::remove_file(&path);
}
