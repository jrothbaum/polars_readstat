use polars::prelude::*;
use polars_readstat_rs::stata::VarType;
use polars_readstat_rs::{StataReader, StataWriter};
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
fn test_strl_long_string_roundtrip() {
    let long = "a".repeat(3000);
    let df = DataFrame::new_infer_height(vec![
        Series::new("long_str".into(), &[long.as_str()]).into_column()
    ])
    .unwrap();

    let path = temp_path("stata_strl_long", "dta");
    StataWriter::new(&path).write_df(&df).unwrap();

    let reader = StataReader::open(&path).unwrap();
    let var = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "long_str")
        .unwrap();
    assert!(matches!(var.var_type, VarType::StrL));

    let out = reader.read().finish().unwrap();
    let got = out.column("long_str").unwrap().str().unwrap().get(0);
    assert_eq!(got, Some(long.as_str()));

    let _ = fs::remove_file(&path);
}

#[test]
fn test_strl_with_null_bytes_roundtrip() {
    let mut s = "prefix".to_string();
    s.push('\u{0000}');
    s.push_str("suffix");
    let long = format!("{}{}", s, "b".repeat(3000));
    let df = DataFrame::new_infer_height(vec![
        Series::new("bin_str".into(), &[long.as_str()]).into_column()
    ])
    .unwrap();

    let path = temp_path("stata_strl_bin", "dta");
    StataWriter::new(&path).write_df(&df).unwrap();

    let reader = StataReader::open(&path).unwrap();
    let var = reader
        .metadata()
        .variables
        .iter()
        .find(|v| v.name == "bin_str")
        .unwrap();
    assert!(matches!(var.var_type, VarType::StrL));

    let out = reader.read().finish().unwrap();
    let got = out.column("bin_str").unwrap().str().unwrap().get(0);
    assert_eq!(got, Some(long.as_str()));

    let _ = fs::remove_file(&path);
}
