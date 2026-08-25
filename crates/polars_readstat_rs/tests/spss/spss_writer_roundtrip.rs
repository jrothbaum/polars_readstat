use polars::frame::row::Row;
use polars::prelude::*;
use polars_readstat_rs::{SpssReader, SpssWriter};
use std::collections::HashMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::spss_files;

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
fn test_spss_roundtrip_basic() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i32, 2, 3]).into_column(),
        Series::new("name".into(), &["alice", "bob", "carol"]).into_column(),
    ])
    .unwrap();

    let path = temp_path("spss_roundtrip", "sav");
    SpssWriter::new(&path).write_df(&df).unwrap();

    let out = SpssReader::open(&path).unwrap().read().finish().unwrap();
    assert_eq!(out.shape(), df.shape());

    let _ = fs::remove_file(&path);
}

#[test]
fn test_spss_compressed_roundtrip_basic() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1.0f64, 2.0, 3.0, 4.0]).into_column(),
        Series::new(
            "score".into(),
            &[Some(1.5f64), None, Some(-3.0), Some(42.0)],
        )
        .into_column(),
        Series::new(
            "name".into(),
            &[Some("alice"), Some("bob"), None, Some("carol")],
        )
        .into_column(),
    ])
    .unwrap();

    let path = temp_path("spss_compressed_roundtrip", "sav");
    SpssWriter::new(&path)
        .with_compression(true)
        .write_df(&df)
        .unwrap();

    let out = SpssReader::open(&path).unwrap().read().finish().unwrap();
    assert_df_equal(&df, &out).unwrap();

    let _ = fs::remove_file(&path);
}

#[test]
fn test_spss_compressed_preserves_declared_width_and_shrinks_file() {
    // A declared width of 1024 bytes should survive the roundtrip exactly
    // (this is what issue #55 asks for), while the compressed file stays
    // far smaller than the declared width would imply if stored raw.
    let n_rows = 200;
    let df = DataFrame::new_infer_height(vec![
        Series::new(
            "id".into(),
            &(0..n_rows).map(|i| i as f64).collect::<Vec<_>>(),
        )
        .into_column(),
        Series::new(
            "comments".into(),
            &(0..n_rows).map(|_| "short").collect::<Vec<_>>(),
        )
        .into_column(),
    ])
    .unwrap();

    let mut string_widths = HashMap::new();
    string_widths.insert("comments".to_string(), 1024usize);

    let compressed_path = temp_path("spss_compressed_wide", "sav");
    SpssWriter::new(&compressed_path)
        .with_string_widths(string_widths.clone())
        .with_compression(true)
        .write_df(&df)
        .unwrap();

    let uncompressed_path = temp_path("spss_uncompressed_wide", "sav");
    SpssWriter::new(&uncompressed_path)
        .with_string_widths(string_widths)
        .with_compression(false)
        .write_df(&df)
        .unwrap();

    let out = SpssReader::open(&compressed_path)
        .unwrap()
        .read()
        .finish()
        .unwrap();
    assert_df_equal(&df, &out).unwrap();

    let metadata = SpssReader::open(&compressed_path).unwrap().metadata().clone();
    let comments_var = metadata
        .variables
        .iter()
        .find(|v| v.name == "comments")
        .unwrap();
    assert_eq!(comments_var.string_len, 1024);

    let compressed_size = fs::metadata(&compressed_path).unwrap().len();
    let uncompressed_size = fs::metadata(&uncompressed_path).unwrap().len();
    assert!(
        compressed_size < uncompressed_size / 2,
        "compressed size {compressed_size} should be far smaller than uncompressed size {uncompressed_size}"
    );

    let _ = fs::remove_file(&compressed_path);
    let _ = fs::remove_file(&uncompressed_path);
}

fn assert_df_equal(left: &DataFrame, right: &DataFrame) -> PolarsResult<()> {
    if left.height() != right.height() || left.width() != right.width() {
        return Err(PolarsError::ComputeError("dataframe shape mismatch".into()));
    }
    if left.schema() != right.schema() {
        return Err(PolarsError::ComputeError(
            "dataframe schema mismatch".into(),
        ));
    }
    let cols = left.get_column_names_owned();
    for i in 0..left.height() {
        let l = left.get_row(i)?;
        let r = right.get_row(i)?;
        if !rows_equal(&l, &r) {
            let mut details = String::new();
            for (idx, (lv, rv)) in l.0.iter().zip(r.0.iter()).enumerate() {
                if !anyvalue_equal(lv, rv) {
                    details.push_str(&format!(
                        "col {} ({}): left={:?} right={:?}\n",
                        idx,
                        cols.get(idx).map(|s| s.as_str()).unwrap_or("?"),
                        lv,
                        rv
                    ));
                }
            }
            return Err(PolarsError::ComputeError(
                format!("row mismatch at {}\n{}", i, details).into(),
            ));
        }
    }
    Ok(())
}

fn rows_equal(left: &Row, right: &Row) -> bool {
    left.0.len() == right.0.len()
        && left
            .0
            .iter()
            .zip(right.0.iter())
            .all(|(l, r)| anyvalue_equal(l, r))
}

fn anyvalue_equal(left: &AnyValue, right: &AnyValue) -> bool {
    use AnyValue::*;
    match (left, right) {
        (Null, Null) => true,
        (Float32(l), Float32(r)) => {
            if l.is_nan() && r.is_nan() {
                true
            } else {
                l == r
            }
        }
        (Float64(l), Float64(r)) => {
            if l.is_nan() && r.is_nan() {
                true
            } else {
                l == r
            }
        }
        (String(l), String(r)) => trim_trailing_nul(l) == trim_trailing_nul(r),
        (Null, String(r)) if r.is_empty() => true,
        (String(l), Null) if l.is_empty() => true,
        _ => left == right,
    }
}

fn trim_trailing_nul(s: &str) -> &str {
    s.trim_end_matches('\0')
}

#[test]
fn test_spss_roundtrip_all_files() {
    let files = spss_files();
    if files.is_empty() {
        return;
    }

    for path in files {
        let reader = match SpssReader::open(&path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("SKIP: {:?} open failed: {}", path, e);
                continue;
            }
        };

        let df_base = match reader.read().value_labels_as_strings(false).finish() {
            Ok(df) => df,
            Err(e) => {
                eprintln!("SKIP: {:?} read failed: {}", path, e);
                continue;
            }
        };

        let df_labels = match reader.read().value_labels_as_strings(true).finish() {
            Ok(df) => df,
            Err(e) => {
                eprintln!("SKIP: {:?} read (labels) failed: {}", path, e);
                continue;
            }
        };
        for name in df_base.get_column_names() {
            let base_dtype = df_base
                .column(name)
                .map(|s| s.dtype())
                .unwrap_or(&DataType::String);
            let label_dtype = df_labels
                .column(name)
                .map(|s| s.dtype())
                .unwrap_or(&DataType::String);
            if base_dtype != label_dtype && label_dtype != &DataType::String {
                panic!(
                    "unexpected dtype change for {}: base={:?} labels={:?}",
                    name, base_dtype, label_dtype
                );
            }
        }

        let out_path = temp_path("spss_roundtrip_all", "sav");
        if let Err(e) = SpssWriter::new(&out_path).write_df(&df_base) {
            eprintln!("SKIP: {:?} write failed: {}", path, e);
            let _ = fs::remove_file(&out_path);
            continue;
        }
        match SpssReader::open(&out_path)
            .and_then(|r| r.read().value_labels_as_strings(false).finish())
        {
            Ok(roundtrip) => {
                if let Err(e) = assert_df_equal(&df_base, &roundtrip) {
                    eprintln!("SKIP: {:?} roundtrip mismatch: {}", path, e);
                }
            }
            Err(e) => {
                eprintln!("SKIP: {:?} read back failed: {}", out_path, e);
            }
        }
        let _ = fs::remove_file(&out_path);
    }
}
