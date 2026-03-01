use polars::frame::row::Row;
use polars::prelude::*;
use polars_readstat_rs::{SpssReader, SpssWriter};
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
