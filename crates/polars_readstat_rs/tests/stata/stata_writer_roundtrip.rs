use polars::frame::row::Row;
use polars::prelude::*;
use polars_readstat_rs::{
    pandas_prepare_df_for_stata, StataReader, StataWriteColumn, StataWriteSchema, StataWriter,
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn small_stata_path() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("stata")
        .join("data");
    let candidates = [
        base.join("sample.dta"),
        base.join("missing_test.dta"),
        base.join("sample_pyreadstat.dta"),
    ];
    for path in candidates {
        if path.exists() {
            return path;
        }
    }
    base.join("sample.dta")
}

fn temp_dta_path() -> PathBuf {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "polars_readstat_rs_roundtrip_{}_{}.dta",
        pid, nanos
    ))
}

fn schema_from_df(df: &DataFrame) -> StataWriteSchema {
    let columns = df
        .columns()
        .iter()
        .map(|col| {
            let s = col.as_materialized_series();
            let string_width_bytes = if matches!(s.dtype(), DataType::String) {
                let mut max_len = 1usize;
                if let Ok(utf8) = s.str() {
                    for opt in utf8.into_iter() {
                        if let Some(v) = opt {
                            max_len = max_len.max(v.len());
                        }
                    }
                }
                Some(max_len)
            } else {
                None
            };
            StataWriteColumn {
                name: s.name().to_string(),
                dtype: s.dtype().clone(),
                string_width_bytes,
            }
        })
        .collect::<Vec<_>>();

    StataWriteSchema {
        columns,
        row_count: None,
        value_labels: None,
        variable_labels: None,
    }
}

fn is_small_file(path: &PathBuf, max_bytes: u64) -> bool {
    std::fs::metadata(path)
        .map(|m| m.len() <= max_bytes)
        .unwrap_or(false)
}

fn collect_small_dta_files(max_bytes: u64) -> Vec<PathBuf> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("stata")
        .join("data");
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(base) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("dta") {
                continue;
            }
            if !is_small_file(&path, max_bytes) {
                continue;
            }
            out.push(path);
        }
    }
    out.sort();
    out
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
fn test_stata_roundtrip_write_read() {
    let path = small_stata_path();
    if !path.exists() {
        return;
    }

    let original = StataReader::open(&path).unwrap().read().finish().unwrap();
    let original = pandas_prepare_df_for_stata(&original).unwrap();
    let out_path = temp_dta_path();
    StataWriter::new(&out_path).write_df(&original).unwrap();

    let roundtrip = StataReader::open(&out_path)
        .unwrap()
        .read()
        .finish()
        .unwrap();
    assert_df_equal(&original, &roundtrip).unwrap();
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn test_stata_roundtrip_small_files() {
    let files = collect_small_dta_files(5 * 1024 * 1024);
    if files.is_empty() {
        return;
    }
    for path in files {
        let original = StataReader::open(&path).unwrap().read().finish().unwrap();
        let original = pandas_prepare_df_for_stata(&original).unwrap();
        let out_path = temp_dta_path();
        if let Err(e) = StataWriter::new(&out_path).write_df(&original) {
            eprintln!("write failed for {:?}: {}", path, e);
            report_out_of_range(&original);
            panic!("write failed");
        }
        let roundtrip = StataReader::open(&out_path)
            .unwrap()
            .read()
            .finish()
            .unwrap();
        if let Err(e) = assert_df_equal(&original, &roundtrip) {
            panic!("roundtrip mismatch for {:?}: {}", path, e);
        }
        let _ = std::fs::remove_file(&out_path);
    }
}

#[test]
fn test_stata_streaming_batches_patch_header_and_map() {
    let original = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i64, 2, 3, 4, 5, 6]).into_column(),
        Series::new("grp".into(), &["a", "bb", "ccc", "d", "ee", "fff"]).into_column(),
    ])
    .unwrap();
    let original = pandas_prepare_df_for_stata(&original).unwrap();
    let schema = schema_from_df(&original);
    let batches = vec![
        original.slice(0, 2),
        original.slice(2, 2),
        original.slice(4, 2),
    ];

    let out_path = temp_dta_path();
    StataWriter::new(&out_path)
        .write_batches_streaming(batches, schema)
        .unwrap();

    let reader = StataReader::open(&out_path).unwrap();
    assert_eq!(reader.metadata().row_count as usize, original.height());
    let roundtrip = reader.read().finish().unwrap();
    assert_df_equal(&original, &roundtrip).unwrap();
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn test_stata_streaming_batches_rejects_strl_without_prepass() {
    let original = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i64, 2]).into_column(),
        Series::new("txt".into(), &["a", "b"]).into_column(),
    ])
    .unwrap();
    let mut schema = schema_from_df(&original);
    for col in &mut schema.columns {
        if col.name == "txt" {
            col.string_width_bytes = Some(3_000);
        }
    }

    let out_path = temp_dta_path();
    let err = StataWriter::new(&out_path)
        .write_batches_streaming(vec![original], schema)
        .unwrap_err();
    assert!(err.to_string().contains("strL"), "unexpected error: {err}");
    let _ = std::fs::remove_file(&out_path);
}

fn report_out_of_range(df: &DataFrame) {
    use polars::prelude::DataType::*;
    let max_f32 = f32::from_bits(0x7effffff) as f64;
    let max_f64 = f64::from_bits(0x7fdfffffffffffff);
    for col in df.columns() {
        let series = col.as_materialized_series();
        match series.dtype() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
                if let Some((min, max)) = min_max_i64(series) {
                    if min < -0x7fffffff || max > 0x7fffffe4 {
                        eprintln!(
                            "out-of-range int column {}: min={} max={}",
                            series.name(),
                            min,
                            max
                        );
                    }
                }
            }
            Float32 => {
                if let Some(max) = max_abs_f64(series) {
                    if max > max_f32 {
                        eprintln!("out-of-range f32 column {}: max_abs={}", series.name(), max);
                    }
                }
            }
            Float64 => {
                if let Some(max) = max_abs_f64(series) {
                    if max > max_f64 {
                        eprintln!("out-of-range f64 column {}: max_abs={}", series.name(), max);
                    }
                }
            }
            _ => {}
        }
    }
}

fn min_max_i64(series: &Series) -> Option<(i64, i64)> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for i in 0..series.len() {
        let value = series.get(i).ok()?;
        if matches!(value, AnyValue::Null) {
            continue;
        }
        let v = match value {
            AnyValue::Int8(v) => v as i64,
            AnyValue::Int16(v) => v as i64,
            AnyValue::Int32(v) => v as i64,
            AnyValue::Int64(v) => v,
            AnyValue::UInt8(v) => v as i64,
            AnyValue::UInt16(v) => v as i64,
            AnyValue::UInt32(v) => v as i64,
            AnyValue::UInt64(v) if v <= i64::MAX as u64 => v as i64,
            _ => continue,
        };
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    if min == i64::MAX {
        None
    } else {
        Some((min, max))
    }
}

fn max_abs_f64(series: &Series) -> Option<f64> {
    let mut max = 0f64;
    let mut any = false;
    for i in 0..series.len() {
        let value = series.get(i).ok()?;
        if matches!(value, AnyValue::Null) {
            continue;
        }
        let v = match value {
            AnyValue::Float32(v) => v as f64,
            AnyValue::Float64(v) => v,
            _ => continue,
        };
        any = true;
        let av = v.abs();
        if av > max {
            max = av;
        }
    }
    if any {
        Some(max)
    } else {
        None
    }
}
