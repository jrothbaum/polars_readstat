use polars::prelude::*;
use std::path::PathBuf;
use polars_readstat_rs::{scan_dta, ScanOptions, StataReader};

fn test_data_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("stata")
        .join("data")
        .join(filename)
}

#[test]
fn test_scan_dta_collect_matches_metadata() -> PolarsResult<()> {
    let path = test_data_path("sample.dta");
    let reader = StataReader::open(&path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let meta = reader.metadata().clone();

    let mut opts = ScanOptions::default();
    opts.value_labels_as_strings = Some(false);
    let df = scan_dta(path, opts)?.collect()?;
    assert_eq!(df.height(), meta.row_count as usize);
    assert_eq!(df.width(), meta.variables.len());
    Ok(())
}

#[test]
fn test_scan_dta_head_and_select() -> PolarsResult<()> {
    let path = test_data_path("sample.dta");
    let reader = StataReader::open(&path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let meta = reader.metadata().clone();
    let col1 = meta.variables[0].name.clone();
    let col2 = meta.variables[1].name.clone();

    let mut opts = ScanOptions::default();
    opts.value_labels_as_strings = Some(false);
    let df = scan_dta(path, opts)?
        .select([col(&col1), col(&col2)])
        .limit(5)
        .collect()?;

    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), usize::min(5, meta.row_count as usize));
    Ok(())
}

#[test]
fn test_scan_dta_value_labels_as_strings() -> PolarsResult<()> {
    let path = test_data_path("stata-dta-partially-labeled.dta");
    let mut opts = ScanOptions::default();
    opts.value_labels_as_strings = Some(true);
    let df = scan_dta(path, opts)?.collect()?;
    assert!(df.width() > 0);
    Ok(())
}

#[test]
fn test_scan_dta_missing_string_as_null_false() -> PolarsResult<()> {
    let path = test_data_path("sample.dta");
    let mut opts = ScanOptions::default();
    opts.missing_string_as_null = Some(false);
    opts.value_labels_as_strings = Some(false);
    let df = scan_dta(path, opts)?
        .limit(5)
        .collect()?;
    assert!(df.height() > 0);
    Ok(())
}
