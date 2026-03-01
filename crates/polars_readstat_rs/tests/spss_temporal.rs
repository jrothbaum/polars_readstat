use std::path::PathBuf;

use polars::prelude::*;
use polars_readstat_rs::{scan_sav, ScanOptions};

fn data_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("spss")
        .join("data")
        .join(rel)
}

fn scan_file(path: PathBuf) -> PolarsResult<DataFrame> {
    let opts = ScanOptions {
        threads: None,
        chunk_size: None,
        missing_string_as_null: Some(true),
        value_labels_as_strings: Some(true),
        ..Default::default()
    };
    scan_sav(path, opts)?.collect()
}

#[test]
fn spss_sample_temporal_types() -> PolarsResult<()> {
    let df = scan_file(data_path("sample.sav"))?;
    let schema = df.schema();

    println!("{schema:?}");
    assert_eq!(
        schema.get("mydate"),
        Some(&DataType::Date),
        "mydate should be Date"
    );

    let dtime = schema.get("dtime").expect("dtime column missing");
    assert!(
        matches!(dtime, DataType::Datetime(_, _)),
        "dtime should be Datetime, got {dtime:?}"
    );

    assert_eq!(
        schema.get("mytime"),
        Some(&DataType::Time),
        "mytime should be Time"
    );

    Ok(())
}

#[test]
fn spss_simple_alltypes_temporal_types() -> PolarsResult<()> {
    let df = scan_file(data_path("simple_alltypes.sav"))?;
    let schema = df.schema();

    assert_eq!(schema.get("y"), Some(&DataType::Date), "y should be Date");

    assert_eq!(
        schema.get("date"),
        Some(&DataType::Date),
        "date should be Date"
    );

    Ok(())
}
