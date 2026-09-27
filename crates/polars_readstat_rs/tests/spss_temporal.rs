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

#[test]
fn spss_duration_over_24h_roundtrip() -> PolarsResult<()> {
    use polars_readstat_rs::spss::metadata_json;
    use polars_readstat_rs::SpssWriter;
    use std::fs;

    // Test duration values, crucially including >= 24h and multi-day durations:
    // - 85,876 s = 23:51:16
    // - 86,873 s = 24:07:53 (> 24 hours, previously dropped by Time mapping)
    // - 337,771 s = 93.8 hours (> 24 hours, previously dropped by Time mapping)
    // - null
    // - -3,600 s = -1 hour
    let raw_us = vec![
        Some(85_876_123_456i64),
        Some(86_873_654_321i64),
        Some(337_771_000_001i64),
        None,
        Some(-3_600_123_456i64),
    ];
    let duration_series = Int64Chunked::from_iter_options("w1intdur".into(), raw_us.iter().copied())
        .into_duration(TimeUnit::Microseconds)
        .into_series();

    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i32, 2, 3, 4, 5]).into_column(),
        duration_series.into_column(),
    ])?;

    let tmp_dir = std::env::temp_dir();
    let file_path = tmp_dir.join("test_spss_duration_roundtrip.sav");
    let _ = fs::remove_file(&file_path);

    SpssWriter::new(&file_path)
        .write_df(&df)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    // Check metadata JSON declares format_type 25 and format_class "Duration"
    let meta_json_str = metadata_json(&file_path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let meta_json: serde_json::Value = serde_json::from_str(&meta_json_str)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    let var_meta = meta_json["variables"]
        .as_array()
        .expect("variables array")
        .iter()
        .find(|v| v["name"] == "w1intdur")
        .expect("w1intdur var in metadata");

    assert_eq!(var_meta["format_type"], 25, "SPSS format_type should be 25 (DTIME)");
    assert_eq!(var_meta["format_class"], "Duration", "format_class should be Duration");
    assert_eq!(var_meta["format_width"], 18);
    assert_eq!(var_meta["format_decimals"], 6);

    // Scan the file back and verify schema and values
    let read_df = scan_file(file_path.clone())?;
    assert_eq!(
        read_df.schema().get("w1intdur"),
        Some(&DataType::Duration(TimeUnit::Microseconds)),
        "w1intdur should be Duration(us)"
    );

    let col = read_df.column("w1intdur")?;
    let dur_ca = col.duration()?;
    assert_eq!(dur_ca.phys.get(0), Some(85_876_123_456));
    assert_eq!(dur_ca.phys.get(1), Some(86_873_654_321), "Duration >= 24h must NOT be dropped");
    assert_eq!(dur_ca.phys.get(2), Some(337_771_000_001), "93.8 hour duration must NOT be dropped");
    assert_eq!(dur_ca.phys.get(3), None, "Null duration must be preserved");
    assert_eq!(dur_ca.phys.get(4), Some(-3_600_123_456), "Negative duration must be preserved");

    let _ = fs::remove_file(&file_path);
    Ok(())
}

#[test]
fn spss_por_duration_roundtrip() -> PolarsResult<()> {
    use polars_readstat_rs::{read_por, scan_por, write_por, PorWriteOptions};
    use std::fs;

    let raw_us = vec![
        Some(85_876_123_456i64),
        Some(86_873_654_321i64),
        Some(337_771_000_001i64),
        None,
    ];
    let duration_series = Int64Chunked::from_iter_options("dur".into(), raw_us.iter().copied())
        .into_duration(TimeUnit::Microseconds)
        .into_series();

    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i32, 2, 3, 4]).into_column(),
        duration_series.into_column(),
    ])?;

    let tmp_dir = std::env::temp_dir();
    let file_path = tmp_dir.join("test_spss_duration_por.por");
    let _ = fs::remove_file(&file_path);

    write_por(&df, &file_path, PorWriteOptions::default())
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    // Read back via read_por
    let (_meta, read_df) = read_por(&file_path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    assert_eq!(
        read_df.schema().get("DUR"),
        Some(&DataType::Duration(TimeUnit::Microseconds)),
        "DUR in POR should be Duration(us)"
    );

    let col = read_df.column("DUR")?;
    let dur_ca = col.duration()?;
    assert_eq!(dur_ca.phys.get(0), Some(85_876_123_456));
    assert_eq!(dur_ca.phys.get(1), Some(86_873_654_321), "Duration >= 24h must NOT be dropped in POR");
    assert_eq!(dur_ca.phys.get(2), Some(337_771_000_001), "93.8h duration must NOT be dropped in POR");
    assert_eq!(dur_ca.phys.get(3), None);

    // Also verify scan_por
    let scanned_df = scan_por(&file_path, ScanOptions::default())?.collect()?;
    assert_eq!(
        scanned_df.schema().get("DUR"),
        Some(&DataType::Duration(TimeUnit::Microseconds)),
    );

    let _ = fs::remove_file(&file_path);
    Ok(())
}
