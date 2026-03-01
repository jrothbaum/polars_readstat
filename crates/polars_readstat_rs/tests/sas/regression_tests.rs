mod common;

use polars_readstat_rs::reader::Sas7bdatReader;
use polars_readstat_rs::{scan_sas7bdat, ScanOptions};
use common::{test_data_path, all_sas_files};
use polars::prelude::*;

/// Regression test: verify specific data values using the Lazy API
#[test]
fn test_test1_data_values() {
    let path = test_data_path("data_pandas/test1.sas7bdat");
    if !path.exists() { return; }

    // Use the new AnonymousScan entry point
    let df = scan_sas7bdat(path, Default::default())
        .expect("Failed to create LazyFrame")
        .collect()
        .expect("Failed to collect LazyFrame");

    assert_eq!(df.height(), 10, "test1 should have 10 rows");
    assert!(df.width() >= 4, "test1 should have at least 4 columns");

    let col_names = df.get_column_names();
    assert!(col_names.iter().any(|&n| n == "Column1"), "Should have Column1");

    let col1 = df.column("Column1").unwrap().f64().unwrap();
    let col3 = df.column("Column3").unwrap().f64().unwrap();
    let col8 = df.column("Column8").unwrap().f64().unwrap();

    // Guard against MIX-page row-start alignment regressions (+4 bytes issue).
    assert_eq!(col1.get(7), Some(0.148));
    assert_eq!(col1.get(8), None);
    assert_eq!(col1.get(9), Some(0.663));
    assert_eq!(col3.get(7), Some(37.0));
    assert_eq!(col3.get(8), Some(15.0));
    assert_eq!(col3.get(9), None);
    assert_eq!(col8.get(7), Some(8833.0));
    assert_eq!(col8.get(8), Some(3227.0));
    assert_eq!(col8.get(9), None);
}

/// Regression test: verify that Projection Pushdown (column selection) works
#[test]
fn test_column_projection() {
    let path = test_data_path("data_pandas/test1.sas7bdat");
    if !path.exists() { return; }

    // This specifically tests that your SasScan resolves names to indices correctly
    let df = scan_sas7bdat(path, Default::default())
        .unwrap()
        .select([col("Column1")])
        .collect()
        .unwrap();

    assert_eq!(df.width(), 1);
    assert_eq!(df.get_column_names()[0], "Column1");
}

/// Regression test: verify limit pushdown
#[test]
fn test_limit_pushdown() {
    let path = test_data_path("data_pandas/test1.sas7bdat");
    if !path.exists() { return; }

    // This tests that opts.n_rows is passed correctly to the scan
    let df = scan_sas7bdat(path, Default::default())
        .unwrap()
        .limit(5)
        .collect()
        .unwrap();

    assert_eq!(df.height(), 5);
}

/// Regression test: verify compressed file decompression via streaming scan
#[test]
fn test_compressed_data_validity() {
    let files = all_sas_files();
    let compressed: Vec<_> = files.iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains("compressed") || s.contains("rdc")
        })
        .collect();

    for file in compressed {
        // The Lazy API/Pipeline automatically handles decompression
        // because the I/O thread uses your DataReader
        let mut opts = ScanOptions::default();
        opts.threads = Some(1);
        let df = scan_sas7bdat(file.to_path_buf(), opts)
            .unwrap()
            .collect()
            .unwrap();

        assert!(df.height() > 0, "Compressed file {} read 0 rows", file.display());
    }
}

// NOTE: test_arrow_stream_integration removed - polars_readstat_rs::arrow_output::read_to_arrow
// no longer exists. Only read_to_arrow_ffi remains in the arrow_output module.

/// Regression test: verify metadata accessibility
#[test]
fn test_metadata_accuracy() {
    let files: Vec<_> = all_sas_files().into_iter().collect();
    let sample = files.iter().take(5);

    for file in sample {
        let reader = Sas7bdatReader::open(file).unwrap();
        let meta = reader.metadata();

        // Ensure the scan produces the same dimensions metadata claims
        let df = scan_sas7bdat(file.to_path_buf(), Default::default())
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(df.height(), meta.row_count, "Row mismatch in {}", file.display());
        assert_eq!(df.width(), meta.column_count, "Col mismatch in {}", file.display());
    }
}
