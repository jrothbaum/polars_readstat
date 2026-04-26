use polars::prelude::*;
use polars_readstat_rs::{scan_sas7bdat, ScanOptions};
use std::path::PathBuf;

fn base() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sas/data")
}

fn read_with_index(path: &std::path::Path, preserve_order: bool, threads: usize) -> DataFrame {
    let opts = ScanOptions {
        threads: Some(threads),
        preserve_order: Some(preserve_order),
        row_index_name: Some("__idx__".to_string()),
        ..Default::default()
    };
    scan_sas7bdat(path, opts)
        .expect("scan")
        .collect()
        .expect("collect")
}

/// Read with preserve_order=true as ground truth; read with preserve_order=false and sort by the
/// row index column.  Both results must be identical after sorting.
#[test]
fn test_row_index_unordered_matches_ordered() {
    let files = [
        base().join("data_poe/cps.sas7bdat"),
        base().join("sas_to_csv/drugprob.sas7bdat"),
        base().join("data_AHS2013/owner.sas7bdat"),
        base().join("data_pandas/test1.sas7bdat"),
    ];

    for path in &files {
        if !path.exists() {
            continue;
        }

        for threads in [2usize, 4] {
            // Ground truth: ordered read assigns row indices in file order.
            let ordered = read_with_index(path, true, threads);

            // Unordered read: workers race, but the row index reflects file order.
            let unordered = read_with_index(path, false, threads);

            assert_eq!(
                ordered.shape(),
                unordered.shape(),
                "{}: threads={threads}: shape mismatch",
                path.display()
            );

            // Both should have __idx__ as the first column.
            assert_eq!(
                ordered.get_column_names()[0],
                "__idx__",
                "ordered: __idx__ not at position 0"
            );
            assert_eq!(
                unordered.get_column_names()[0],
                "__idx__",
                "unordered: __idx__ not at position 0"
            );

            // Sort the unordered result by __idx__ to recover file order.
            let unordered_sorted = unordered
                .lazy()
                .sort(["__idx__"], SortMultipleOptions::default())
                .collect()
                .expect("sort");

            // After sorting, every column must match the ordered baseline.
            assert_eq!(
                ordered.columns(),
                unordered_sorted.columns(),
                "{}: threads={threads}: data mismatch after sorting unordered result",
                path.display()
            );
        }
    }
}
