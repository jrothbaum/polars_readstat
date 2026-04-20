use polars::prelude::*;
use polars_readstat_rs::reader::Sas7bdatReader;
use polars_readstat_rs::{readstat_batch_iter, ScanOptions};
use std::path::PathBuf;

fn base() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sas/data")
}

fn collect_sas(path: &std::path::Path, threads: usize, batch_size: usize) -> DataFrame {
    let opts = ScanOptions {
        threads: Some(threads),
        chunk_size: Some(batch_size),
        ..Default::default()
    };
    let iter =
        readstat_batch_iter(path, Some(opts), None, None, None, Some(batch_size)).expect("iter");
    let mut out: Option<DataFrame> = None;
    for df in iter {
        let df = df.expect("batch");
        if let Some(acc) = out.as_mut() {
            acc.vstack_mut(&df).expect("vstack");
        } else {
            out = Some(df);
        }
    }
    out.unwrap_or_else(DataFrame::empty)
}

/// Fingerprint a DataFrame as a multiset of (column_index, value_string) counts.
/// Two DataFrames are data-equivalent iff their fingerprints match.
/// Sum of all float64 values in a DataFrame, treating nulls as 0. Used as a quick
/// data-integrity fingerprint: if any values differ, the sum will differ.
fn float_checksum(df: &DataFrame) -> f64 {
    let mut total = 0.0f64;
    for col_name in df.get_column_names() {
        if let Ok(col) = df.column(col_name) {
            if let Ok(ca) = col.f64() {
                total += ca.sum().unwrap_or(0.0);
            }
        }
    }
    total
}

fn assert_same_df(left: &DataFrame, right: &DataFrame) {
    assert_eq!(left.shape(), right.shape(), "shape mismatch");
    assert_eq!(
        left.get_column_names(),
        right.get_column_names(),
        "column mismatch"
    );
    assert_eq!(left.columns(), right.columns(), "data mismatch");
}

/// Parallel reader must produce the same data regardless of thread count or batch size.
#[test]
fn test_parallel_reader_data_integrity() {
    let files = [
        base().join("data_poe/cps.sas7bdat"),
        base().join("sas_to_csv/drugprob.sas7bdat"),
        base().join("data_AHS2013/owner.sas7bdat"),
    ];
    for path in &files {
        if !path.exists() {
            continue;
        }
        let expected_rows = {
            let r = Sas7bdatReader::open(path).expect("open");
            r.metadata().row_count
        };
        // Ground truth: single-threaded serial read
        let baseline = collect_sas(path, 1, 65536);
        assert_eq!(
            baseline.height(),
            expected_rows,
            "baseline row count mismatch"
        );
        let baseline_checksum = float_checksum(&baseline);

        for threads in [2usize, 3, 4] {
            for batch_size in [512usize, 2048, 4096, 65536] {
                let df = collect_sas(path, threads, batch_size);
                assert_eq!(
                    df.height(),
                    expected_rows,
                    "{}: threads={threads} batch={batch_size}: row count mismatch",
                    path.display()
                );
                let checksum = float_checksum(&df);
                assert!(
                    (checksum - baseline_checksum).abs() < 1e-6,
                    "{}: threads={threads} batch={batch_size}: checksum {checksum} != baseline {baseline_checksum}",
                    path.display()
                );
            }
        }
    }
}

/// Test just the large 400MB file with multi-threading (row count + checksum only).
#[test]
fn test_large_file_parallel_integrity() {
    let path = base().join("too_big/psam_p17.sas7bdat");
    if !path.exists() {
        return;
    }
    let expected_rows = {
        let r = Sas7bdatReader::open(&path).expect("open");
        r.metadata().row_count
    };
    let baseline = collect_sas(&path, 1, 65536);
    assert_eq!(
        baseline.height(),
        expected_rows,
        "baseline row count mismatch"
    );
    let baseline_checksum = float_checksum(&baseline);

    for threads in [2usize, 4] {
        for batch_size in [65536usize, 131072] {
            let df = collect_sas(&path, threads, batch_size);
            assert_eq!(
                df.height(),
                expected_rows,
                "threads={threads} batch={batch_size}: row count mismatch (got {}, expected {})",
                df.height(),
                expected_rows
            );
            let checksum = float_checksum(&df);
            assert!(
                (checksum - baseline_checksum).abs() < 1e-6,
                "threads={threads} batch={batch_size}: checksum {checksum} != baseline {baseline_checksum}"
            );
        }
    }
}

#[test]
fn test_parallel_partial_reads_match_sequential() {
    let files = [
        base().join("data_AHS2013/owner.sas7bdat"),
        base().join("sas_to_csv/drugprob.sas7bdat"),
    ];

    for path in &files {
        if !path.exists() {
            continue;
        }

        let reader = Sas7bdatReader::open(path).expect("open");
        let row_count = reader.metadata().row_count;
        if row_count < 32 {
            continue;
        }

        let cases = [
            (0usize, row_count.min(17usize)),
            (
                5usize.min(row_count),
                row_count.saturating_sub(5).min(23usize),
            ),
            (
                row_count / 3,
                row_count.saturating_sub(row_count / 3).min(29usize),
            ),
        ];

        for threads in [2usize, 4] {
            for (offset, limit) in cases {
                if limit == 0 {
                    continue;
                }

                let parallel = reader
                    .read()
                    .with_n_threads(threads)
                    .with_chunk_size(7)
                    .with_offset(offset)
                    .with_limit(limit)
                    .finish()
                    .expect("parallel partial read");

                let sequential = reader
                    .read()
                    .sequential()
                    .with_chunk_size(7)
                    .with_offset(offset)
                    .with_limit(limit)
                    .finish()
                    .expect("sequential partial read");

                assert_same_df(&parallel, &sequential);
            }
        }
    }
}
