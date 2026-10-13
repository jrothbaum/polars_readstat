//! Parallel reads run on Polars' global thread pool, so many concurrent scans must not
//! each create their own threads. (Kept as the only test in this binary so other tests'
//! threads don't skew the count.)

#![cfg(target_os = "linux")]

use polars::prelude::*;
use polars_readstat_rs::{readstat_batch_iter, ScanOptions, StataWriter};
use std::time::{SystemTime, UNIX_EPOCH};

fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task").unwrap().count()
}

#[test]
fn concurrent_scans_do_not_multiply_threads() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "polars_readstat_rs_pool_{}_{}.dta",
        std::process::id(),
        nanos
    ));
    let a: Vec<f64> = (0..50_000).map(|i| i as f64).collect();
    StataWriter::new(&path)
        .write_df(&df!("a" => a).unwrap())
        .unwrap();

    let open = || {
        let opts = ScanOptions {
            threads: Some(4),
            chunk_size: Some(100),
            ..Default::default()
        };
        readstat_batch_iter(&path, Some(opts), None, None, None, Some(100)).unwrap()
    };

    // Warm up so the global pool exists before taking the baseline.
    open().next().unwrap().unwrap();
    let baseline = thread_count();

    // 16 scans open at once, each mid-read.
    let mut scans: Vec<_> = (0..16).map(|_| open()).collect();
    for scan in scans.iter_mut() {
        scan.next().unwrap().unwrap();
    }
    let during = thread_count();
    drop(scans);
    let _ = std::fs::remove_file(&path);

    // A private pool per scan would add 16 × 4 threads here.
    assert!(
        during <= baseline + 2,
        "threads grew from {baseline} to {during} with 16 concurrent scans"
    );
}
