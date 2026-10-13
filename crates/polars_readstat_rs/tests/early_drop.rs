//! Dropping a parallel batch iterator before it is exhausted must not hang.
//!
//! The parallel readers decode on worker threads that `send` into a bounded
//! channel. If `Drop` joins those workers while the receiver is still alive, a
//! worker blocked on a full channel never wakes and the join waits forever.

use polars::prelude::*;
use polars_readstat_rs::{readstat_batch_iter, ScanOptions, SpssWriter, StataWriter, XptWriter};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ROWS: usize = 50_000;
const BATCH: usize = 100;
const TIMEOUT: Duration = Duration::from_secs(30);

fn temp_path(ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "polars_readstat_rs_early_drop_{}_{}.{}",
        std::process::id(),
        nanos,
        ext
    ))
}

fn sample_df() -> DataFrame {
    let a: Vec<f64> = (0..ROWS).map(|i| i as f64).collect();
    let b: Vec<f64> = (0..ROWS).map(|i| (i * 2) as f64).collect();
    df!("a" => a, "b" => b).unwrap()
}

/// Run `f` on another thread and fail if it doesn't finish within `TIMEOUT`.
fn assert_finishes(label: &str, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(TIMEOUT)
        .unwrap_or_else(|_| panic!("{label}: dropping a partially consumed iterator hung"));
}

fn take_one_and_drop(path: &Path, preserve_order: bool) {
    let opts = ScanOptions {
        threads: Some(4),
        chunk_size: Some(BATCH),
        preserve_order: Some(preserve_order),
        ..Default::default()
    };
    let mut iter = readstat_batch_iter(path, Some(opts), None, None, None, Some(BATCH))
        .expect("batch iter");
    let first = iter.next().expect("at least one batch").expect("batch ok");
    assert!(first.height() > 0);
    drop(iter);
}

fn check(label: &'static str, path: PathBuf) {
    for preserve_order in [false, true] {
        let p = path.clone();
        assert_finishes(label, move || take_one_and_drop(&p, preserve_order));
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn early_drop_stata() {
    let path = temp_path("dta");
    StataWriter::new(&path).write_df(&sample_df()).unwrap();
    check("stata", path);
}

#[test]
fn early_drop_spss() {
    let path = temp_path("sav");
    SpssWriter::new(&path).write_df(&sample_df()).unwrap();
    check("spss", path);
}

#[test]
fn early_drop_xpt() {
    let path = temp_path("xpt");
    XptWriter::new(&path).write_df(&sample_df()).unwrap();
    check("xpt", path);
}
