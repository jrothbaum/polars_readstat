use polars_readstat_rs::{readstat_batch_iter, ReadStatFormat, ScanOptions};
use std::time::Instant;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("Usage: benchmark <file> [reps]");
    let n_reps: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    // Detect format and log file info via a quick metadata read.
    let fmt = detect_format(&path);
    let opts_meta = ScanOptions::default();
    let mut warmup = readstat_batch_iter(&path, Some(opts_meta), Some(fmt), None, Some(1), Some(1))
        .expect("open");
    let _ = warmup.next();
    drop(warmup);

    println!("File: {path}");
    println!("Reps per configuration: {n_reps}");
    println!();

    // Warm up the OS page cache with one full sequential pass (dropped per batch).
    stream_read(&path, fmt, 1);

    // Sequential baseline.
    let seq_s = avg_time(n_reps, || stream_read(&path, fmt, 1));
    println!("sequential  threads=1    {seq_s:.3}s avg");

    // Sweep thread counts.
    let max_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let mut t = 2;
    while t <= max_threads {
        let s = avg_time(n_reps, || stream_read(&path, fmt, t));
        let speedup = seq_s / s;
        println!("pipelined   threads={t:<3}  {s:.3}s avg   ({speedup:.2}x vs sequential)");
        t *= 2;
    }
    if {
        let mut p = 2;
        while p * 2 <= max_threads {
            p *= 2;
        }
        p != max_threads
    } {
        let s = avg_time(n_reps, || stream_read(&path, fmt, max_threads));
        let speedup = seq_s / s;
        println!(
            "pipelined   threads={max_threads:<3}  {s:.3}s avg   ({speedup:.2}x vs sequential)"
        );
    }
}

/// Read the entire file in streaming fashion at `threads`, dropping each batch immediately.
/// Returns (rows_read, elapsed).
fn stream_read(path: &str, fmt: ReadStatFormat, threads: usize) -> (usize, f64) {
    let opts = ScanOptions {
        threads: Some(threads),
        ..Default::default()
    };
    let start = Instant::now();
    let mut iter =
        readstat_batch_iter(path, Some(opts), Some(fmt), None, None, None).expect("open");
    let mut rows = 0usize;
    while let Some(batch) = iter.next() {
        rows += batch.expect("batch").height();
        // DataFrame is dropped here — only its row count is retained.
    }
    (rows, start.elapsed().as_secs_f64())
}

fn avg_time<F>(reps: usize, mut f: F) -> f64
where
    F: FnMut() -> (usize, f64),
{
    let mut total = 0f64;
    for _ in 0..reps {
        let (_, s) = f();
        total += s;
    }
    total / reps as f64
}

fn detect_format(path: &str) -> ReadStatFormat {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "dta" => ReadStatFormat::Stata,
        "sav" | "zsav" => ReadStatFormat::Spss,
        _ => ReadStatFormat::Sas,
    }
}
