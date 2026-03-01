use polars_readstat_rs::{readstat_batch_iter, ReadStatFormat, ScanOptions};
use std::path::PathBuf;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use std::time::Instant;

/// Read current resident set size from /proc/self/status (Linux only).
fn current_rss_kb() -> u64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else { return 0 };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Ok(v) = rest.trim().trim_end_matches(" kB").parse::<u64>() {
                return v;
            }
        }
    }
    0
}

/// Spawn a background thread that polls RSS every 50 ms and records the peak.
/// Returns (peak_kb_arc, stop_sender) — send anything on the sender to stop.
fn start_rss_monitor() -> (Arc<AtomicU64>, std::sync::mpsc::SyncSender<()>) {
    let peak = Arc::new(AtomicU64::new(0));
    let peak_clone = peak.clone();
    let (stop_tx, stop_rx) = std::sync::mpsc::sync_channel::<()>(1);
    std::thread::spawn(move || {
        loop {
            peak_clone.fetch_max(current_rss_kb(), Ordering::Relaxed);
            if stop_rx.recv_timeout(std::time::Duration::from_millis(50)).is_ok() {
                break;
            }
        }
    });
    (peak, stop_tx)
}

fn psam_p17() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("sas")
        .join("data")
        .join("too_big")
        .join("psam_p17.sas7bdat")
}

fn pu2023() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("sas")
        .join("data")
        .join("too_big")
        .join("pu2023.sas7bdat")
}

const EXPECTED_ROWS: usize = 623_757;
const BATCH_SIZE: usize = 50_000;

fn stream_and_count(path: &PathBuf, threads: Option<usize>, batch_size: usize) -> (usize, usize, std::time::Duration) {
    let opts = ScanOptions {
        threads,
        chunk_size: Some(batch_size),
        ..Default::default()
    };
    let t0 = Instant::now();
    let mut stream =
        readstat_batch_iter(path, Some(opts), Some(ReadStatFormat::Sas), None, None, None)
            .expect("open stream");
    let mut total_rows = 0usize;
    let mut batch_count = 0usize;
    while let Some(df) = stream.next() {
        let df = df.expect("batch");
        total_rows += df.height();
        batch_count += 1;
        // df is dropped here — each batch is discarded immediately
    }
    (total_rows, batch_count, t0.elapsed())
}

/// Verify that readstat_batch_iter actually streams: multiple batches are returned
/// and the total row count is correct.
#[test]
fn test_sas_psam_streams_multiple_batches() {
    let path = psam_p17();
    if !path.exists() {
        return;
    }

    let (total_rows, batch_count, elapsed) = stream_and_count(&path, Some(1), BATCH_SIZE);
    assert!(batch_count > 1, "expected multiple batches, got {}", batch_count);
    assert_eq!(total_rows, EXPECTED_ROWS, "total rows mismatch");
    println!("serial: {} rows in {} batches, {:.2?}", total_rows, batch_count, elapsed);
}

/// Benchmark single-thread vs multi-thread streaming on psam_p17.sas7bdat.
/// Run with: cargo test test_sas_psam_multithread -- --nocapture --ignored
#[test]
#[ignore]
fn test_sas_psam_multithread_benchmark() {
    let path = psam_p17();
    if !path.exists() {
        println!("psam_p17.sas7bdat not found, skipping");
        return;
    }

    let n_cpus = num_cpus::get_physical().max(1);
    let all_cpus_label = format!("{} threads (physical)", n_cpus);
    let configs: Vec<(&str, Option<usize>)> = vec![
        ("1 thread", Some(1)),
        ("2 threads", Some(2)),
        ("4 threads", Some(4)),
        ("default threads", None),
        (all_cpus_label.as_str(), Some(n_cpus)),
    ];

    println!("\npsam_p17.sas7bdat ({} rows, batch_size={})", EXPECTED_ROWS, BATCH_SIZE);
    println!("{:<22} {:>10} {:>10}", "config", "rows", "time");
    println!("{}", "-".repeat(46));

    for (label, threads) in configs {
        let (rows, batches, elapsed) = stream_and_count(&path, threads, BATCH_SIZE);
        assert_eq!(rows, EXPECTED_ROWS, "{}: row count mismatch", label);
        println!("{:<22} {:>10} {:>8.2?}  ({} batches)", label, rows, elapsed, batches);
    }
}

fn stream_with_rss(
    path: &PathBuf,
    threads: Option<usize>,
    batch_size: usize,
    n_rows: Option<usize>,
) -> (usize, usize, std::time::Duration, u64) {
    let opts = ScanOptions {
        threads,
        chunk_size: Some(batch_size),
        ..Default::default()
    };
    let (peak_rss, stop) = start_rss_monitor();
    let t0 = Instant::now();
    let mut stream =
        readstat_batch_iter(path, Some(opts), Some(ReadStatFormat::Sas), None, n_rows, None)
            .expect("open stream");
    let mut total_rows = 0usize;
    let mut batch_count = 0usize;
    while let Some(df) = stream.next() {
        let df = df.expect("batch");
        total_rows += df.height();
        batch_count += 1;
    }
    let elapsed = t0.elapsed();
    stop.send(()).ok();
    let peak_kb = peak_rss.load(Ordering::Relaxed);
    (total_rows, batch_count, elapsed, peak_kb)
}

/// Stream 1/4 and 1/2 of pu2023.sas7bdat with RSS monitoring at varying batch sizes.
/// Run with: cargo test --release test_sas_pu2023_partial -- --nocapture --ignored
#[test]
#[ignore]
fn test_sas_pu2023_partial_stream_memory() {
    let path = pu2023();
    if !path.exists() {
        println!("pu2023.sas7bdat not found, skipping");
        return;
    }

    const TOTAL_ROWS: usize = 476_744;
    const THREADS: usize = 4;

    // (label, batch_size, n_rows)
    let configs: Vec<(&str, usize, usize)> = vec![
        ("1/4 rows, batch=1000", 1_000, TOTAL_ROWS / 4),
        ("1/4 rows, batch=2000", 2_000, TOTAL_ROWS / 4),
        ("1/2 rows, batch=1000", 1_000, TOTAL_ROWS / 2),
        ("1/2 rows, batch=2000", 2_000, TOTAL_ROWS / 2),
    ];

    println!("\npu2023.sas7bdat ({} total rows × 5200 cols, {} threads)", TOTAL_ROWS, THREADS);
    println!("{:<28} {:>10} {:>10} {:>10} {:>10}", "config", "rows", "batches", "time", "peak RSS");
    println!("{}", "-".repeat(74));

    for (label, batch_size, n_rows) in configs {
        let (rows, batches, elapsed, peak_kb) =
            stream_with_rss(&path, Some(THREADS), batch_size, Some(n_rows));
        assert_eq!(rows, n_rows, "{}: row count mismatch", label);
        assert!(batches > 1, "{}: expected multiple batches", label);
        let peak_gb = peak_kb as f64 / 1_048_576.0;
        println!(
            "{:<28} {:>10} {:>10} {:>8.2?}  {:>6.2} GB",
            label, rows, batches, elapsed, peak_gb
        );
    }
}

/// Stream pu2023.sas7bdat (11 GB, 476k rows × 5200 cols) discarding each batch immediately.
/// Uses batch_size=1000 to keep in-flight memory bounded (threads × 1000 rows × ~24 KB/row).
/// Run with: cargo test --release test_sas_pu2023 -- --nocapture --ignored
#[test]
#[ignore]
fn test_sas_pu2023_stream_discard() {
    let path = pu2023();
    if !path.exists() {
        println!("pu2023.sas7bdat not found, skipping");
        return;
    }

    const PU_BATCH: usize = 1_000;
    const PU_ROWS: usize = 476_744;

    let n_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let capped_cpus = n_cpus.min(8);
    let all_cpus_label = format!("{} threads (physical)", capped_cpus);
    // (label, threads)
    let configs: Vec<(&str, Option<usize>)> = vec![
        ("1 thread", Some(1)),
        ("4 threads", Some(4)),
        (all_cpus_label.as_str(), Some(capped_cpus)),
    ];

    println!(
        "\npu2023.sas7bdat ({} rows × 5200 cols, 11 GB, batch_size={})",
        PU_ROWS, PU_BATCH
    );
    println!("{:<28} {:>10} {:>10} {:>10}", "config", "rows", "time", "peak RSS");
    println!("{}", "-".repeat(64));

    for (label, threads) in configs {
        let (rows, batches, elapsed, peak_kb) =
            stream_with_rss(&path, threads, PU_BATCH, None);
        assert_eq!(rows, PU_ROWS, "{}: row count mismatch", label);
        let peak_gb = peak_kb as f64 / 1_048_576.0;
        println!(
            "{:<28} {:>10} {:>8.2?}  {:>6.2} GB  ({} batches)",
            label, rows, elapsed, peak_gb, batches
        );
    }
}
