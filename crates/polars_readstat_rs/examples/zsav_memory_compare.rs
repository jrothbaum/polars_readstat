use polars_readstat_rs::SpssReader;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn spawn_peak_tracker() -> (Arc<AtomicU64>, Arc<std::sync::atomic::AtomicBool>) {
    let peak = Arc::new(AtomicU64::new(rss_kb()));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let peak2 = Arc::clone(&peak);
    let running2 = Arc::clone(&running);
    std::thread::spawn(move || {
        while running2.load(Ordering::Relaxed) {
            let rss = rss_kb();
            let prev = peak2.load(Ordering::Relaxed);
            if rss > prev {
                peak2.store(rss, Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    (peak, running)
}

fn main() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/spss/data/too_big/psam_p17.zsav");

    if !path.exists() {
        eprintln!("File not found: {}", path.display());
        std::process::exit(1);
    }

    let baseline = rss_kb();
    println!("Baseline RSS: {} MB", baseline / 1024);

    // --- Streaming: discard each batch immediately ---
    let (peak, running) = spawn_peak_tracker();
    let reader = SpssReader::open(&path).expect("open");
    let mut total_rows = 0usize;
    reader
        .read()
        .finish_batched(10_000, |df| {
            total_rows += df.height();
            true
        })
        .expect("stream");
    running.store(false, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(20));
    let stream_peak = peak.load(Ordering::Relaxed);
    println!(
        "Streaming   peak RSS: {} MB  ({} rows, batch=10k)",
        stream_peak / 1024,
        total_rows
    );

    // Wait for RSS to settle
    std::thread::sleep(Duration::from_millis(200));

    // --- Full read: collect entire file into one DataFrame ---
    let (peak2, running2) = spawn_peak_tracker();
    let reader2 = SpssReader::open(&path).expect("open");
    let df = reader2.read().finish().expect("full read");
    running2.store(false, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(20));
    let full_peak = peak2.load(Ordering::Relaxed);
    println!(
        "Full read   peak RSS: {} MB  ({} rows)",
        full_peak / 1024,
        df.height()
    );
}
