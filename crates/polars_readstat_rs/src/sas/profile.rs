//! Coarse-grained, opt-in stage timers for the SAS parallel read path.
//!
//! Enabled by setting the `PROFILE_SAS` env var (any value). When disabled, the only
//! runtime cost is a single `OnceLock` read per page-worker iteration.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

pub struct StageTimers {
    /// Reading page bytes from disk + decompression + raw row-byte extraction
    /// (`DataReader::read_rows_bulk`).
    pub io_decompress_ns: AtomicU64,
    /// Parsing numeric/date/datetime/time columns from raw row bytes
    /// (`DataFrameBuilder::add_row_raw`, non-character plans only).
    pub numeric_parse_ns: AtomicU64,
    /// Parsing character columns from raw row bytes: trimming, encoding
    /// conversion, and string allocation (`DataFrameBuilder::add_row_raw`,
    /// character plans only).
    pub string_parse_ns: AtomicU64,
    /// Finalizing per-batch column buffers into a Polars `DataFrame`
    /// (`DataFrameBuilder::build`).
    pub df_build_ns: AtomicU64,
}

pub static STAGE_TIMERS: StageTimers = StageTimers {
    io_decompress_ns: AtomicU64::new(0),
    numeric_parse_ns: AtomicU64::new(0),
    string_parse_ns: AtomicU64::new(0),
    df_build_ns: AtomicU64::new(0),
};

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PROFILE_SAS").is_ok())
}

/// RAII guard that adds elapsed time to `counter` on drop, only if profiling is enabled.
pub struct StageGuard<'a> {
    start: Option<Instant>,
    counter: &'a AtomicU64,
}

pub fn stage(counter: &AtomicU64) -> StageGuard<'_> {
    StageGuard {
        start: enabled().then(Instant::now),
        counter,
    }
}

impl Drop for StageGuard<'_> {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            self.counter
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
}

/// Read and zero out the accumulated stage timings, in milliseconds:
/// `(io_decompress_ms, numeric_parse_ms, string_parse_ms, df_build_ms)`.
pub fn take_snapshot_ms() -> (f64, f64, f64, f64) {
    let io = STAGE_TIMERS.io_decompress_ns.swap(0, Ordering::Relaxed);
    let numeric = STAGE_TIMERS.numeric_parse_ns.swap(0, Ordering::Relaxed);
    let string = STAGE_TIMERS.string_parse_ns.swap(0, Ordering::Relaxed);
    let build = STAGE_TIMERS.df_build_ns.swap(0, Ordering::Relaxed);
    (
        io as f64 / 1e6,
        numeric as f64 / 1e6,
        string as f64 / 1e6,
        build as f64 / 1e6,
    )
}
