use polars_readstat_rs::sas::profile::take_snapshot_ms;
use polars_readstat_rs::sas::Sas7bdatReader;
use std::time::Instant;

/// Repeatedly opens + fully reads a SAS file with default (real-world) settings and
/// reports a per-stage timing breakdown. Set PROFILE_SAS=1 to enable the finer-grained
/// stage timers inside the parallel read path (page I/O+decompress, value parsing,
/// DataFrame building); without it, only open (header+metadata) and total read are shown.
fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("Usage: profile_sas_read <file> [reps]");
    let reps: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let profiling = std::env::var("PROFILE_SAS").is_ok();
    if !profiling {
        eprintln!("(set PROFILE_SAS=1 for a per-stage breakdown of the read path)");
    }

    let mut total_rows = 0usize;
    let mut header_ms = 0.0;
    let mut metadata_ms = 0.0;
    let mut read_ms = 0.0;
    let mut io_ms = 0.0;
    let mut numeric_ms = 0.0;
    let mut string_ms = 0.0;
    let mut build_ms = 0.0;

    let start = Instant::now();
    for i in 0..reps {
        let (reader, open_profile) = Sas7bdatReader::open_with_profile(&path).expect("open");
        header_ms += open_profile.header_ms;
        metadata_ms += open_profile.metadata_ms;

        let read_start = Instant::now();
        let df = reader.read().finish().expect("read");
        read_ms += read_start.elapsed().as_secs_f64() * 1000.0;

        let (io, numeric, string, build) = take_snapshot_ms();
        io_ms += io;
        numeric_ms += numeric;
        string_ms += string;
        build_ms += build;

        total_rows += df.height();
        eprintln!("rep {}/{}: {} rows", i + 1, reps, df.height());
    }
    let elapsed = start.elapsed();

    println!();
    println!(
        "{reps} reps, {total_rows} total rows, {:.3}s total, {:.3}s/rep",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() / reps as f64
    );
    println!();
    println!("Per-rep averages (open):");
    println!("  header parse:      {:8.2} ms", header_ms / reps as f64);
    println!("  metadata parse:    {:8.2} ms", metadata_ms / reps as f64);
    println!("Per-rep averages (read, wall clock):");
    println!("  total read:        {:8.2} ms", read_ms / reps as f64);
    if profiling {
        println!();
        println!("Per-rep stage totals, SUMMED ACROSS WORKER THREADS (so these can");
        println!("add up to more than the wall-clock read time above -- compare");
        println!("their RELATIVE proportions, not their absolute magnitude). Note");
        println!("numeric/string parsing run as two separate passes over each batch");
        println!("when profiling is on, vs. one interleaved pass normally -- this adds");
        println!("its own overhead, so read/parse times above are somewhat inflated:");
        println!("  page I/O + decompress:  {:8.2} ms", io_ms / reps as f64);
        println!("  numeric/date parse:     {:8.2} ms", numeric_ms / reps as f64);
        println!("  string parse+decode:    {:8.2} ms", string_ms / reps as f64);
        println!("  DataFrame build:        {:8.2} ms", build_ms / reps as f64);
    }
}
