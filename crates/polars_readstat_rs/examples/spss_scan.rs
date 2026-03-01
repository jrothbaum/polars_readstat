use polars::prelude::*;
use polars_readstat_rs::{scan_sav, CompressOptionsLite, ScanOptions};

/// Usage: spss_scan <file> [threads] [chunk_size] [missing_string_as_null] [value_labels_as_strings] [compress] [compress_cols] [n_rows]
fn main() -> PolarsResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: spss_scan <file> [threads] [chunk_size] [missing_string_as_null] [value_labels_as_strings] [compress] [compress_cols] [n_rows]");
        std::process::exit(1);
    }

    let path = args[1].clone();
    let threads = args.get(2).and_then(|s| s.parse::<usize>().ok());
    let chunk_size = args.get(3).and_then(|s| s.parse::<usize>().ok());
    let missing_string_as_null = args.get(4).map(|s| s == "true" || s == "1").unwrap_or(true);
    let value_labels_as_strings = args.get(5).map(|s| s == "true" || s == "1").unwrap_or(true);
    let compress = args
        .get(6)
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);
    let compress_cols = args
        .get(7)
        .map(|s| {
            s.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    let mut compress_opts = CompressOptionsLite::default();
    if compress {
        compress_opts.enabled = true;
        compress_opts.compress_numeric = true;
    }
    if let Some(cols) = compress_cols {
        compress_opts.cols = Some(cols);
    }

    let opts = ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings: Some(value_labels_as_strings),
        preserve_order: Some(true),
        compress_opts,
    };
    let n_rows = args.get(8).and_then(|s| s.parse::<u32>().ok());
    let t0 = std::time::Instant::now();
    let mut lf = scan_sav(path, opts)?;
    if let Some(n) = n_rows {
        lf = lf.limit(n);
    }
    let df = lf.collect()?;
    let elapsed = t0.elapsed();
    println!("rows={} cols={} elapsed={:.3}s", df.height(), df.width(), elapsed.as_secs_f64());
    Ok(())
}
