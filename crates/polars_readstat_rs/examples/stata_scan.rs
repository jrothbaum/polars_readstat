use polars::prelude::*;
use polars_readstat_rs::{scan_dta, CompressOptionsLite, ScanOptions};

/// Usage: stata_scan <file> [threads] [chunk_size] [missing_string_as_null] [value_labels_as_strings] [compress] [compress_cols]
fn main() -> PolarsResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: stata_scan <file> [threads] [chunk_size] [missing_string_as_null] [value_labels_as_strings] [compress] [compress_cols]");
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
    let df = scan_dta(path, opts)?.collect()?;
    println!("rows={} cols={}", df.height(), df.width());
    Ok(())
}
