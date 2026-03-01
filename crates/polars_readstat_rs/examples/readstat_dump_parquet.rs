use polars::prelude::*;
use polars_readstat_rs::{readstat_scan, InformativeNullColumns, InformativeNullOpts, ScanOptions};
use std::path::PathBuf;

fn main() -> PolarsResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: readstat_dump_parquet <input> <output> [rows] [cols]");
        std::process::exit(1);
    }

    let input = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);
    let rows = args.get(3).and_then(|s| s.parse::<usize>().ok());
    let cols = args.get(4).and_then(|s| s.parse::<usize>().ok());

    let mut opts = ScanOptions::default();
    opts.missing_string_as_null = Some(false);
    if let Ok(value) = std::env::var("READSTAT_INFORMATIVE_NULLS") {
        let value = value.trim();
        if !value.is_empty() {
            let columns = if matches!(value, "1" | "true" | "all" | "ALL") {
                InformativeNullColumns::All
            } else {
                let cols = value
                    .split(',')
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<_>>();
                InformativeNullColumns::Selected(cols)
            };
            opts.informative_nulls = Some(InformativeNullOpts::new(columns));
        }
    }
    if std::env::var("READSTAT_VALUE_LABELS_AS_STRINGS")
        .ok()
        .is_some()
    {
        opts.value_labels_as_strings = Some(true);
    } else {
        opts.value_labels_as_strings = Some(false);
    }
    if std::env::var("READSTAT_PRESERVE_ORDER").ok().is_some() {
        opts.preserve_order = Some(true);
    }
    let mut lf = readstat_scan(&input, Some(opts), None)?;

    if let Some(n_cols) = cols {
        if n_cols > 0 {
            let schema = lf.collect_schema()?;
            let names = schema
                .iter_names()
                .take(n_cols)
                .map(|name| col(name.as_str()))
                .collect::<Vec<_>>();
            lf = lf.select(names);
        }
    }

    let mut df = lf.collect()?;
    if let Some(n_rows) = rows {
        if n_rows > 0 {
            df = df.head(Some(n_rows));
        }
    }

    let file = std::fs::File::create(output)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}
