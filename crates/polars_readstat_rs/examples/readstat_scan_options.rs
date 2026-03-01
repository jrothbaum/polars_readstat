use polars::prelude::*;
use polars_readstat_rs::{readstat_scan, ReadStatFormat, ScanOptions};
use std::path::PathBuf;

/// Usage:
///   readstat_scan_options <file>
///       [--format sas|stata|spss]
///       [--columns a,b,c]
///       [--offset N] [--limit N]
///       [--threads N] [--chunk-size N]
///       [--missing-string-as-null true|false]
///       [--value-labels-as-strings true|false]
///       [--compress true|false]
///       [--compress-cols a,b,c]
///       [--compress-numeric true|false]
///       [--compress-datetime-to-date true|false]
///       [--compress-string-to-numeric true|false]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let file = args.next().ok_or("missing <file>")?;

    let mut format: Option<ReadStatFormat> = None;
    let mut columns: Option<Vec<String>> = None;
    let mut offset: usize = 0;
    let mut limit: Option<usize> = None;
    let mut opts = ScanOptions::default();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--format" => {
                let v = args.next().ok_or("missing --format value")?;
                format = match v.as_str() {
                    "sas" => Some(ReadStatFormat::Sas),
                    "stata" => Some(ReadStatFormat::Stata),
                    "spss" => Some(ReadStatFormat::Spss),
                    _ => return Err("format must be sas|stata|spss".into()),
                };
            }
            "--columns" => {
                let v = args.next().ok_or("missing --columns value")?;
                let cols = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>();
                if !cols.is_empty() {
                    columns = Some(cols);
                }
            }
            "--offset" => {
                let v = args.next().ok_or("missing --offset value")?;
                offset = v.parse()?;
            }
            "--limit" => {
                let v = args.next().ok_or("missing --limit value")?;
                limit = Some(v.parse()?);
            }
            "--threads" => {
                let v = args.next().ok_or("missing --threads value")?;
                opts.threads = Some(v.parse()?);
            }
            "--chunk-size" => {
                let v = args.next().ok_or("missing --chunk-size value")?;
                opts.chunk_size = Some(v.parse()?);
            }
            "--missing-string-as-null" => {
                let v = args
                    .next()
                    .ok_or("missing --missing-string-as-null value")?;
                opts.missing_string_as_null = Some(parse_bool(&v)?);
            }
            "--value-labels-as-strings" => {
                let v = args
                    .next()
                    .ok_or("missing --value-labels-as-strings value")?;
                opts.value_labels_as_strings = Some(parse_bool(&v)?);
            }
            "--compress" => {
                let v = args.next().ok_or("missing --compress value")?;
                opts.compress_opts.enabled = parse_bool(&v)?;
            }
            "--compress-cols" => {
                let v = args.next().ok_or("missing --compress-cols value")?;
                let cols = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>();
                if !cols.is_empty() {
                    opts.compress_opts.cols = Some(cols);
                }
            }
            "--compress-numeric" => {
                let v = args.next().ok_or("missing --compress-numeric value")?;
                opts.compress_opts.compress_numeric = parse_bool(&v)?;
            }
            "--compress-datetime-to-date" => {
                let v = args
                    .next()
                    .ok_or("missing --compress-datetime-to-date value")?;
                opts.compress_opts.datetime_to_date = parse_bool(&v)?;
            }
            "--compress-string-to-numeric" => {
                let v = args
                    .next()
                    .ok_or("missing --compress-string-to-numeric value")?;
                opts.compress_opts.string_to_numeric = parse_bool(&v)?;
            }
            _ => return Err(format!("unknown arg: {}", arg).into()),
        }
    }

    let mut lf = readstat_scan(PathBuf::from(file), Some(opts), format)?;

    if let Some(cols) = columns {
        let exprs: Vec<Expr> = cols.iter().map(|c| col(c)).collect();
        lf = lf.select(exprs);
    }

    if offset > 0 || limit.is_some() {
        let len: u32 = limit.unwrap_or(usize::MAX).try_into().unwrap_or(u32::MAX);
        lf = lf.slice(offset as i64, len);
    }

    let df = lf.collect()?;
    println!("rows={} cols={}", df.height(), df.width());
    Ok(())
}

fn parse_bool(v: &str) -> Result<bool, Box<dyn std::error::Error>> {
    match v {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err("expected bool: true|false".into()),
    }
}
