use polars_readstat_rs::Sas7bdatReader;
use std::time::Instant;

fn main() {
    let path = std::env::args().nth(1).expect("Usage: benchmark <file.sas7bdat>");

    // Warm up: open once to cache file in OS page cache
    {
        let reader = Sas7bdatReader::open(&path).expect("Failed to open");
        let _ = reader.read().with_limit(1).finish();
    }

    // Sequential read
    let start = Instant::now();
    let reader = Sas7bdatReader::open(&path).expect("Failed to open");
    let rows = reader.metadata().row_count;
    let cols = reader.metadata().columns.len();
    let df = reader.read().sequential().finish().expect("Failed to read");
    let elapsed = start.elapsed();
    println!("Sequential: {rows} rows x {cols} cols in {:.3}s ({} df rows)", elapsed.as_secs_f64(), df.height());

    // Parallel read
    let start = Instant::now();
    let reader = Sas7bdatReader::open(&path).expect("Failed to open");
    let df = reader.read().finish().expect("Failed to read");
    let elapsed = start.elapsed();
    println!("Parallel:   {rows} rows x {cols} cols in {:.3}s ({} df rows)", elapsed.as_secs_f64(), df.height());

    // Pipeline read
    let start = Instant::now();
    let reader = Sas7bdatReader::open(&path).expect("Failed to open");
    let df = reader.read().finish().expect("Failed to read");
    let elapsed = start.elapsed();
    println!("Pipeline:   {rows} rows x {cols} cols in {:.3}s ({} df rows)", elapsed.as_secs_f64(), df.height());
}
