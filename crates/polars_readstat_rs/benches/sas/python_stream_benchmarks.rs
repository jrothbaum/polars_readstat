use criterion::{black_box, criterion_group, criterion_main, Criterion};
use glob::glob;
use polars_readstat_rs::{readstat_batch_iter, ScanOptions};
use std::path::{Path, PathBuf};

fn find_test_files() -> Vec<PathBuf> {
    let pattern = format!("{}/tests/sas/data/**/*.sas7bdat", env!("CARGO_MANIFEST_DIR"));
    glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

fn find_test_file(pattern: &str) -> Option<PathBuf> {
    let files = find_test_files();
    files
        .into_iter()
        .find(|p| p.to_string_lossy().contains(pattern))
}

fn stream_sas_python_like(
    path: &Path,
    batch_size: usize,
    threads: usize,
    missing_string_as_null: bool,
    n_rows: Option<usize>,
) -> Result<usize, String> {
    let opts = ScanOptions {
        threads: Some(threads),
        chunk_size: Some(batch_size),
        missing_string_as_null: Some(missing_string_as_null),
        ..Default::default()
    };
    let iter = readstat_batch_iter(path, Some(opts), None, None, n_rows, Some(batch_size))
        .map_err(|e| e.to_string())?;
    let mut total_rows = 0;
    for batch in iter {
        let df = batch.map_err(|e| e.to_string())?;
        total_rows += df.height();
    }
    Ok(total_rows)
}

fn bench_python_stream_large_file(c: &mut Criterion) {
    if let Some(file) = find_test_file("psam_p17") {
        let threads = num_cpus::get_physical().max(1);
        c.bench_function("python_stream_large_file", |b| {
            b.iter(|| {
                let rows = stream_sas_python_like(
                    black_box(&file),
                    black_box(100_000),
                    black_box(threads),
                    black_box(false),
                    black_box(None),
                )
                .expect("python-like stream should succeed");
                black_box(rows);
            });
        });
    }
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(20))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_python_stream_large_file,
}

criterion_main!(benches);
