use criterion::{black_box, criterion_group, criterion_main, Criterion};
use glob::glob;
use polars_readstat_rs::readstat_scan;
use std::path::PathBuf;

fn find_test_files() -> Vec<PathBuf> {
    let pattern = format!(
        "{}/tests/spss/data/**/*.sav",
        env!("CARGO_MANIFEST_DIR")
    );

    glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

fn find_test_file(pattern: &str) -> Option<PathBuf> {
    let files = find_test_files();
    files.into_iter().find(|p| p.to_string_lossy().contains(pattern))
}

fn find_large_file() -> Option<PathBuf> {
    find_test_file("GSS2024").or_else(|| find_test_file("sample_large"))
}

fn bench_read_small_file(c: &mut Criterion) {
    if let Some(file) = find_test_file("sample") {
        c.bench_function("read_small_file", |b| {
            b.iter(|| {
                readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
            });
        });
    }
}

fn bench_read_large_file(c: &mut Criterion) {
    if let Some(file) = find_large_file() {
        c.bench_function("read_large_file", |b| {
            b.iter(|| {
                readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
            });
        });
    }
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(2))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_read_small_file,
        bench_read_large_file,
}

criterion_main!(benches);
