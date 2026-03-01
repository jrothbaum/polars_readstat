use criterion::{black_box, criterion_group, criterion_main, Criterion};
use glob::glob;
use polars_readstat_rs::readstat_scan;
use std::path::PathBuf;

fn find_test_files() -> Vec<PathBuf> {
    let pattern = format!(
        "{}/tests/stata/data/**/*.dta",
        env!("CARGO_MANIFEST_DIR")
    );

    glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .filter(|p| p.to_string_lossy().ends_with(".dta"))
        .collect()
}

fn find_test_file(pattern: &str) -> Option<PathBuf> {
    let files = find_test_files();
    files.into_iter().find(|p| p.to_string_lossy().contains(pattern))
}

fn bench_basic_read(c: &mut Criterion) {
    if let Some(file) = find_test_file("stata1_118") {
        c.bench_function("basic_read", |b| {
            b.iter(|| {
                readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
            });
        });
    }
}

fn bench_large_read(c: &mut Criterion) {
    if let Some(file) = find_test_file("usa_00009") {
        c.bench_function("large_read", |b| {
            b.iter(|| {
                readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
            });
        });
    }
}

criterion_group!(
    benches,
    bench_basic_read,
    bench_large_read,
);

criterion_main!(benches);
