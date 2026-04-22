use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars_readstat_rs::readstat_scan;
use std::path::PathBuf;

fn test_file(relative: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    assert!(path.exists(), "benchmark file not found: {}", path.display());
    path
}

fn bench_basic_read(c: &mut Criterion) {
    let file = test_file("tests/sas/data/data_pandas/airline.sas7bdat");
    c.bench_function("basic_read", |b| {
        b.iter(|| {
            readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
        });
    });
}

fn bench_large_read(c: &mut Criterion) {
    let file = test_file("tests/sas/data/too_big/numeric_1000000_2.sas7bdat");
    c.bench_function("large_read", |b| {
        b.iter(|| {
            readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
        });
    });
}

criterion_group!(
    benches,
    bench_basic_read,
    bench_large_read,
);

criterion_main!(benches);
