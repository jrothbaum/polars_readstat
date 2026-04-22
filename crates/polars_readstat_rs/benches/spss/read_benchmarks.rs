use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars_readstat_rs::readstat_scan;
use std::path::PathBuf;

fn test_file(relative: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    assert!(path.exists(), "benchmark file not found: {}", path.display());
    path
}

fn bench_read_small_file(c: &mut Criterion) {
    let file = test_file("tests/spss/data/variable-label.sav");
    c.bench_function("read_small_file", |b| {
        b.iter(|| {
            readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
        });
    });
}

fn bench_read_large_file(c: &mut Criterion) {
    let file = test_file("tests/spss/data/too_big/GSS2024.sav");
    c.bench_function("read_large_file", |b| {
        b.iter(|| {
            readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
        });
    });
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
