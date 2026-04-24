use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use polars_readstat_rs::{readstat_scan, ScanOptions};
use std::path::PathBuf;

fn test_file(relative: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    assert!(path.exists(), "benchmark file not found: {}", path.display());
    path
}

fn bench_read_small_file(c: &mut Criterion) {
    let file = test_file("tests/sas/data/data_pandas/airline.sas7bdat");
    c.bench_function("read_small_file", |b| {
        b.iter(|| {
            readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
        });
    });
}

fn bench_read_large_file(c: &mut Criterion) {
    let file = test_file("tests/sas/data/too_big/numeric_1000000_2.sas7bdat");
    c.bench_function("read_large_file", |b| {
        b.iter(|| {
            readstat_scan(black_box(&file), None, None).unwrap().collect().unwrap()
        });
    });
}

fn bench_read_compressed_threads(c: &mut Criterion) {
    let file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/sas/data/too_big/topical.sas7bdat");
    if !file_path.exists() {
        return;
    }

    let mut group = c.benchmark_group("compressed_rle_threads");
    for threads in [1usize, 2, 4, 8] {
        group.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, &t| {
            let opts = ScanOptions {
                threads: Some(t),
                ..Default::default()
            };
            b.iter(|| {
                readstat_scan(black_box(&file_path), Some(opts.clone()), None)
                    .unwrap()
                    .collect()
                    .unwrap()
            });
        });
    }
    group.finish();
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
    targets =
        bench_read_small_file,
        bench_read_large_file,
        bench_read_compressed_threads,
}

criterion_main!(benches);
