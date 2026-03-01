use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use glob::glob;
use polars_readstat_rs::{readstat_batch_iter, ReadStatFormat, ScanOptions};
use std::path::PathBuf;
use std::time::Duration;

const ROW_LIMIT: usize = 200_000;
const BATCH_SIZE: usize = 8_192;

fn find_files(pattern: &str) -> Vec<PathBuf> {
    glob(pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

fn bench_format(c: &mut Criterion, label: &str, files: Vec<PathBuf>, format: ReadStatFormat) {
    if files.is_empty() {
        return;
    }

    let mut group = c.benchmark_group(format!("stream_{}", label));
    for path in files {
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());
        group.bench_with_input(BenchmarkId::from_parameter(name), &path, |b, p| {
            b.iter(|| {
                let opts = ScanOptions::default();
                let mut iter = readstat_batch_iter(
                    black_box(p.as_path()),
                    Some(opts),
                    Some(format),
                    None,
                    Some(ROW_LIMIT),
                    Some(BATCH_SIZE),
                )
                .expect("batch iter");
                let mut rows = 0usize;
                while let Some(batch) = iter.next() {
                    let df = batch.expect("batch");
                    rows += df.height();
                    black_box(&df);
                }
                black_box(rows);
            });
        });
    }
    group.finish();
}

fn bench_sas(c: &mut Criterion) {
    let pattern = format!(
        "{}/tests/sas/data/too_big/*.sas7bdat",
        env!("CARGO_MANIFEST_DIR")
    );
    let files = find_files(&pattern);
    bench_format(c, "sas", files, ReadStatFormat::Sas);
}

fn bench_stata(c: &mut Criterion) {
    let pattern = format!(
        "{}/tests/stata/data/too_big/*.dta",
        env!("CARGO_MANIFEST_DIR")
    );
    let files = find_files(&pattern);
    bench_format(c, "stata", files, ReadStatFormat::Stata);
}

fn bench_spss(c: &mut Criterion) {
    let pattern_sav = format!(
        "{}/tests/spss/data/too_big/*.sav",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut files = find_files(&pattern_sav);
    let pattern_zsav = format!(
        "{}/tests/spss/data/too_big/*.zsav",
        env!("CARGO_MANIFEST_DIR")
    );
    files.extend(find_files(&pattern_zsav));
    bench_format(c, "spss", files, ReadStatFormat::Spss);
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_sas, bench_stata, bench_spss,
}

criterion_main!(benches);
