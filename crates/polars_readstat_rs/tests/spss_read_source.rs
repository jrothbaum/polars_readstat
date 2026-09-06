//! Exercises `SpssReader::open_bytes` against the same fixture corpus used
//! for path-based reads, so the new `ReadSource` code path gets the existing
//! edge-case coverage "for free" instead of needing new fixtures.

mod common;

use common::spss_files;
use polars_readstat_rs::SpssReader;

#[test]
fn open_bytes_matches_open_path_for_all_spss_files() {
    let files = spss_files();
    assert!(!files.is_empty(), "expected at least one SPSS .sav/.zsav fixture");

    let mut mismatches = Vec::new();

    for path in &files {
        let via_path = match SpssReader::open(path) {
            Ok(r) => r,
            Err(e) => {
                mismatches.push(format!("{}: failed to open by path: {e}", path.display()));
                continue;
            }
        };
        let limit = usize::min(100_000, via_path.metadata().row_count as usize);
        let df_path = match via_path.read().with_limit(limit).finish() {
            Ok(df) => df,
            Err(e) => {
                mismatches.push(format!("{}: failed to read by path: {e}", path.display()));
                continue;
            }
        };

        let bytes = std::fs::read(path).expect("read fixture bytes");
        let via_bytes = match SpssReader::open_bytes(bytes) {
            Ok(r) => r,
            Err(e) => {
                mismatches.push(format!("{}: failed to open_bytes: {e}", path.display()));
                continue;
            }
        };
        let df_bytes = match via_bytes.read().with_limit(limit).finish() {
            Ok(df) => df,
            Err(e) => {
                mismatches.push(format!("{}: failed to read via open_bytes: {e}", path.display()));
                continue;
            }
        };

        if !df_path.equals_missing(&df_bytes) {
            mismatches.push(format!(
                "{}: open() and open_bytes() produced different DataFrames (shapes {:?} vs {:?})",
                path.display(),
                df_path.shape(),
                df_bytes.shape()
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "open_bytes() diverged from open() for {} file(s):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn open_bytes_parallel_matches_sequential() {
    let files = spss_files();
    let Some(path) = files
        .into_iter()
        .max_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
    else {
        return;
    };

    let bytes = std::fs::read(&path).expect("read fixture bytes");
    let reader = SpssReader::open_bytes(bytes).expect("open_bytes");
    let limit = usize::min(200_000, reader.metadata().row_count as usize);

    let df_parallel = reader.read().with_limit(limit).finish().expect("parallel read");
    let df_sequential = reader
        .read()
        .sequential()
        .with_limit(limit)
        .finish()
        .expect("sequential read");

    assert!(
        df_parallel.equals_missing(&df_sequential),
        "parallel and sequential open_bytes() reads diverged for {}",
        path.display()
    );
}
