//! Regression tests for how much a reader pulls from an object store: bytes fetched
//! vs. the file size, and the number of range requests (each is a network round trip
//! against S3/GCS/Azure). Uses the real cloud reader (`ObjectStoreSource`) over a
//! local-filesystem store, so there is no network and no credentials.
//!
//! These guard against: chunk workers re-reading data they discard (Stata read the
//! `<data>` tag through an 8MB buffer per chunk: 2x the file), tiny seeks each costing
//! a request (Stata value-label tables), and a window refetched after a jump to the
//! file trailer (SPSS zsav).
#![cfg(feature = "cloud")]

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use polars::prelude::*;
use polars_readstat_rs::reader::Sas7bdatReader;
use polars_readstat_rs::{
    ObjectStoreSource, SpssReader, StataReader, StataWriter, ValueLabelMap, ValueLabels,
};
use polars_readstat_rs::SpssWriter;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A temp file path that is removed on drop.
struct TempPath(PathBuf);
impl TempPath {
    fn new(name: &str, ext: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "prs-fetch-eff-{}-{name}.{ext}",
            std::process::id()
        )))
    }
}
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn source(path: &Path) -> Arc<ObjectStoreSource> {
    Arc::new(
        ObjectStoreSource::new(
            Arc::new(LocalFileSystem::new()),
            ObjectPath::from_absolute_path(path).unwrap(),
        )
        .unwrap(),
    )
}

/// `rows` x `cols` of f64 — roughly `rows * cols * 8` bytes in a Stata/SPSS file.
fn numeric_df(rows: usize, cols: usize) -> DataFrame {
    let columns: Vec<Column> = (0..cols)
        .map(|c| {
            let v: Vec<f64> = (0..rows).map(|r| (r * 31 + c) as f64 * 0.25).collect();
            Series::new(format!("v{c}").into(), v).into()
        })
        .collect();
    DataFrame::new(rows, columns).unwrap()
}

/// Asserts the reader fetched at most `max_ratio` x the file (plus 1MiB for metadata
/// windows) and no more than `max_requests` range requests.
fn assert_efficient(
    what: &str,
    file_len: u64,
    src: &ObjectStoreSource,
    max_requests: usize,
    max_ratio: f64,
) {
    let (requests, bytes) = src.fetch_stats();
    let ratio = bytes as f64 / file_len as f64;
    eprintln!("{what}: {requests} requests, {bytes} bytes ({ratio:.3}x of {file_len})");
    assert!(
        bytes <= (file_len as f64 * max_ratio) as u64 + (1 << 20),
        "{what}: fetched {bytes} bytes for a {file_len}-byte file ({ratio:.2}x)"
    );
    assert!(
        requests <= max_requests,
        "{what}: {requests} range requests (limit {max_requests})"
    );
}

/// Number of requests a sequential 1MiB-window reader could need, with slack.
fn request_budget(file_len: u64) -> usize {
    (file_len / (1 << 20)) as usize + 24
}

#[test]
fn stata_chunked_read_fetches_each_byte_once() {
    let path = TempPath::new("stata-wide", "dta");
    let df = numeric_df(150_000, 25); // ~30MB, many parallel chunks
    StataWriter::new(&path.0).write_df(&df).unwrap();
    let file_len = std::fs::metadata(&path.0).unwrap().len();

    let src = source(&path.0);
    let out = StataReader::open_source(src.clone()).unwrap().read().finish().unwrap();
    assert_eq!(out.shape(), df.shape());
    assert_efficient("stata chunked", file_len, &src, request_budget(file_len), 1.10);
}

#[test]
fn stata_many_value_label_tables_do_not_cost_a_request_each() {
    let path = TempPath::new("stata-labels", "dta");
    let (rows, cols) = (4_000, 400);
    let columns: Vec<Column> = (0..cols)
        .map(|c| {
            let v: Vec<i32> = (0..rows).map(|r| ((r + c) % 3) as i32).collect();
            Series::new(format!("v{c}").into(), v).into()
        })
        .collect();
    let df = DataFrame::new(rows, columns).unwrap();
    let labels: ValueLabels = (0..cols)
        .map(|c| {
            let map: ValueLabelMap = BTreeMap::from([
                (0, "zero".to_string()),
                (1, "one".to_string()),
                (2, "two".to_string()),
            ]);
            (format!("v{c}"), map)
        })
        .collect();
    StataWriter::new(&path.0).with_value_labels(labels).write_df(&df).unwrap();
    let file_len = std::fs::metadata(&path.0).unwrap().len();

    let src = source(&path.0);
    let reader = StataReader::open_source(src.clone()).unwrap();
    // 400 label tables are parsed at open; with one request per table this was 400+.
    let (open_requests, _) = src.fetch_stats();
    assert!(open_requests <= 8, "{open_requests} requests just to open");
    let out = reader.read().finish().unwrap();
    assert_eq!(out.shape(), df.shape());
    assert_efficient("stata labels", file_len, &src, request_budget(file_len), 1.50);
}

#[test]
fn spss_sav_fetches_about_the_file_size() {
    let path = TempPath::new("spss-sav", "sav");
    let df = numeric_df(120_000, 25); // ~24MB
    SpssWriter::new(&path.0).write_df(&df).unwrap();
    let file_len = std::fs::metadata(&path.0).unwrap().len();

    let src = source(&path.0);
    let out = SpssReader::open_source(src.clone()).unwrap().read().finish().unwrap();
    assert_eq!(out.shape(), df.shape());
    assert_efficient("spss sav", file_len, &src, request_budget(file_len), 1.10);
}

#[test]
fn spss_bytecode_compressed_sav_fetches_about_the_file_size() {
    // Compression code 1 ("bytecode"); the Rust writer cannot write real zlib .zsav.
    let path = TempPath::new("spss-sav-bytecode", "sav");
    let df = numeric_df(120_000, 25);
    SpssWriter::new(&path.0).with_compression(true).write_df(&df).unwrap();
    let file_len = std::fs::metadata(&path.0).unwrap().len();

    let src = source(&path.0);
    let out = SpssReader::open_source(src.clone()).unwrap().read().finish().unwrap();
    assert_eq!(out.shape(), df.shape());
    assert_efficient("spss bytecode sav", file_len, &src, request_budget(file_len), 1.10);
}

#[test]
fn spss_zsav_does_not_refetch_after_the_trailer_jump() {
    // A real zlib .zsav (106MB fixture); skipped where it isn't checked out. The
    // trailer jump used to cost one extra 8MB window (~1.09x), hence the tight bound.
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/spss/data/too_big/psam_p17.zsav");
    if !path.exists() {
        eprintln!("skipping: {} not present", path.display());
        return;
    }
    let file_len = std::fs::metadata(&path).unwrap().len();
    let src = source(&path);
    let out = SpssReader::open_source(src.clone()).unwrap().read().finish().unwrap();
    assert!(out.height() > 0);
    assert_efficient("spss zsav", file_len, &src, request_budget(file_len), 1.04);
}

#[test]
fn sas7bdat_fetches_about_the_file_size() {
    // A large multi-chunk fixture; skipped where it isn't checked out.
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sas/data/too_big/psam_p17.sas7bdat");
    if !path.exists() {
        eprintln!("skipping: {} not present", path.display());
        return;
    }
    let file_len = std::fs::metadata(&path).unwrap().len();
    let src = source(&path);
    let out = Sas7bdatReader::open_source(src.clone()).unwrap().read().finish().unwrap();
    assert!(out.height() > 0);
    assert_efficient("sas7bdat", file_len, &src, request_budget(file_len), 1.05);
}
