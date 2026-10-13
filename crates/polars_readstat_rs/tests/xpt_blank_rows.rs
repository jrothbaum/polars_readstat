//! XPT rows that are entirely blank are real rows unless they are trailing padding.
//! Offset reads and parallel chunk boundaries must not drop or shift them.

use polars::prelude::*;
use polars_readstat_rs::{readstat_batch_iter_with_offset, ScanOptions, XptWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// The last row (index 3002) is non-blank, so nothing is trailing padding.
const ROWS: usize = 3003;

fn temp_xpt() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "polars_readstat_rs_xpt_blank_{}_{}.xpt",
        std::process::id(),
        nanos
    ))
}

/// One character column, so an empty value makes the whole row blank.
/// Rows 3 and 4 of every 7 are blank.
fn sample_df() -> DataFrame {
    let s: Vec<String> = (0..ROWS)
        .map(|i| {
            if matches!(i % 7, 3 | 4) {
                String::new()
            } else {
                format!("{}", i % 97)
            }
        })
        .collect();
    df!("s" => s).unwrap()
}

fn read(path: &Path, threads: usize, offset: usize, n_rows: Option<usize>, batch: usize) -> DataFrame {
    let opts = ScanOptions {
        threads: Some(threads),
        preserve_order: Some(true),
        ..Default::default()
    };
    let frames: Vec<DataFrame> =
        readstat_batch_iter_with_offset(path, Some(opts), None, None, offset, n_rows, Some(batch))
            .unwrap()
            .collect::<PolarsResult<_>>()
            .unwrap();
    let mut it = frames.into_iter();
    let Some(mut acc) = it.next() else {
        return DataFrame::empty();
    };
    for f in it {
        acc.vstack_mut(&f).unwrap();
    }
    acc
}

#[test]
fn xpt_blank_rows_survive_offsets_and_chunking() {
    let path = temp_xpt();
    XptWriter::new(&path).write_df(&sample_df()).unwrap();

    let full = read(&path, 1, 0, None, 10_000);
    assert_eq!(full.height(), ROWS);

    // Offsets landing before, inside and just after a blank run.
    for offset in [3usize, 4, 5, 10, 11, 12, 1502, 1503, 1504] {
        let got = read(&path, 1, offset, Some(20), 10_000);
        assert!(
            got.equals_missing(&full.slice(offset as i64, 20)),
            "serial offset={offset}"
        );
    }

    // Chunk sizes whose boundaries fall inside blank runs.
    for batch in [7usize, 11, 100] {
        let par = read(&path, 4, 0, None, batch);
        assert!(par.equals_missing(&full), "parallel batch={batch}");
    }

    let _ = std::fs::remove_file(&path);
}
