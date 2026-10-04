mod common;

use common::{sas_files, spss_files, stata_files};
use polars::prelude::*;
use polars_readstat_rs::{readstat_batch_iter, readstat_batch_iter_with_offset, ScanOptions};
use std::path::Path;

fn collect(
    path: &Path,
    offset: usize,
    n_rows: Option<usize>,
    batch_size: usize,
) -> PolarsResult<DataFrame> {
    let opts = ScanOptions {
        preserve_order: Some(true),
        ..Default::default()
    };
    let iter = readstat_batch_iter_with_offset(
        path,
        Some(opts),
        None,
        None,
        offset,
        n_rows,
        Some(batch_size),
    )?;
    let frames: Vec<DataFrame> = iter.collect::<PolarsResult<_>>()?;
    let mut it = frames.into_iter();
    let Some(mut acc) = it.next() else {
        return Ok(DataFrame::empty());
    };
    for f in it {
        acc.vstack_mut(&f)?;
    }
    Ok(acc)
}

fn check_file(path: &Path) {
    let Ok(full) = collect(path, 0, None, 100) else {
        return;
    };
    let h = full.height();
    if h < 10 {
        return;
    }
    // Offset equals the reference slice, with and without a limit.
    for (offset, n) in [(3usize, Some(5usize)), (h / 2, None), (h - 1, Some(10))] {
        let got = collect(path, offset, n, 7).unwrap();
        let len = n.unwrap_or(h).min(h - offset);
        assert!(
            got.equals_missing(&full.slice(offset as i64, len)),
            "mismatch for {} offset={offset} n={n:?}",
            path.display()
        );
    }
    // Offset past the end yields no rows.
    assert_eq!(collect(path, h + 5, None, 7).unwrap().height(), 0);
    // The offset-free entry point is unchanged.
    let plain: Vec<DataFrame> = readstat_batch_iter(path, None, None, None, Some(4), None)
        .unwrap()
        .collect::<PolarsResult<_>>()
        .unwrap();
    assert_eq!(plain.iter().map(|d| d.height()).sum::<usize>(), 4);
}

#[test]
fn offset_matches_slice_of_full_read() {
    let mut files = sas_files();
    files.extend(stata_files());
    files.extend(spss_files());
    let mut checked = 0;
    for f in files.iter().filter(|f| {
        std::fs::metadata(f).map(|m| m.len() < 20_000_000).unwrap_or(false)
    }) {
        check_file(f);
        checked += 1;
    }
    assert!(checked > 0, "no test data found");
}
