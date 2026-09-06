//! Lighter parity checks for the formats that don't have a persistent
//! `*Reader` struct with `open`/`open_bytes` (catalogs, XPT, POR) — each of
//! these compares the path-based entry point against the same bytes routed
//! through an `InMemorySource`.

use polars_readstat_rs::{
    metadata_por, metadata_por_from_source, read_por, read_por_from_source, read_sas7bcat,
    read_sas7bcat_from_source, read_xpt_metadata, read_xpt_metadata_from_source, InMemorySource,
};
use std::path::{Path, PathBuf};

fn first_existing(candidates: &[&str]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(PathBuf::from)
        .find(|p| p.exists())
}

#[test]
fn catalog_from_source_matches_path() {
    let Some(path) = first_existing(&[
        "tests/sas/data/too_big/sas_format/formats.sas7bcat",
        "tests/sas/data/data_gov/formats.sas7bcat",
    ]) else {
        return;
    };

    let via_path = read_sas7bcat(&path).expect("read_sas7bcat");
    let bytes = std::fs::read(&path).expect("read fixture bytes");
    let source = InMemorySource::new(bytes);
    let via_source = read_sas7bcat_from_source(&source).expect("read_sas7bcat_from_source");

    assert_eq!(via_path, via_source, "catalog contents diverged for {}", path.display());
}

#[test]
fn xpt_metadata_from_source_matches_path() {
    let Some(path) = first_existing(&[
        "tests/sas/data/xpt/sample.xpt",
        "tests/sas/data/xpt/ACQ_G.xpt",
    ]) else {
        return;
    };

    let via_path = read_xpt_metadata(Path::new(&path)).expect("read_xpt_metadata");
    let bytes = std::fs::read(&path).expect("read fixture bytes");
    let source = InMemorySource::new(bytes);
    let via_source =
        read_xpt_metadata_from_source(&source).expect("read_xpt_metadata_from_source");

    assert_eq!(via_path.row_count, via_source.row_count);
    assert_eq!(via_path.columns.len(), via_source.columns.len());
    assert!(
        via_path.metadata_df.equals_missing(&via_source.metadata_df),
        "XPT metadata_df diverged for {}",
        path.display()
    );
}

#[test]
fn por_from_source_matches_path() {
    let Some(path) = first_existing(&["tests/spss/data/sample.por"]) else {
        return;
    };

    let (meta_path, df_path) = read_por(&path).expect("read_por");
    let bytes = std::fs::read(&path).expect("read fixture bytes");
    let source = InMemorySource::new(bytes);
    let (meta_source, df_source) = read_por_from_source(&source).expect("read_por_from_source");

    assert_eq!(meta_path.row_count, meta_source.row_count);
    assert!(
        df_path.equals_missing(&df_source),
        "POR data diverged for {}",
        path.display()
    );

    let bytes2 = std::fs::read(&path).expect("read fixture bytes");
    let source2 = InMemorySource::new(bytes2);
    let meta_only_path = metadata_por(&path).expect("metadata_por");
    let meta_only_source =
        metadata_por_from_source(&source2).expect("metadata_por_from_source");
    assert_eq!(meta_only_path.row_count, meta_only_source.row_count);
}
