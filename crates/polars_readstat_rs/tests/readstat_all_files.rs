mod common;

use common::{sas_files, spss_files, stata_files};
use polars_readstat_rs::{
    readstat_metadata_json, readstat_schema, ReadStatFormat, Sas7bdatReader, SpssReader,
    StataReader,
};

const MAX_ROWS: usize = 100_000;
const STREAM_BATCH: usize = 10_000;

#[test]
fn test_sas_all_files() {
    for path in sas_files() {
        // Regular loader (Polars DF)
        let reader = Sas7bdatReader::open(&path).expect("open sas");
        let row_count = reader.metadata().row_count;
        let mut limit = usize::min(MAX_ROWS, row_count);
        if reader.metadata().column_count == 0 {
            limit = 0;
        }
        if limit == 0 {
            let schema =
                readstat_schema(&path, None, Some(ReadStatFormat::Sas)).expect("schema sas");
            assert_eq!(schema.len(), reader.metadata().column_count);
            let meta =
                readstat_metadata_json(&path, Some(ReadStatFormat::Sas)).expect("metadata sas");
            assert!(!meta.trim().is_empty());
            continue;
        }

        let df = reader.read().with_limit(limit).finish().expect("read sas");
        assert_eq!(df.height(), limit);

        // Schema
        let schema = readstat_schema(&path, None, Some(ReadStatFormat::Sas)).expect("schema sas");
        assert_eq!(schema.len(), reader.metadata().column_count);

        // Metadata JSON
        let meta = readstat_metadata_json(&path, Some(ReadStatFormat::Sas)).expect("metadata sas");
        assert!(!meta.trim().is_empty());

        // Streaming loader (batch read via offsets)
        let mut rows = 0usize;
        let mut offset = 0usize;
        while offset < limit {
            let take = usize::min(STREAM_BATCH, limit - offset);
            let df = reader
                .read()
                .with_offset(offset)
                .with_limit(take)
                .finish()
                .expect("sas batch");
            rows += df.height();
            offset += take;
        }
        assert_eq!(rows, limit);
    }
}

#[test]
fn test_stata_all_files() {
    for path in stata_files() {
        let reader = StataReader::open(&path).expect("open stata");
        let row_count = reader.metadata().row_count as usize;
        let limit = usize::min(MAX_ROWS, row_count);
        let df = reader
            .read()
            .with_limit(limit)
            .finish()
            .expect("read stata");
        assert_eq!(df.height(), limit);

        let schema =
            readstat_schema(&path, None, Some(ReadStatFormat::Stata)).expect("schema stata");
        assert_eq!(schema.len(), reader.metadata().variables.len());

        let meta =
            readstat_metadata_json(&path, Some(ReadStatFormat::Stata)).expect("metadata stata");
        assert!(!meta.trim().is_empty());

        let mut rows = 0usize;
        let mut offset = 0usize;
        while offset < limit {
            let take = usize::min(STREAM_BATCH, limit - offset);
            let df = reader
                .read()
                .with_offset(offset)
                .with_limit(take)
                .finish()
                .expect("stata batch");
            rows += df.height();
            offset += take;
        }
        assert_eq!(rows, limit);
    }
}

#[test]
fn test_spss_all_files() {
    for path in spss_files() {
        let reader = SpssReader::open(&path).expect("open spss");
        let row_count = reader.metadata().row_count as usize;
        let limit = usize::min(MAX_ROWS, row_count);
        let df = reader.read().with_limit(limit).finish().expect("read spss");
        assert_eq!(df.height(), limit);

        let schema = readstat_schema(&path, None, Some(ReadStatFormat::Spss)).expect("schema spss");
        assert_eq!(schema.len(), reader.metadata().variables.len());

        let meta =
            readstat_metadata_json(&path, Some(ReadStatFormat::Spss)).expect("metadata spss");
        assert!(!meta.trim().is_empty());

        let mut rows = 0usize;
        let mut offset = 0usize;
        while offset < limit {
            let take = usize::min(STREAM_BATCH, limit - offset);
            let df = reader
                .read()
                .with_offset(offset)
                .with_limit(take)
                .finish()
                .expect("spss batch");
            rows += df.height();
            offset += take;
        }
        assert_eq!(rows, limit);
    }
}
