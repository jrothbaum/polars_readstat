use polars_arrow::ffi::ArrowArrayStreamReader;
use polars_readstat_rs::sas::arrow_output::read_to_arrow_stream_ffi;
use std::path::Path;

fn count_rows_via_ffi(path: &Path, batch_size: usize) -> usize {
    let ptr = read_to_arrow_stream_ffi(path, None, true, None, 0, None).expect("stream");
    let boxed = unsafe { Box::from_raw(ptr) };
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(boxed).expect("reader") };
    let mut total = 0;
    while let Some(result) = unsafe { reader.next() } {
        total += result.expect("batch").len();
    }
    total
}

#[test]
fn test_arrow_stream_row_count() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sas/data/data_AHS2013/owner.sas7bdat");

    let expected = {
        let reader = polars_readstat_rs::Sas7bdatReader::open(&path).unwrap();
        reader.metadata().row_count
    };

    for batch_size in [2048, 8192, 65536] {
        let got = count_rows_via_ffi(&path, batch_size);
        assert_eq!(
            got, expected,
            "batch_size={batch_size}: got {got} rows, expected {expected}"
        );
    }
}

#[test]
fn test_mix_page_info() {
    for name in &[
        "data_AHS2013/owner.sas7bdat",
        "sas_to_csv/drugprob.sas7bdat",
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/sas/data")
            .join(name);
        if !path.exists() {
            continue;
        }
        let reader = polars_readstat_rs::Sas7bdatReader::open(&path).unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len();
        let page_len = reader.header().page_length;
        let header_len = reader.header().header_length;
        let total_pages = ((file_len - header_len as u64) / page_len as u64) as usize;
        println!("{}: row_count={} mix_data_rows={} first_data_page={} total_pages={} page_len={} row_len={}",
            name, reader.metadata().row_count, reader.mix_data_rows(), reader.first_data_page(),
            total_pages, page_len, reader.metadata().row_length);
    }
}

#[test]
fn test_worker_row_counts() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/sas/data/data_AHS2013/owner.sas7bdat");
    if !path.exists() {
        return;
    }
    // Force 2 threads with various batch sizes
    for batch_size in [512usize, 2048, 65536] {
        let opts = polars_readstat_rs::ScanOptions {
            threads: Some(2),
            chunk_size: Some(batch_size),
            ..Default::default()
        };
        let iter = polars_readstat_rs::readstat_batch_iter(
            &path,
            Some(opts),
            None,
            None,
            None,
            Some(batch_size),
        )
        .expect("iter");
        let mut total = 0usize;
        for batch in iter {
            total += batch.expect("batch").height();
        }
        let expected = 27570;
        println!(
            "batch_size={batch_size}: got {total} rows (expected {expected}) diff={}",
            expected as isize - total as isize
        );
    }
}
