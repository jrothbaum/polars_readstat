mod common;

use polars_readstat_rs::reader::Sas7bdatReader;
use common::{all_sas_files, test_data_path};

#[test]
fn test_all_files_can_be_opened() {
    let files = all_sas_files();
    println!("Testing {} files...", files.len());

    let mut failed = Vec::new();
    let mut succeeded = 0;

    for file in &files {
        match Sas7bdatReader::open(file) {
            Ok(_) => succeeded += 1,
            Err(e) => failed.push((file.clone(), e)),
        }
    }

    println!("Success: {}/{}", succeeded, files.len());
    assert_eq!(failed.len(), 0, "{} files failed to open", failed.len());
}

#[test]
fn test_all_files_can_read_metadata() {
    let files = all_sas_files();
    let mut failed = Vec::new();

    for file in &files {
        match Sas7bdatReader::open(file) {
            Ok(reader) => {
                let metadata = reader.metadata();
                if metadata.row_count == 0 && metadata.column_count == 0 {
                    failed.push((file.clone(), "Zero rows and columns".to_string()));
                }
            }
            Err(e) => failed.push((file.clone(), e.to_string())),
        }
    }

    assert_eq!(failed.len(), 0, "{} files failed metadata check", failed.len());
}

#[test]
fn test_all_files_can_read_data() {
    let files = all_sas_files();
    let mut failed = Vec::new();
    for file in &files {
        if file.to_string_lossy().contains("zero_variables.sas7bdat") {
            continue;
        }

        // Limit check for test speed
        // file size already filtered above

        match Sas7bdatReader::open(file) {
            Ok(reader) => {
                // Using new builder pattern
                let limit = usize::min(200_000, reader.metadata().row_count);
                match reader.read().with_limit(limit).finish() {
                    Ok(df) => {
                        assert_eq!(df.height(), limit);
                        assert_eq!(df.width(), reader.metadata().column_count);
                    }
                    Err(e) => failed.push((file.clone(), e.to_string())),
                }
            }
            Err(e) => failed.push((file.clone(), e.to_string())),
        }
    }
    assert_eq!(failed.len(), 0, "{} files failed to read data", failed.len());
}

#[test]
fn test_batch_reading_matches_full_read() {
    let test_file = test_data_path("data_pandas/test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();
    let limit = usize::min(200_000, reader.metadata().row_count);
    let full_df = reader.read().with_limit(limit).finish().unwrap();

    let mid = full_df.height() / 2;
    
    // Batch 1 using Builder
    let b1 = reader.read()
        .with_offset(0)
        .with_limit(mid)
        .finish().unwrap();

    // Batch 2 using Builder
    let b2 = reader.read()
        .with_offset(mid)
        .with_limit(full_df.height() - mid)
        .finish().unwrap();

    assert_eq!(b1.height() + b2.height(), full_df.height());
}

#[test]
fn test_parallel_and_sequential_match() {
    let test_file = test_data_path("test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();

    // 1. Parallel (default)
    let limit = usize::min(200_000, reader.metadata().row_count);
    let df_par = reader.read().with_limit(limit).finish().unwrap();

    // 2. Sequential
    let df_seq = reader.read().sequential().with_limit(limit).finish().unwrap();

    assert_eq!(df_par.height(), df_seq.height());
}

#[test]
fn test_column_selection_builder() {
    let test_file = test_data_path("test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();
    let selected: Vec<String> = reader
        .metadata()
        .columns
        .iter()
        .take(2)
        .map(|c| c.name.clone())
        .collect();

    if selected.len() < 2 {
        return;
    }

    let df = reader.read()
        .with_columns(selected.clone())
        .with_limit(usize::min(200_000, reader.metadata().row_count))
        .finish()
        .unwrap();

    assert_eq!(df.width(), 2);
    assert!(df.column(&selected[0]).is_ok());
    assert!(df.column(&selected[1]).is_ok());
}

#[test]
fn test_large_file_streaming_default() {
    let files = all_sas_files();

    for file in &files {
        if let Ok(metadata) = std::fs::metadata(file) {
            if metadata.len() > 100 * 1024 * 1024 && metadata.len() < 1024 * 1024 * 1024 { // 100MB-1GB (skip 11GB+ files)
                let reader = Sas7bdatReader::open(file).unwrap();
                
                // For large files, test streaming with a limit
                // This validates the I/O + Worker logic without filling RAM
                let df = reader.read()
                    .with_limit(5000)
                    .finish()
                    .unwrap();

                assert_eq!(df.height(), 5000);
                println!("✓ Streaming limit test passed for {}", file.display());
            }
        }
    }
}

#[test]
fn test_error_on_missing_column() {
    let test_file = test_data_path("test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();
    let res = reader.read()
        .with_columns(vec!["TOTALLY_REAL_COLUMN".into()])
        .finish();

    assert!(res.is_err());
}
