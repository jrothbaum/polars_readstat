use polars::prelude::*;
use polars_readstat_rs::{pandas_prepare_df_for_stata, StataReader, StataWriter};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    path.push(format!("{prefix}_{pid}_{nanos}.{ext}"));
    path
}

#[test]
fn test_direct_strl_roundtrip() {
    // Test with ACTUAL strL (>2045 bytes)
    let s1 = "a".repeat(3000) + "hello";
    let s2 = "b".repeat(3000) + "hell";
    let s3 = "c".repeat(3000) + "hel";

    let test_strings = vec![s1.as_str(), s2.as_str(), s3.as_str()];
    let df = df!(
        "str" => &test_strings
    )
    .unwrap();

    let dta_path = temp_path("direct_strl", "dta");
    StataWriter::new(dta_path.to_str().unwrap())
        .write_df(&df)
        .unwrap();

    let df_read = StataReader::open(dta_path.to_str().unwrap())
        .unwrap()
        .read()
        .finish()
        .unwrap();

    let original_col = df.column("str").unwrap().str().unwrap();
    let read_col = df_read.column("str").unwrap().str().unwrap();

    for i in 0..df.height() {
        let original = original_col.get(i).unwrap();
        let read_back = read_col.get(i).unwrap();
        let orig_last10 = &original[original.len().saturating_sub(10)..];
        let read_last10 = &read_back[read_back.len().saturating_sub(10)..];
        println!("Row {}: len {} -> {} | last10: {:?} -> {:?}",
            i, original.len(), read_back.len(), orig_last10, read_last10);
        assert_eq!(original.len(), read_back.len(), "Length mismatch at row {}", i);
        assert_eq!(original, read_back, "Content mismatch at row {}", i);
    }

    let _ = fs::remove_file(&dta_path);
}

#[test]
fn test_parquet_to_dta_strl_roundtrip() {
    // Create a DataFrame with a long string (>2045 bytes triggers strL)
    let long = "a".repeat(3000);
    let df = df!(
        "long_str" => &[long.as_str(), "hello"]
    )
    .unwrap();

    // Write to parquet
    let parquet_path = temp_path("test_strl", "parquet");
    let mut file = std::fs::File::create(&parquet_path).unwrap();
    ParquetWriter::new(&mut file)
        .finish(&mut df.clone())
        .unwrap();

    // Read from parquet
    let df_from_parquet = LazyFrame::scan_parquet(parquet_path.to_str().unwrap().into(), Default::default())
        .unwrap()
        .collect()
        .unwrap();

    println!("DataFrame from parquet:");
    println!("{:?}", df_from_parquet);

    // Write to .dta
    let dta_path = temp_path("test_strl", "dta");
    StataWriter::new(dta_path.to_str().unwrap())
        .write_df(&df_from_parquet)
        .unwrap();

    // Read from .dta
    let df_from_dta = StataReader::open(dta_path.to_str().unwrap())
        .unwrap()
        .read()
        .finish()
        .unwrap();

    println!("DataFrame from .dta:");
    println!("{:?}", df_from_dta);

    // Compare the long string values
    let original_col = df_from_parquet.column("long_str").unwrap().str().unwrap();
    let roundtrip_col = df_from_dta.column("long_str").unwrap().str().unwrap();

    for i in 0..df_from_parquet.height() {
        let original = original_col.get(i).unwrap();
        let roundtrip = roundtrip_col.get(i).unwrap();

        println!("Row {}: original len={}, roundtrip len={}", i, original.len(), roundtrip.len());
        println!("  Original last 20 chars: {:?}", &original[original.len().saturating_sub(20)..]);
        println!("  Roundtrip last 20 chars: {:?}", &roundtrip[roundtrip.len().saturating_sub(20)..]);

        assert_eq!(original, roundtrip, "String mismatch at row {}", i);
    }

    // Cleanup
    let _ = fs::remove_file(&parquet_path);
    let _ = fs::remove_file(&dta_path);
}

#[test]
fn test_real_parquet_file() {
    // Test with the actual random_types.parquet file
    let parquet_path = "C:/Users/jonro/Downloads/random_types.parquet";

    if !std::path::Path::new(parquet_path).exists() {
        println!("Skipping test - file does not exist: {}", parquet_path);
        return;
    }

    // Read from parquet - use ParquetReader with low_memory mode
    use polars::prelude::ParquetReader;
    let file = std::fs::File::open(parquet_path).unwrap();
    let df_from_parquet = ParquetReader::new(file)
        .set_low_memory(true)
        .set_rechunk(false)
        .finish()
        .unwrap();

    // Prepare for Stata (handles UInt8, etc.)
    let df_from_parquet = pandas_prepare_df_for_stata(&df_from_parquet).unwrap();

    println!("Columns in parquet: {:?}", df_from_parquet.get_column_names());
    println!("Shape: {:?}", df_from_parquet.shape());

    // Find string columns
    let string_cols: Vec<_> = df_from_parquet.columns()
        .iter()
        .filter(|col| matches!(col.dtype(), DataType::String))
        .map(|col| col.name())
        .collect();

    println!("String columns: {:?}", string_cols);

    // Write to .dta
    let dta_path = temp_path("random_types_test", "dta");
    StataWriter::new(&dta_path)
        .with_compress(false)
        .write_df(&df_from_parquet)
        .unwrap();

    // Read from .dta
    let df_from_dta = StataReader::open(&dta_path)
        .unwrap()
        .read()
        .finish()
        .unwrap();

    println!("DataFrame from .dta shape: {:?}", df_from_dta.shape());

    // Compare each string column
    for col_name in string_cols {
        let original_col = df_from_parquet.column(col_name).unwrap().str().unwrap();
        let roundtrip_col = df_from_dta.column(col_name).unwrap().str().unwrap();

        println!("\nChecking column: {}", col_name);

        for i in 0..df_from_parquet.height() {
            if let (Some(original), Some(roundtrip)) = (original_col.get(i), roundtrip_col.get(i)) {
                if original.len() != roundtrip.len() {
                    println!("Row {}: LENGTH MISMATCH - original={}, roundtrip={}", i, original.len(), roundtrip.len());
                    println!("  Original last 50 chars: {:?}", &original[original.len().saturating_sub(50)..]);
                    println!("  Roundtrip: {:?}", roundtrip);
                }
                assert_eq!(original, roundtrip, "String mismatch in column {} at row {}", col_name, i);
            }
        }
    }

    // Cleanup
    let _ = fs::remove_file(&dta_path);
}

#[test]
fn test_strl_write_read_exact_lengths() {
    // Test to verify StataWriter/Reader preserve exact string lengths
    // This tests if there's truncation in the library itself
    let s1 = "a".repeat(3000) + "hello";
    let s2 = "b".repeat(3000) + "hell";
    let s3 = "c".repeat(3000) + "hel";

    println!("\n===== Testing StataWriter/Reader for truncation =====");
    println!("Original strings:");
    println!("  s1 length: {}, last 10: {:?}", s1.len(), &s1[s1.len()-10..]);
    println!("  s2 length: {}, last 10: {:?}", s2.len(), &s2[s2.len()-10..]);
    println!("  s3 length: {}, last 10: {:?}", s3.len(), &s3[s3.len()-10..]);

    let df_original = df!(
        "longstr" => &[s1.as_str(), s2.as_str(), s3.as_str()]
    ).unwrap();

    let dta_path = temp_path("test_exact_lengths", "dta");

    // Write with StataWriter
    StataWriter::new(dta_path.to_str().unwrap())
        .write_df(&df_original)
        .unwrap();

    // Read back with StataReader
    let df_read = StataReader::open(dta_path.to_str().unwrap())
        .unwrap()
        .read()
        .finish()
        .unwrap();

    println!("\nAfter write/read with StataWriter/Reader:");

    let original_col = df_original.column("longstr").unwrap().str().unwrap();
    let read_col = df_read.column("longstr").unwrap().str().unwrap();

    for i in 0..3 {
        let original = original_col.get(i).unwrap();
        let read_back = read_col.get(i).unwrap();

        println!("Row {}: {} -> {} chars, last 10: {:?} -> {:?}",
            i,
            original.len(),
            read_back.len(),
            &original[original.len()-10..],
            &read_back[read_back.len()-10..]);

        if original.len() != read_back.len() {
            println!("  ⚠️  TRUNCATION DETECTED: Lost {} character(s)",
                original.len() - read_back.len());
        }

        // Check for truncation
        assert_eq!(original.len(), read_back.len(),
            "TRUNCATION: Row {} length mismatch - StataWriter/Reader lost {} character(s)",
            i, original.len() - read_back.len());
        assert_eq!(original, read_back,
            "Content mismatch at row {}", i);
    }

    println!("\n✓ No truncation detected - StataWriter/Reader preserve exact lengths");

    let _ = fs::remove_file(&dta_path);
}
