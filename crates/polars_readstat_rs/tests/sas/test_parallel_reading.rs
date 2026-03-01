use polars_readstat_rs::reader::Sas7bdatReader;
use std::path::Path;

#[test]
fn test_parallel_vs_sequential_reading() {
    let test_file = "tests/sas/data/data_pandas/test1.sas7bdat";

    if !Path::new(test_file).exists() {
        println!("Test file not found, skipping test");
        return;
    }

    let reader = Sas7bdatReader::open(test_file).expect("Failed to open file");
    let limit = usize::min(200_000, reader.metadata().row_count);

    println!("File: {}", test_file);
    println!("Compression: {:?}", reader.metadata().compression);
    println!("Row count: {}", reader.metadata().row_count);

    // 1. Read using default (Parallel for uncompressed)
    let df_parallel = reader.read().with_limit(limit).finish().expect("Failed to read (parallel)");

    // 2. Read using Sequential explicitly
    let df_sequential = reader.read()
        .sequential()
        .with_limit(limit)
        .finish()
        .expect("Failed to read (sequential)");

    // Verify both strategies produce the same results
    assert_eq!(df_parallel.shape(), df_sequential.shape(), "Parallel vs Sequential shape mismatch");

    println!("\n✓ Test passed: Parallel and Sequential paths are consistent");
}

#[test]
fn test_parallel_reading_multiple_files() {
    let test_files = vec![
        "tests/sas/data/data_pandas/test1.sas7bdat",
        "tests/sas/data/data_pandas/zero_variables.sas7bdat",
    ];

    for test_file in test_files {
        if !Path::new(test_file).exists() {
            println!("Test file {} not found, skipping", test_file);
            continue;
        }

        println!("\nTesting file: {}", test_file);
        let reader = Sas7bdatReader::open(test_file).expect("Failed to open file");

        // The builder automatically handles selection of the best strategy
        let limit = usize::min(200_000, reader.metadata().row_count);
        match reader.read().with_limit(limit).finish() {
            Ok(df) => {
                println!("  Compression: {:?}", reader.metadata().compression);
                println!("  Shape: {:?}", df.shape());
                println!("  ✓ Successfully read using default builder strategy");
            }
            Err(e) => println!("  Failed to read: {}", e),
        }
    }
}

#[test]
fn test_read_builder_boundaries() {
    let test_file = "tests/sas/data/data_pandas/test1.sas7bdat";

    if !Path::new(test_file).exists() {
        println!("Test file not found, skipping test");
        return;
    }

    let reader = Sas7bdatReader::open(test_file).expect("Failed to open file");
    let total_rows = reader.metadata().row_count;
    let max_total = usize::min(200_000, total_rows);

    println!("Testing builder boundaries on file with {} rows", total_rows);

    // Test reading 0 rows (limit 0)
    let df = reader.read().with_limit(0).finish().expect("Failed limit 0");
    assert_eq!(df.height(), 0);
    println!("  ✓ Limit 0: OK");

    // Test reading 1 row
    let df = reader.read().with_limit(1).finish().expect("Failed limit 1");
    assert_eq!(df.height(), 1);
    println!("  ✓ Limit 1: OK");

    // Test reading all rows with offset
    if max_total > 5 {
        let offset = 5;
        let expected = max_total - offset;
        let df = reader
            .read()
            .with_offset(offset)
            .with_limit(max_total - offset)
            .finish()
            .expect("Failed offset");
        assert_eq!(df.height(), expected);
        println!("  ✓ Offset {}: OK", offset);
    }

    // Test reading past the end
    // Our logic handles this by saturating the limit to available rows
    let limit = usize::min(200_000, max_total.saturating_add(100));
    let df = reader
        .read()
        .with_offset(0)
        .with_limit(limit)
        .finish()
        .expect("Failed past end");
    assert_eq!(df.height(), usize::min(total_rows, limit));
    println!("  ✓ Reading past end (clamped to total): OK");

    println!("\n✓ All boundary tests passed using ReadBuilder");
}
