use polars_readstat_rs::header::{check_header, read_header};
use polars_readstat_rs::metadata::read_metadata;
use std::fs::File;

#[test]
fn test_read_metadata_file1() {
    let path = "crates/cpp-sas7bdat/vendor/test/data/file1.sas7bdat";
    let mut file = File::open(path).expect("Failed to open test file");

    // Read header
    let (endian, format) = check_header(&mut file).expect("Failed to check header");
    let header = read_header(&mut file, endian, format).expect("Failed to read header");

    println!("\n=== Header Info ===");
    println!("Dataset: {}, {} rows in {} pages", header.dataset_name, header.page_count, header.page_length);

    // Read metadata
    let (metadata, _data_subheaders, _first_data_page, _mix_data_rows) =
        read_metadata(file, &header, endian, format).expect("Failed to read metadata");

    println!("\n=== Metadata ===");
    println!("Compression: {:?}", metadata.compression);
    println!("Row count: {}", metadata.row_count);
    println!("Row length: {}", metadata.row_length);
    println!("Column count: {}", metadata.column_count);

    println!("\n=== Columns ===");
    for (i, col) in metadata.columns.iter().enumerate() {
        println!("  {}: {} ({:?}) - offset={}, length={}",
            i, col.name, col.col_type, col.offset, col.length);
    }

    // Assertions
    assert!(metadata.row_count > 0, "Should have at least one row");
    assert!(metadata.column_count > 0, "Should have at least one column");
    assert_eq!(metadata.columns.len(), metadata.column_count, "Column vec should match count");

    // Check that columns have names
    for col in &metadata.columns {
        assert!(!col.name.is_empty(), "Column should have a name");
    }
}
