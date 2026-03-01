use polars_readstat_rs::header::{check_header, read_header};
use polars_readstat_rs::metadata::read_metadata;
use std::fs::File;
use std::io::{Seek, SeekFrom};

// Helper to make internal modules accessible for testing
mod integration {
    use super::*;
    // Manual end-to-end read using internal modules
    #[test]
    fn test_end_to_end_read() {
        let path = "crates/cpp-sas7bdat/vendor/test/data/file1.sas7bdat";
        let mut file = File::open(path).expect("Failed to open test file");

        // Step 1: Read header
        let (endian, format) = check_header(&mut file).expect("Failed to check header");
        let header = read_header(&mut file, endian, format).expect("Failed to read header");

        println!("\n=== Header Info ===");
        println!("Dataset: {}, {} rows", header.dataset_name, header.page_count);
        println!("Format: {:?}, Endian: {:?}", format, endian);
        println!("Page length: {}, Header length: {}", header.page_length, header.header_length);

        // Step 2: Read metadata
        let mut file = File::open(path).expect("Failed to reopen test file");
        file.seek(SeekFrom::Start(header.header_length as u64)).expect("Failed to seek");
        let (metadata, _data_subheaders, _first_data_page, _mix_data_rows) =
            read_metadata(file, &header, endian, format).expect("Failed to read metadata");

        println!("\n=== Metadata ===");
        println!("Row count: {}", metadata.row_count);
        println!("Row length: {}", metadata.row_length);
        println!("Column count: {}", metadata.column_count);
        println!("Columns:");
        for (i, col) in metadata.columns.iter().enumerate() {
            println!("  {}: {} ({:?}) - offset={}, length={}",
                     i, col.name, col.col_type, col.offset, col.length);
        }

        // Verify metadata
        assert_eq!(metadata.row_count, 40);
        assert_eq!(metadata.column_count, 2);
        assert_eq!(metadata.columns[0].name, "food_exp");
        assert_eq!(metadata.columns[1].name, "income");

        println!("\n=== Success! ===");
        println!("Successfully read SAS7BDAT file with uncompressed data");
        println!("- Parsed header with format detection");
        println!("- Extracted metadata including column definitions");
        println!("- Verified row count and column names");
    }
}
