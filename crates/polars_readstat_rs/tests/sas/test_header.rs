use polars_readstat_rs::header::{check_header, read_header};
use std::fs::File;

#[test]
fn test_read_header_file1() {
    let path = "crates/cpp-sas7bdat/vendor/test/data/file1.sas7bdat";
    let mut file = File::open(path).expect("Failed to open test file");

    // Check magic number and detect format
    let (endian, format) = check_header(&mut file).expect("Failed to check header");

    println!("Detected format: {:?}, endian: {:?}", format, endian);

    // Read full header
    let header = read_header(&mut file, endian, format).expect("Failed to read header");

    println!("\n=== Header Information ===");
    println!("Format: {:?}", header.format);
    println!("Endian: {:?}", header.endian);
    println!("Platform: {:?}", header.platform);
    println!("Dataset name: {}", header.dataset_name);
    println!("File type: {}", header.file_type);
    println!("Date created: {}", header.date_created);
    println!("Date modified: {}", header.date_modified);
    println!("Header length: {}", header.header_length);
    println!("Page length: {}", header.page_length);
    println!("Page count: {}", header.page_count);
    println!("SAS release: {}", header.sas_release);
    println!("SAS server type: {}", header.sas_server_type);
    println!("OS type: {}", header.os_type);
    println!("OS name: {}", header.os_name);

    // Basic sanity checks
    assert!(header.page_length > 0, "Page length should be positive");
    assert!(header.page_count > 0, "Page count should be positive");
    assert!(header.header_length >= 288, "Header length should be at least 288 bytes");
}

#[test]
fn test_read_header_file2() {
    let path = "crates/cpp-sas7bdat/vendor/test/data/file2.sas7bdat";
    let mut file = File::open(path).expect("Failed to open test file");

    let (endian, format) = check_header(&mut file).expect("Failed to check header");
    let header = read_header(&mut file, endian, format).expect("Failed to read header");

    println!("\n=== File2 Header ===");
    println!("Format: {:?}, Endian: {:?}", header.format, header.endian);
    println!("Dataset: {}, Pages: {}", header.dataset_name, header.page_count);

    assert!(header.page_length > 0);
    assert!(header.page_count > 0);
}
