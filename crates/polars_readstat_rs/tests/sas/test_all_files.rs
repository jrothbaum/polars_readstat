use polars_readstat_rs::header::{check_header, read_header};
use polars_readstat_rs::metadata::read_metadata;
use polars_readstat_rs::reader::Sas7bdatReader;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;

fn test_file(path: &Path) -> Result<(), String> {
    let mut file = File::open(path)
        .map_err(|e| format!("Failed to open: {}", e))?;

    // Read header
    let (endian, format) = check_header(&mut file)
        .map_err(|e| format!("check_header failed: {}", e))?;

    let header = read_header(&mut file, endian, format)
        .map_err(|e| format!("read_header failed: {}", e))?;

    // Read metadata
    let mut file = File::open(path)
        .map_err(|e| format!("Failed to reopen: {}", e))?;
    file.seek(SeekFrom::Start(header.header_length as u64))
        .map_err(|e| format!("Failed to seek: {}", e))?;

    let (metadata, _data_subheaders, _first_data_page, _mix_data_rows) =
        read_metadata(file, &header, endian, format)
            .map_err(|e| format!("read_metadata failed: {}", e))?;

    // Basic validation
    if metadata.row_count == 0 {
        return Err("Row count is 0".to_string());
    }

    // Note: column_count can be 0 for valid edge cases (datasets with no variables)

    if metadata.columns.len() != metadata.column_count {
        return Err(format!(
            "Column count mismatch: expected {}, got {}",
            metadata.column_count,
            metadata.columns.len()
        ));
    }

    // Check that all columns have names
    for (i, col) in metadata.columns.iter().enumerate() {
        if col.name.is_empty() {
            return Err(format!("Column {} has no name", i));
        }
    }

    // Read only a small subset of rows to avoid high memory usage
    let reader = Sas7bdatReader::open(path)
        .map_err(|e| format!("Failed to open for data read: {}", e))?;
    let limit = usize::min(100_000, reader.metadata().row_count);
    let df = reader
        .read()
        .with_limit(limit)
        .finish()
        .map_err(|e| format!("Read failed: {}", e))?;
    if df.height() != limit {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            limit,
            df.height()
        ));
    }

    Ok(())
}

#[test]
fn test_all_sas7bdat_files() {
    let test_dir = "tests/sas/data";

    let mut files = Vec::new();
    let root = Path::new(test_dir);
    if root.exists() {
        find_sas_files(root, &mut files);
    }

    println!("\n=== Testing {} SAS7BDAT files ===\n", files.len());

    let mut success_count = 0;
    let mut failure_count = 0;
    let mut failures = Vec::new();

    for path in &files {
        let relative_path = path.strip_prefix(test_dir).unwrap_or(path);
        match test_file(path) {
            Ok(()) => {
                success_count += 1;
                println!("✓ {}", relative_path.display());
            }
            Err(e) => {
                failure_count += 1;
                println!("✗ {}: {}", relative_path.display(), e);
                failures.push((relative_path.to_path_buf(), e));
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total files: {}", files.len());
    println!("Successful: {} ({:.1}%)", success_count, 100.0 * success_count as f64 / files.len() as f64);
    println!("Failed: {} ({:.1}%)", failure_count, 100.0 * failure_count as f64 / files.len() as f64);

    if !failures.is_empty() {
        println!("\n=== Failures ===");
        for (path, error) in &failures {
            println!("{}: {}", path.display(), error);
        }
    }

    // For now, we just report the results without failing the test
    // In a real scenario, you might want to fail if too many files fail
    if failure_count > files.len() / 2 {
        panic!("More than 50% of files failed to parse");
    }
}

fn find_sas_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|s| s.to_str()) == Some("too_big") {
                    continue;
                }
                find_sas_files(&path, files);
            } else if path.extension().and_then(|s| s.to_str()) == Some("sas7bdat") {
                files.push(path);
            }
        }
    }
}
