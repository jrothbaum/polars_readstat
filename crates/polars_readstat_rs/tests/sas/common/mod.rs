use std::path::PathBuf;
use glob::glob;

/// Get the absolute path to a test data file
pub fn test_data_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("sas")
        .join("data")
        .join(filename)
}

/// Get all SAS7BDAT test files
pub fn all_sas_files() -> Vec<PathBuf> {
    let pattern = format!(
        "{}/tests/sas/data/**/*.sas7bdat",
        env!("CARGO_MANIFEST_DIR")
    );

    glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

/// Get test files by category
#[allow(dead_code)]
pub fn files_by_pattern(pattern: &str) -> Vec<PathBuf> {
    let search_pattern = format!(
        "{}/tests/sas/data/**/*{}*.sas7bdat",
        env!("CARGO_MANIFEST_DIR"),
        pattern
    );

    glob(&search_pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

/// Count total files and print summary
#[allow(dead_code)]
pub fn test_file_summary() {
    let files = all_sas_files();
    println!("Found {} SAS7BDAT test files", files.len());

    // Count by type
    let compressed = files.iter().filter(|p| {
        p.to_string_lossy().contains("compressed") ||
        p.to_string_lossy().contains("rdc")
    }).count();

    let numeric = files.iter().filter(|p| {
        p.to_string_lossy().contains("numeric")
    }).count();

    println!("  - Compressed: {}", compressed);
    println!("  - Numeric: {}", numeric);
    println!("  - Other: {}", files.len() - compressed - numeric);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_path_works() {
        let path = test_data_path("test1.sas7bdat");
        assert!(path.to_string_lossy().ends_with("tests/sas/data/test1.sas7bdat"));
    }

    #[test]
    fn test_finds_sas_files() {
        let files = all_sas_files();
        assert!(files.len() > 0, "Should find at least one test file");
    }
}
