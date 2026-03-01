use std::path::{Path, PathBuf};

pub fn collect_files(dir: &Path, exts: &[&str]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_files_inner(dir, exts, &mut out);
    out
}

fn collect_files_inner(dir: &Path, exts: &[&str], out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|s| s.to_str()) == Some("too_big") {
                    continue;
                }
                collect_files_inner(&path, exts, out);
                continue;
            }
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            if exts.iter().any(|e| e.eq_ignore_ascii_case(&ext)) {
                out.push(path);
            }
        }
    }
}

#[allow(dead_code)]
pub fn sas_files() -> Vec<PathBuf> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("sas")
        .join("data");
    collect_files(&base, &["sas7bdat"])
}

#[allow(dead_code)]
pub fn stata_files() -> Vec<PathBuf> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("stata")
        .join("data");
    collect_files(&base, &["dta"])
}

pub fn spss_files() -> Vec<PathBuf> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("spss")
        .join("data");
    collect_files(&base, &["sav", "zsav"])
}
