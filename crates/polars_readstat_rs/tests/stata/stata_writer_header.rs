use polars::df;
use polars_readstat_rs::StataWriter;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dta_path() -> PathBuf {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("polars_readstat_rs_header_{}_{}.dta", pid, nanos))
}

fn find_subslice(buf: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > buf.len() {
        return None;
    }
    buf.windows(needle.len()).position(|w| w == needle)
}

fn find_subslice_from(buf: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    find_subslice(&buf[start..], needle).map(|i| i + start)
}

#[test]
fn test_writer_defaults_to_release_118_for_small_dataset() {
    let df = df!("x" => [1i32]).unwrap();
    let out_path = temp_dta_path();

    StataWriter::new(&out_path).write_df(&df).unwrap();
    let bytes = std::fs::read(&out_path).unwrap();
    let _ = std::fs::remove_file(&out_path);

    assert!(
        find_subslice(&bytes, b"<release>118</release>").is_some(),
        "expected release 118 header"
    );

    let k_open = find_subslice(&bytes, b"<K>").expect("missing <K> tag");
    let k_start = k_open + "<K>".len();
    let k_end = find_subslice_from(&bytes, b"</K>", k_start).expect("missing </K> tag");
    assert_eq!(k_end - k_start, 2, "expected 2-byte K field for release 118");
}
