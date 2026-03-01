use polars_readstat_rs::spss::metadata_json;
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const HEADER_LEN: usize = 176;
const REC_TYPE_VARIABLE: u32 = 2;
const REC_TYPE_HAS_DATA: u32 = 7;
const REC_TYPE_DICT_TERMINATION: u32 = 999;
const SUBTYPE_LONG_STRING_VALUE_LABELS: u32 = 21;

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

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}


fn write_name(buf: &mut Vec<u8>, name: &str) {
    let mut out = [b' '; 8];
    let bytes = name.as_bytes();
    let copy_len = bytes.len().min(8);
    out[..copy_len].copy_from_slice(&bytes[..copy_len]);
    buf.extend_from_slice(&out);
}

fn write_header(buf: &mut Vec<u8>, row_count: i32, nominal_case_size: i32) {
    let mut header = vec![0u8; HEADER_LEN];
    header[0..4].copy_from_slice(b"$FL2");
    header[64..68].copy_from_slice(&2i32.to_le_bytes());
    header[68..72].copy_from_slice(&nominal_case_size.to_le_bytes());
    header[72..76].copy_from_slice(&0i32.to_le_bytes());
    header[80..84].copy_from_slice(&row_count.to_le_bytes());
    header[84..92].copy_from_slice(&100.0f64.to_le_bytes());
    buf.extend_from_slice(&header);
}

fn build_long_string_value_labels(
    var_name: &str,
    string_len: usize,
    value: &str,
    label: &str,
) -> Vec<u8> {
    let mut data = Vec::new();
    write_u32(&mut data, var_name.len() as u32);
    data.extend_from_slice(var_name.as_bytes());
    write_u32(&mut data, string_len as u32);
    write_u32(&mut data, 1);
    write_u32(&mut data, value.len() as u32);
    data.extend_from_slice(value.as_bytes());
    write_u32(&mut data, label.len() as u32);
    data.extend_from_slice(label.as_bytes());
    data
}

fn write_minimal_sav_with_long_string_labels(path: &Path) -> std::io::Result<()> {
    let var_name = "STRVAR";
    let label = "L".repeat(300);
    let string_len = 8;
    let data = build_long_string_value_labels(var_name, string_len, "A", &label);

    let mut buf = Vec::new();
    write_header(&mut buf, 0, 1);
    write_u32(&mut buf, REC_TYPE_VARIABLE);
    write_i32(&mut buf, string_len as i32);
    write_i32(&mut buf, 0);
    write_i32(&mut buf, 0);
    write_i32(&mut buf, 0);
    write_i32(&mut buf, 0);
    write_name(&mut buf, var_name);

    write_u32(&mut buf, REC_TYPE_HAS_DATA);
    write_u32(&mut buf, SUBTYPE_LONG_STRING_VALUE_LABELS);
    write_u32(&mut buf, 1);
    write_u32(&mut buf, data.len() as u32);
    buf.extend_from_slice(&data);

    write_u32(&mut buf, REC_TYPE_DICT_TERMINATION);
    write_u32(&mut buf, 0);
    fs::write(path, buf)
}

#[test]
fn test_spss_long_string_value_labels_record_parses() {
    let path = temp_path("spss_long_string_labels", "sav");
    write_minimal_sav_with_long_string_labels(&path).unwrap();

    let json = metadata_json(&path).expect("metadata");
    let meta: Value = serde_json::from_str(&json).expect("json");
    let vars = meta
        .get("variables")
        .and_then(|v| v.as_array())
        .expect("variables array");
    let var = vars
        .iter()
        .find(|v| v.get("name").and_then(|n| n.as_str()) == Some("STRVAR"))
        .expect("STRVAR variable");
    assert_eq!(
        var.get("value_label").and_then(|v| v.as_str()),
        Some("labels0")
    );
    let labels = var
        .get("value_labels")
        .and_then(|v| v.as_object())
        .expect("value_labels map");
    let expected_label = "L".repeat(300);
    assert_eq!(
        labels.get("A").and_then(|v| v.as_str()),
        Some(expected_label.as_str())
    );

    let _ = fs::remove_file(&path);
}

#[test]
fn test_spss_long_string_value_labels_pyreadstat_regression() {
    let path = temp_path("spss_long_string_labels_pyreadstat", "sav");
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("spss")
        .join("generate_long_string_value_labels.py");

    let output = Command::new("uv")
        .arg("run")
        .arg(script.as_os_str())
        .arg("--out")
        .arg(&path)
        .output()
        .expect("uv run");
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "uv run failed (status {}):\nstdout:\n{}\nstderr:\n{}",
            output.status, stdout, stderr
        );
    }

    let json = metadata_json(&path).expect("metadata");
    let meta: Value = serde_json::from_str(&json).expect("json");
    let vars = meta
        .get("variables")
        .and_then(|v| v.as_array())
        .expect("variables array");
    let var = vars
        .iter()
        .find(|v| {
            v.get("name")
                .and_then(|n| n.as_str())
                .map(|n| n.eq_ignore_ascii_case("longstr"))
                .unwrap_or(false)
        })
        .expect("longstr variable");
    assert!(
        var.get("value_label").and_then(|v| v.as_str()).is_some(),
        "expected long string value labels"
    );
    let labels = var
        .get("value_labels")
        .and_then(|v| v.as_object())
        .expect("value_labels map");
    assert!(!labels.is_empty(), "expected value_labels entries");

    let reader = polars_readstat_rs::SpssReader::open(&path).expect("open");
    let df = reader
        .read()
        .value_labels_as_strings(true)
        .finish()
        .expect("read");
    assert!(df.height() > 0);

    let _ = fs::remove_file(&path);
}
