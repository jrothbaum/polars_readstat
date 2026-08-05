use polars_readstat_rs::{read_sas7bcat, CatalogKey};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

/// Dump a `.sas7bcat` catalog to JSON for cross-checking against pyreadstat.
/// Usage: sas_catalog_dump <input.sas7bcat> <output.json>
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: sas_catalog_dump <input.sas7bcat> <output.json>");
        std::process::exit(1);
    }
    let input = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);

    let catalog = read_sas7bcat(&input).expect("failed to read catalog");

    let mut out = Map::with_capacity(catalog.len());
    for (name, labels) in catalog.iter() {
        let entries: Vec<Value> = labels
            .iter()
            .map(|(key, label)| match key {
                CatalogKey::Numeric(n) => {
                    json!({"key_type": "numeric", "key": n, "label": label})
                }
                CatalogKey::Text(s) => {
                    json!({"key_type": "text", "key": s, "label": label})
                }
                CatalogKey::Missing => {
                    json!({"key_type": "missing", "key": null, "label": label})
                }
            })
            .collect();
        out.insert(name.clone(), Value::Array(entries));
    }

    let json_str = serde_json::to_string(&Value::Object(out)).expect("serialize catalog json");
    std::fs::write(&output, json_str).expect("failed to write output file");
}
