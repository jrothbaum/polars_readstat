pub mod arrow_output;
pub(crate) mod data;
pub(crate) mod error;
pub(crate) mod header;
pub(crate) mod metadata;
pub mod polars_output;
pub(crate) mod reader;
pub(crate) mod types;
pub mod writer;

pub use error::{Error, Result};
pub use polars_output::scan_sav;
pub use reader::SpssReader;
pub use types::{Endian, Header, Metadata, VarType};
pub use writer::{
    SpssValueLabelKey, SpssValueLabelMap, SpssValueLabels, SpssVariableLabels, SpssWriteColumn,
    SpssWriteSchema, SpssWriter,
};

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::Path;

/// Export SPSS metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = SpssReader::open(path)?;
    let meta = reader.metadata();
    let hdr = reader.header();
    let compression_str = match hdr.compression {
        0 => "None",
        2 => "ZLIB",
        _ => "RLE", // 1 = bytecode/RLE
    };
    let mut value_labels_by_name: HashMap<String, Value> = HashMap::new();
    for label in &meta.value_labels {
        let mut mapping = Map::new();
        for (key, value) in &label.mapping {
            let key_str = match key {
                crate::spss::types::ValueLabelKey::Double(v) => v.to_string(),
                crate::spss::types::ValueLabelKey::Str(s) => s.clone(),
            };
            mapping.insert(key_str, json!(value));
        }
        value_labels_by_name.insert(label.name.clone(), Value::Object(mapping));
    }

    let variables = meta
        .variables
        .iter()
        .map(|v| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), json!(v.name));
            obj.insert("type".to_string(), json!(format!("{:?}", v.var_type)));
            obj.insert("string_len".to_string(), json!(v.string_len));
            obj.insert("format_type".to_string(), json!(v.format_type));
            obj.insert(
                "format_class".to_string(),
                json!(v.format_class.map(|c| format!("{:?}", c))),
            );
            obj.insert("label".to_string(), json!(v.label));
            obj.insert("value_label".to_string(), json!(v.value_label));
            if let Some(label_name) = v.value_label.as_ref() {
                if let Some(labels) = value_labels_by_name.get(label_name) {
                    obj.insert("value_labels".to_string(), labels.clone());
                }
            }
            obj.insert("missing_range".to_string(), json!(v.missing_range));
            obj.insert("missing_doubles".to_string(), json!(v.missing_doubles));
            obj.insert("missing_strings".to_string(), json!(v.missing_strings));
            Value::Object(obj)
        })
        .collect::<Vec<_>>();
    let v = json!({
        "row_count": meta.row_count,
        "file_label": hdr.data_label,
        "compression": compression_str,
        "version": hdr.version,
        "data_offset": meta.data_offset,
        "encoding": meta.encoding.name(),
        "variables": variables,
    });
    Ok(v.to_string())
}
