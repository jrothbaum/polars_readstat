pub(crate) mod compress;
pub(crate) mod data;
pub(crate) mod encoding;
pub(crate) mod error;
pub(crate) mod header;
pub(crate) mod metadata;
pub(crate) mod types;
pub(crate) mod value;

pub mod polars_output;

pub mod arrow_output;
pub mod reader;
pub mod writer;

pub use compress::{compress_df, CompressOptions};
pub use error::{Error, Result};
pub use polars_output::scan_dta;
pub use reader::StataReader;
pub use types::{Endian, Header, Metadata, NumericType, VarType};
pub use writer::{
    pandas_make_stata_column_names, pandas_prepare_df_for_stata, pandas_rename_df,
    StataWriteColumn, StataWriteSchema, StataWriter, ValueLabelMap, ValueLabels, VariableLabels,
};

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::Path;

fn missing_value_label(v: i32, rules: crate::stata::value::MissingRules) -> Option<String> {
    if !rules.system_missing_enabled || v < rules.system_missing_int32 {
        return None;
    }
    let offset = (v - rules.system_missing_int32) as u8;
    if offset == 0 {
        return Some("MISSING".to_string());
    }
    if offset <= 26 {
        let letter = (b'a' + offset - 1) as char;
        return Some(format!("MISSING_{}", letter));
    }
    None
}

fn value_label_key_to_string(
    key: &crate::stata::types::ValueLabelKey,
    rules: crate::stata::value::MissingRules,
) -> String {
    match key {
        crate::stata::types::ValueLabelKey::Integer(v) => {
            missing_value_label(*v, rules).unwrap_or_else(|| v.to_string())
        }
        crate::stata::types::ValueLabelKey::Double(v) => {
            if v.is_finite() {
                let iv = *v as i32;
                if (iv as f64) == *v {
                    if let Some(missing) = missing_value_label(iv, rules) {
                        return missing;
                    }
                }
            }
            v.to_string()
        }
        crate::stata::types::ValueLabelKey::Str(s) => s.clone(),
    }
}

/// Export Stata metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = StataReader::open(path)?;
    let meta = reader.metadata();
    let hdr = reader.header();
    let missing_rules = crate::stata::value::missing_rules(hdr.version);
    let mut value_labels_by_name: HashMap<String, Value> = HashMap::new();
    for label in &meta.value_labels {
        let mut mapping = Map::new();
        for (key, value) in label.mapping.iter() {
            let key_str = value_label_key_to_string(key, missing_rules);
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
            obj.insert("format".to_string(), json!(v.format));
            obj.insert("label".to_string(), json!(v.label));
            obj.insert("value_label_name".to_string(), json!(v.value_label_name));
            if let Some(label_name) = v.value_label_name.as_ref() {
                if let Some(labels) = value_labels_by_name.get(label_name) {
                    obj.insert("value_labels".to_string(), labels.clone());
                }
            }
            Value::Object(obj)
        })
        .collect::<Vec<_>>();
    let v = json!({
        "version": hdr.version,
        "byte_order": format!("{:?}", meta.byte_order),
        "row_count": meta.row_count,
        "data_label": meta.data_label,
        "timestamp": meta.timestamp,
        "data_offset": meta.data_offset,
        "strls_offset": meta.strls_offset,
        "value_labels_offset": meta.value_labels_offset,
        "encoding": meta.encoding.name(),
        "variables": variables,
    });
    Ok(v.to_string())
}
