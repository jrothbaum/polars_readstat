pub mod arrow_output;
pub(crate) mod data;
pub(crate) mod error;
pub(crate) mod header;
pub(crate) mod metadata;
pub mod polars_output;
pub mod por;
pub(crate) mod reader;
pub(crate) mod types;
pub mod writer;

pub use error::{Error, Result};
pub use polars_output::scan_sav;
pub use por::{metadata_json_por, metadata_por, read_por, scan_por, write_por, PorMetadata, PorVariable, PorWriteOptions};
pub use reader::SpssReader;
pub use types::{Alignment, Endian, Header, Measure, Metadata, VarType};
pub use writer::{
    SpssValueLabelKey, SpssValueLabelMap, SpssValueLabels, SpssVariableAlignments,
    SpssVariableDisplayWidths, SpssVariableFormat, SpssVariableFormats, SpssVariableLabels,
    SpssVariableMeasures, SpssWriteColumn, SpssWriteSchema, SpssWriter,
};

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::Path;

/// Build SPSS metadata JSON from an already-parsed Metadata + Header (avoids re-reading the file).
pub fn metadata_json_from_meta(meta: &Metadata, hdr: &Header) -> Result<String> {
    let compression_str = match hdr.compression {
        0 => "None",
        2 => "ZLIB",
        _ => "RLE",
    };
    let mut value_labels_by_name: HashMap<String, Value> = HashMap::new();
    for label in &meta.value_labels {
        let mut mapping = Map::new();
        for (key, value) in &label.mapping {
            let key_str = match key {
                types::ValueLabelKey::Double(v) => v.to_string(),
                types::ValueLabelKey::Str(s) => s.clone(),
            };
            mapping.insert(key_str, json!(value));
        }
        value_labels_by_name.insert(label.name.clone(), Value::Object(mapping));
    }

    let df = &meta.metadata_df;

    let variables = meta
        .variables
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), json!(v.name));
            obj.insert("type".to_string(), json!(format!("{:?}", v.var_type)));
            obj.insert("string_len".to_string(), json!(v.string_len));
            obj.insert("storage_width_segments".to_string(), json!(v.width));
            obj.insert("storage_width_bytes".to_string(), json!(v.width * 8));
            let format_width = df_col_i32(df, "format_width", i);
            let display_width = df_col_i32(df, "display_width", i);
            obj.insert(
                "width".to_string(),
                json!(display_width.or(format_width).unwrap_or(0)),
            );
            obj.insert("display_width".to_string(), json!(display_width));
            obj.insert("alignment".to_string(), json!(df_col_str(df, "alignment", i)));
            obj.insert("measure".to_string(), json!(df_col_str(df, "measure", i)));
            obj.insert("format_type".to_string(), json!(df_col_i32(df, "format_type", i)));
            obj.insert("format_width".to_string(), json!(format_width));
            obj.insert("decimal_places".to_string(), json!(df_col_i32(df, "format_decimals", i)));
            obj.insert("format_decimals".to_string(), json!(df_col_i32(df, "format_decimals", i)));
            obj.insert("write_format_type".to_string(), json!(v.write_format_type));
            obj.insert("write_format_width".to_string(), json!(v.write_format_width));
            obj.insert("write_format_decimals".to_string(), json!(v.write_format_decimals));
            obj.insert(
                "format_class".to_string(),
                json!(v.format_class.map(|c| format!("{:?}", c))),
            );
            obj.insert("label".to_string(), json!(df_col_str(df, "label", i)));
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

/// Export SPSS metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = SpssReader::open(path)?;
    metadata_json_from_meta(reader.metadata(), reader.header())
}

fn df_col_str<'a>(df: &'a polars::prelude::DataFrame, col: &str, row: usize) -> Option<&'a str> {
    df.column(col)
        .ok()?
        .str()
        .ok()?
        .get(row)
}

fn df_col_i32(df: &polars::prelude::DataFrame, col: &str, row: usize) -> Option<i32> {
    df.column(col).ok()?.i32().ok()?.get(row)
}
