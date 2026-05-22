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
pub use por::{metadata_json_por, read_por, scan_por, write_por, PorMetadata, PorVariable, PorWriteOptions};
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

    let variables = meta
        .variables
        .iter()
        .map(|v| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), json!(v.name));
            obj.insert("type".to_string(), json!(format!("{:?}", v.var_type)));
            obj.insert("string_len".to_string(), json!(v.string_len));
            obj.insert("storage_width_segments".to_string(), json!(v.width));
            obj.insert("storage_width_bytes".to_string(), json!(v.width * 8));
            obj.insert(
                "width".to_string(),
                json!(v.display_width.unwrap_or(v.format_width as i32)),
            );
            obj.insert("display_width".to_string(), json!(v.display_width));
            obj.insert(
                "alignment".to_string(),
                json!(v.alignment.map(|a| format!("{:?}", a))),
            );
            obj.insert(
                "measure".to_string(),
                json!(v.measure.map(|m| format!("{:?}", m))),
            );
            obj.insert("format_type".to_string(), json!(v.format_type));
            obj.insert("format_width".to_string(), json!(v.format_width));
            obj.insert("decimal_places".to_string(), json!(v.format_decimals));
            obj.insert("format_decimals".to_string(), json!(v.format_decimals));
            obj.insert("write_format_type".to_string(), json!(v.write_format_type));
            obj.insert("write_format_width".to_string(), json!(v.write_format_width));
            obj.insert("write_format_decimals".to_string(), json!(v.write_format_decimals));
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

/// Export SPSS metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = SpssReader::open(path)?;
    metadata_json_from_meta(reader.metadata(), reader.header())
}

/// Configure a SpssWriter with metadata from a parsed SPSS file, filtered to the given columns.
pub fn configure_writer_from_metadata(
    mut writer: SpssWriter,
    meta: &Metadata,
    col_names: &std::collections::HashSet<String>,
) -> SpssWriter {
    // Build label_name → SpssValueLabelMap from meta.value_labels
    let mut label_by_name: HashMap<String, SpssValueLabelMap> = HashMap::new();
    for vl in &meta.value_labels {
        let mut map: SpssValueLabelMap = HashMap::new();
        for (key, value) in &vl.mapping {
            if let types::ValueLabelKey::Double(v) = key {
                map.insert(SpssValueLabelKey::from_f64(*v), value.clone());
            }
        }
        label_by_name.insert(vl.name.clone(), map);
    }

    let mut value_labels: SpssValueLabels = HashMap::new();
    let mut variable_labels: SpssVariableLabels = HashMap::new();
    let mut variable_measures: SpssVariableMeasures = HashMap::new();
    let mut variable_alignments: SpssVariableAlignments = HashMap::new();
    let mut variable_display_widths: SpssVariableDisplayWidths = HashMap::new();
    let mut variable_formats: SpssVariableFormats = HashMap::new();

    for v in &meta.variables {
        if !col_names.contains(&v.name) {
            continue;
        }
        if let Some(label_name) = &v.value_label {
            if let Some(map) = label_by_name.get(label_name) {
                if !map.is_empty() {
                    value_labels.insert(v.name.clone(), map.clone());
                }
            }
        }
        if let Some(label) = &v.label {
            variable_labels.insert(v.name.clone(), label.clone());
        }
        if let Some(measure) = v.measure {
            variable_measures.insert(v.name.clone(), measure);
        }
        if let Some(alignment) = v.alignment {
            variable_alignments.insert(v.name.clone(), alignment);
        }
        if let Some(dw) = v.display_width {
            variable_display_widths.insert(v.name.clone(), dw);
        }
        variable_formats.insert(
            v.name.clone(),
            SpssVariableFormat {
                format_type: Some(v.format_type),
                width: Some(v.format_width),
                decimals: Some(v.format_decimals),
            },
        );
    }

    if !value_labels.is_empty() {
        writer = writer.with_value_labels(value_labels);
    }
    if !variable_labels.is_empty() {
        writer = writer.with_variable_labels(variable_labels);
    }
    if !variable_measures.is_empty() {
        writer = writer.with_variable_measures(variable_measures);
    }
    if !variable_alignments.is_empty() {
        writer = writer.with_variable_alignments(variable_alignments);
    }
    if !variable_display_widths.is_empty() {
        writer = writer.with_variable_display_widths(variable_display_widths);
    }
    if !variable_formats.is_empty() {
        writer = writer.with_variable_formats(variable_formats);
    }
    writer
}
