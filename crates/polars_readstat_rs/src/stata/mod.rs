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
    StataWriteColumn, StataWriteSchema, StataWriter, ValueLabelMap, ValueLabels, VariableFormats,
    VariableLabels,
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

/// Build Stata metadata JSON from an already-parsed Metadata + Header (avoids re-reading the file).
pub fn metadata_json_from_meta(meta: &Metadata, hdr: &Header) -> Result<String> {
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

/// Export Stata metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = StataReader::open(path)?;
    metadata_json_from_meta(reader.metadata(), reader.header())
}

/// Configure a StataWriter with metadata from a parsed Stata file, filtered to the given columns.
pub fn configure_writer_from_metadata(
    mut writer: StataWriter,
    meta: &Metadata,
    col_names: &std::collections::HashSet<String>,
) -> StataWriter {
    use std::collections::BTreeMap;

    // Build label_name → BTreeMap<i32, String> from meta.value_labels
    let mut label_by_name: HashMap<String, BTreeMap<i32, String>> = HashMap::new();
    for vl in &meta.value_labels {
        let mut map: BTreeMap<i32, String> = BTreeMap::new();
        for (key, value) in vl.mapping.iter() {
            let key_i32 = match key {
                types::ValueLabelKey::Integer(v) => Some(*v),
                types::ValueLabelKey::Double(v) => {
                    let iv = *v as i32;
                    if (iv as f64) == *v && *v >= i32::MIN as f64 && *v <= i32::MAX as f64 {
                        Some(iv)
                    } else {
                        None
                    }
                }
                types::ValueLabelKey::Str(_) => None,
            };
            if let Some(k) = key_i32 {
                map.insert(k, value.clone());
            }
        }
        label_by_name.insert(vl.name.clone(), map);
    }

    let mut value_labels: writer::ValueLabels = HashMap::new();
    let mut variable_labels: writer::VariableLabels = HashMap::new();
    let mut variable_formats: writer::VariableFormats = HashMap::new();

    for v in &meta.variables {
        if !col_names.contains(&v.name) {
            continue;
        }
        if let Some(label_name) = &v.value_label_name {
            if let Some(map) = label_by_name.get(label_name) {
                if !map.is_empty() {
                    value_labels.insert(v.name.clone(), map.clone());
                }
            }
        }
        if let Some(label) = &v.label {
            variable_labels.insert(v.name.clone(), label.clone());
        }
        if let Some(fmt) = &v.format {
            variable_formats.insert(v.name.clone(), fmt.clone());
        }
    }

    if !value_labels.is_empty() {
        writer = writer.with_value_labels(value_labels);
    }
    if !variable_labels.is_empty() {
        writer = writer.with_variable_labels(variable_labels);
    }
    if !variable_formats.is_empty() {
        writer = writer.with_variable_formats(variable_formats);
    }
    writer
}

/// Build a Polars DataFrame from Stata metadata.
///
/// Schema: name, label, value_label_codes (List[str]), value_label_labels (List[str]),
/// format (str), format_type/format_width/format_decimals/measure/display_width/alignment (null).
pub fn build_metadata_df(meta: &Metadata) -> polars::prelude::PolarsResult<polars::prelude::DataFrame> {
    use polars::prelude::*;
    use types::ValueLabelKey;

    let n = meta.variables.len();

    let mut vl_map: std::collections::HashMap<&str, (Vec<String>, Vec<String>)> =
        std::collections::HashMap::with_capacity(meta.value_labels.len());
    for vl in &meta.value_labels {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut codes: Vec<String> = Vec::new();
        let mut labels: Vec<String> = Vec::new();
        for (k, lbl) in vl.mapping.iter() {
            let code = match k {
                ValueLabelKey::Integer(v) => v.to_string(),
                ValueLabelKey::Double(v) => v.to_string(),
                ValueLabelKey::Str(s) => s.clone(),
            };
            if seen.insert(code.clone()) {
                codes.push(code);
                labels.push(lbl.clone());
            }
        }
        vl_map.insert(vl.name.as_str(), (codes, labels));
    }

    let mut names: Vec<&str> = Vec::with_capacity(n);
    let mut var_labels: Vec<Option<&str>> = Vec::with_capacity(n);
    let mut vl_codes: Vec<Option<Vec<String>>> = Vec::with_capacity(n);
    let mut vl_lbls: Vec<Option<Vec<String>>> = Vec::with_capacity(n);
    let mut formats: Vec<Option<&str>> = Vec::with_capacity(n);

    for (i, var) in meta.variables.iter().enumerate() {
        names.push(var.name.as_str());
        let lbl = meta.variable_labels.get(i).filter(|s| !s.is_empty()).map(|s| s.as_str());
        var_labels.push(lbl);

        if let Some(vl_name) = var.value_label_name.as_ref() {
            if let Some((codes, lbls)) = vl_map.get(vl_name.as_str()) {
                vl_codes.push(Some(codes.clone()));
                vl_lbls.push(Some(lbls.clone()));
            } else {
                vl_codes.push(None);
                vl_lbls.push(None);
            }
        } else {
            vl_codes.push(None);
            vl_lbls.push(None);
        }

        formats.push(meta.formats.get(i).filter(|s| !s.is_empty()).map(|s| s.as_str()));
    }

    let list_str_dtype = DataType::List(Box::new(DataType::String));
    let make_list = |col_name: &str, vecs: Vec<Option<Vec<String>>>| -> PolarsResult<Series> {
        let any_vals: Vec<AnyValue> = vecs.into_iter().map(|opt| match opt {
            None => AnyValue::Null,
            Some(v) => AnyValue::List(Series::new(PlSmallStr::EMPTY, v)),
        }).collect();
        Series::from_any_values_and_dtype(col_name.into(), &any_vals, &list_str_dtype, true)
    };

    let null_i32: Vec<Option<i32>> = vec![None; n];
    let null_str: Vec<Option<&str>> = vec![None; n];

    DataFrame::new_infer_height(vec![
        Series::new("name".into(), names).into_column(),
        Series::new("label".into(), var_labels).into_column(),
        make_list("value_label_codes", vl_codes)?.into_column(),
        make_list("value_label_labels", vl_lbls)?.into_column(),
        Series::new("format".into(), formats).into_column(),
        Series::new("format_type".into(), null_i32.clone()).into_column(),
        Series::new("format_width".into(), null_i32.clone()).into_column(),
        Series::new("format_decimals".into(), null_i32.clone()).into_column(),
        Series::new("measure".into(), null_str.clone()).into_column(),
        Series::new("display_width".into(), null_i32).into_column(),
        Series::new("alignment".into(), null_str).into_column(),
    ])
}
