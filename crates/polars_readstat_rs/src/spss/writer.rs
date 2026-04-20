use crate::spss::error::{Error, Result};
use crate::spss::types::VarType;
use polars::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const SAV_HEADER_LEN: usize = 176;
const SAV_RECORD_VARIABLE: u32 = 2;
const SAV_RECORD_VALUE_LABEL: u32 = 3;
const SAV_RECORD_VALUE_LABEL_VARS: u32 = 4;
const SAV_RECORD_HAS_DATA: u32 = 7;
const SAV_RECORD_DICT_TERMINATION: u32 = 999;
const SUBTYPE_INTEGER_INFO: u32 = 3;
const SUBTYPE_FP_INFO: u32 = 4;
const SUBTYPE_VAR_DISPLAY: u32 = 11;
const SUBTYPE_LONG_VAR_NAME: u32 = 13;
const SUBTYPE_VERY_LONG_STR: u32 = 14;
const SUBTYPE_NUMBER_OF_CASES: u32 = 16;
const SUBTYPE_CHAR_ENCODING: u32 = 20;
const SPSS_SEC_SHIFT: i64 = 12_219_379_200;

const SPSS_FORMAT_A: u8 = 1;
const SPSS_FORMAT_F: u8 = 5;
const SAV_MISSING_DOUBLE: u64 = 0xFFEFFFFFFFFFFFFF;
const SAV_HIGHEST_DOUBLE: u64 = 0x7FEFFFFFFFFFFFFF;
const SAV_LOWEST_DOUBLE: u64 = 0xFFEFFFFFFFFFFFFE;
const SAV_FLOATING_POINT_REP_IEEE: i32 = 1;
const SAV_ENDIANNESS_BIG: i32 = 1;
const SAV_ENDIANNESS_LITTLE: i32 = 2;
const SAV_CONTINUATION_FORMAT: i32 = 0x011d01;

const SAV_MEASURE_NOMINAL: i32 = 1;
const SAV_MEASURE_SCALE: i32 = 3;
const SAV_ALIGNMENT_LEFT: i32 = 0;
const SAV_ALIGNMENT_RIGHT: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpssValueLabelKey(u64);

impl SpssValueLabelKey {
    pub fn from_f64(v: f64) -> Self {
        Self(v.to_bits())
    }

    pub fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl From<f64> for SpssValueLabelKey {
    fn from(value: f64) -> Self {
        Self::from_f64(value)
    }
}

pub type SpssValueLabelMap = HashMap<SpssValueLabelKey, String>;
pub type SpssValueLabels = HashMap<String, SpssValueLabelMap>;
pub type SpssVariableLabels = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct SpssWriteSchema {
    pub columns: Vec<SpssWriteColumn>,
    pub row_count: Option<usize>,
    pub value_labels: Option<SpssValueLabels>,
    pub variable_labels: Option<SpssVariableLabels>,
}

#[derive(Debug, Clone)]
pub struct SpssWriteColumn {
    pub name: String,
    pub dtype: DataType,
    pub string_width_bytes: Option<usize>,
}

pub struct SpssWriter {
    path: PathBuf,
    schema: Option<SpssWriteSchema>,
    value_labels: Option<SpssValueLabels>,
    variable_labels: Option<SpssVariableLabels>,
}

impl SpssWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            schema: None,
            value_labels: None,
            variable_labels: None,
        }
    }

    pub fn with_schema(mut self, schema: SpssWriteSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_value_labels(mut self, labels: SpssValueLabels) -> Self {
        self.value_labels = Some(labels);
        self
    }

    pub fn with_variable_labels(mut self, labels: SpssVariableLabels) -> Self {
        self.variable_labels = Some(labels);
        self
    }

    pub fn write_df(&self, df: &DataFrame) -> Result<()> {
        let schema = self.schema.as_ref();
        let value_labels = merge_value_labels(
            schema.and_then(|s| s.value_labels.clone()),
            self.value_labels.clone(),
        );
        let variable_labels = merge_variable_labels(
            schema.and_then(|s| s.variable_labels.clone()),
            self.variable_labels.clone(),
        );
        let columns = infer_columns(df, schema, variable_labels.as_ref())?;
        let encoding = choose_encoding(
            df,
            &columns,
            value_labels.as_ref(),
            variable_labels.as_ref(),
        )?;
        let file = File::create(&self.path)?;
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        write_header(
            &mut writer,
            df.height() as i32,
            columns.iter().map(|c| c.width).sum(),
        )?;
        write_variable_records(&mut writer, &columns, encoding)?;
        if let Some(labels) = value_labels.as_ref() {
            write_value_labels(&mut writer, &columns, labels, encoding)?;
        }
        write_integer_info_record(&mut writer, encoding)?;
        write_floating_point_info_record(&mut writer)?;
        write_variable_display_record(&mut writer, &columns)?;
        write_long_var_names_record(&mut writer, &columns, encoding)?;
        write_very_long_string_record(&mut writer, &columns)?;
        write_encoding_record(&mut writer, encoding)?;
        write_number_of_cases_record(&mut writer, df.height() as u64)?;
        write_dict_termination(&mut writer)?;
        write_data(&mut writer, df, &columns, encoding)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    name: String,
    short_name: String,
    var_type: VarType,
    string_len: usize,
    width: usize,
    offset: usize,
    format_type: u8,
    format_width: u8,
    format_decimals: u8,
    label: Option<String>,
}

fn infer_columns(
    df: &DataFrame,
    schema: Option<&SpssWriteSchema>,
    variable_labels: Option<&SpssVariableLabels>,
) -> Result<Vec<ColumnSpec>> {
    if let Some(schema) = schema {
        let mut cols = Vec::with_capacity(schema.columns.len());
        let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let short_names = build_short_names(&names)?;
        let mut offset = 0usize;
        for (idx, col) in schema.columns.iter().enumerate() {
            validate_long_name(&col.name)?;
            let (var_type, string_len, width, format_type, format_width, format_decimals) =
                dtype_to_spss(col, df)?;
            let label = variable_labels.and_then(|labels| labels.get(&col.name).cloned());
            cols.push(ColumnSpec {
                name: col.name.clone(),
                short_name: short_names[idx].clone(),
                var_type,
                string_len,
                width,
                offset,
                format_type,
                format_width,
                format_decimals,
                label,
            });
            offset += width;
        }
        return Ok(cols);
    }

    let mut cols = Vec::with_capacity(df.width());
    let names: Vec<String> = df
        .columns()
        .iter()
        .map(|c| c.as_materialized_series().name().to_string())
        .collect();
    let short_names = build_short_names(&names)?;
    let mut offset = 0usize;
    for (idx, column) in df.columns().iter().enumerate() {
        let series = column.as_materialized_series();
        let name = series.name().to_string();
        validate_long_name(&name)?;
        let (var_type, string_len, width, format_type, format_width, format_decimals) =
            infer_series(series)?;
        let label = variable_labels.and_then(|labels| labels.get(&name).cloned());
        cols.push(ColumnSpec {
            name,
            short_name: short_names[idx].clone(),
            var_type,
            string_len,
            width,
            offset,
            format_type,
            format_width,
            format_decimals,
            label,
        });
        offset += width;
    }
    Ok(cols)
}

fn dtype_to_spss(
    col: &SpssWriteColumn,
    df: &DataFrame,
) -> Result<(VarType, usize, usize, u8, u8, u8)> {
    match col.dtype {
        DataType::String => {
            let width = if let Some(w) = col.string_width_bytes {
                w
            } else {
                let series = df
                    .column(&col.name)
                    .map_err(|e| Error::ParseError(e.to_string()))?
                    .as_materialized_series();
                max_string_width(series)?
            };
            let (var_type, string_len, width) = string_layout(width)?;
            Ok((
                var_type,
                string_len,
                width,
                SPSS_FORMAT_A,
                string_len.min(255) as u8,
                0,
            ))
        }
        DataType::Date => Ok((VarType::Numeric, 0, 1, 20, 11, 0)),
        DataType::Datetime(_, _) => Ok((VarType::Numeric, 0, 1, 22, 20, 0)),
        DataType::Time => Ok((VarType::Numeric, 0, 1, 21, 8, 0)),
        DataType::Float32 | DataType::Float64 => Ok((VarType::Numeric, 0, 1, SPSS_FORMAT_F, 8, 2)),
        _ => Ok((VarType::Numeric, 0, 1, SPSS_FORMAT_F, 8, 0)),
    }
}

fn infer_series(series: &Series) -> Result<(VarType, usize, usize, u8, u8, u8)> {
    match series.dtype() {
        DataType::String => {
            let width = max_string_width(series)?;
            let (var_type, string_len, width) = string_layout(width)?;
            Ok((
                var_type,
                string_len,
                width,
                SPSS_FORMAT_A,
                string_len.min(255) as u8,
                0,
            ))
        }
        DataType::Date => Ok((VarType::Numeric, 0, 1, 20, 11, 0)),
        DataType::Datetime(_, _) => Ok((VarType::Numeric, 0, 1, 22, 20, 0)),
        DataType::Time => Ok((VarType::Numeric, 0, 1, 21, 8, 0)),
        DataType::Float32 | DataType::Float64 => Ok((VarType::Numeric, 0, 1, SPSS_FORMAT_F, 8, 2)),
        _ => Ok((VarType::Numeric, 0, 1, SPSS_FORMAT_F, 8, 0)),
    }
}

fn string_layout(len: usize) -> Result<(VarType, usize, usize)> {
    let len = len.max(1);
    if len <= 255 {
        let width = len.div_ceil(8);
        return Ok((VarType::Str, len, width));
    }
    let n_segments = long_string_segment_count(len);
    let last_payload = len - (n_segments - 1) * 252;
    let last_storage = last_payload.div_ceil(8) * 8;
    let storage_bytes = (n_segments - 1) * 256 + last_storage;
    Ok((VarType::Str, len, storage_bytes / 8))
}

fn max_string_width(series: &Series) -> Result<usize> {
    let utf8 = series.str().map_err(|e| Error::ParseError(e.to_string()))?;
    let mut max_len = 0usize;
    for opt in utf8.into_iter() {
        if let Some(s) = opt {
            let s: &str = s;
            let len = s.as_bytes().len();
            if len > max_len {
                max_len = len;
            }
        }
    }
    Ok(max_len.max(1))
}

fn validate_long_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::ParseError(
            "invalid SPSS variable name: empty".to_string(),
        ));
    }
    if name.as_bytes().len() > 64 {
        return Err(Error::ParseError(format!(
            "invalid SPSS variable name (too long): {name}"
        )));
    }
    Ok(())
}

fn is_valid_short_name(name: &str) -> bool {
    if name.is_empty() || name.as_bytes().len() > 8 || !name.is_ascii() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn sanitize_short_base(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c.to_ascii_uppercase());
        }
    }
    if out.is_empty() || !out.chars().next().unwrap().is_ascii_alphabetic() {
        out.insert(0, 'V');
    }
    if out.len() > 8 {
        out.truncate(8);
    }
    out
}

fn make_unique_short_name(base: &str, used: &mut HashSet<String>) -> String {
    let mut candidate = base.to_string();
    if candidate.len() > 8 {
        candidate.truncate(8);
    }
    if !used.contains(&candidate) {
        used.insert(candidate.clone());
        return candidate;
    }
    let mut i = 1usize;
    loop {
        let suffix = i.to_string();
        let max_base = 8usize.saturating_sub(suffix.len());
        let mut b = base.to_string();
        if b.len() > max_base {
            b.truncate(max_base);
        }
        let cand = format!("{b}{suffix}");
        if !used.contains(&cand) {
            used.insert(cand.clone());
            return cand;
        }
        i += 1;
    }
}

fn build_short_names(names: &[String]) -> Result<Vec<String>> {
    let mut used: HashSet<String> = HashSet::new();
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let base = if is_valid_short_name(name) {
            name.to_ascii_uppercase()
        } else {
            sanitize_short_base(name)
        };
        let short = make_unique_short_name(&base, &mut used);
        out.push(short);
    }
    Ok(out)
}

fn sanitize_long_name_for_record(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '\t' => ' ',
            '=' => '_',
            _ => c,
        })
        .collect()
}

fn merge_value_labels(
    base: Option<SpssValueLabels>,
    extra: Option<SpssValueLabels>,
) -> Option<SpssValueLabels> {
    match (base, extra) {
        (None, None) => None,
        (Some(mut base), Some(extra)) => {
            for (k, v) in extra {
                base.entry(k).or_insert(v);
            }
            Some(base)
        }
        (Some(base), None) => Some(base),
        (None, Some(extra)) => Some(extra),
    }
}

fn merge_variable_labels(
    base: Option<SpssVariableLabels>,
    extra: Option<SpssVariableLabels>,
) -> Option<SpssVariableLabels> {
    match (base, extra) {
        (None, None) => None,
        (Some(mut base), Some(extra)) => {
            for (k, v) in extra {
                base.entry(k).or_insert(v);
            }
            Some(base)
        }
        (Some(base), None) => Some(base),
        (None, Some(extra)) => Some(extra),
    }
}

fn choose_encoding(
    df: &DataFrame,
    columns: &[ColumnSpec],
    value_labels: Option<&SpssValueLabels>,
    variable_labels: Option<&SpssVariableLabels>,
) -> Result<&'static encoding_rs::Encoding> {
    let enc_1252 = encoding_rs::WINDOWS_1252;
    let mut needs_utf8 = false;

    if let Some(labels) = variable_labels {
        for label in labels.values() {
            let (_, _, had_errors) = enc_1252.encode(label);
            if had_errors {
                needs_utf8 = true;
                break;
            }
        }
    }

    if !needs_utf8 {
        if let Some(vlabels) = value_labels {
            for map in vlabels.values() {
                for label in map.values() {
                    let (_, _, had_errors) = enc_1252.encode(label);
                    if had_errors {
                        needs_utf8 = true;
                        break;
                    }
                }
                if needs_utf8 {
                    break;
                }
            }
        }
    }

    if !needs_utf8 {
        for col in columns {
            let (_, _, had_errors) = enc_1252.encode(&col.name);
            if had_errors {
                needs_utf8 = true;
                break;
            }
            if col.var_type != VarType::Str {
                continue;
            }
            let series = df
                .column(&col.name)
                .map_err(|e| Error::ParseError(e.to_string()))?
                .as_materialized_series();
            let ca = series.str().map_err(|e| Error::ParseError(e.to_string()))?;
            for opt in ca.into_iter() {
                if let Some(s) = opt {
                    let (_, _, had_errors) = enc_1252.encode(s);
                    if had_errors {
                        needs_utf8 = true;
                        break;
                    }
                }
            }
            if needs_utf8 {
                break;
            }
        }
    }

    Ok(if needs_utf8 {
        encoding_rs::UTF_8
    } else {
        enc_1252
    })
}

fn write_header<W: Write>(writer: &mut W, row_count: i32, nominal_case_size: usize) -> Result<()> {
    let mut buf = vec![0u8; SAV_HEADER_LEN];
    buf[0..4].copy_from_slice(b"$FL2");
    buf[64..68].copy_from_slice(&2i32.to_le_bytes());
    buf[68..72].copy_from_slice(&(nominal_case_size as i32).to_le_bytes());
    buf[72..76].copy_from_slice(&0i32.to_le_bytes());
    buf[80..84].copy_from_slice(&row_count.to_le_bytes());
    buf[84..92].copy_from_slice(&100.0f64.to_le_bytes());
    writer.write_all(&buf)?;
    Ok(())
}

fn write_variable_records<W: Write>(
    writer: &mut W,
    columns: &[ColumnSpec],
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    for col in columns {
        if col.var_type == VarType::Str && col.string_len > 255 {
            write_very_long_variable_records(writer, col, encoding)?;
        } else {
            write_variable_record(writer, col, encoding)?;
            if col.width > 1 {
                for _ in 1..col.width {
                    write_variable_continuation(writer)?;
                }
            }
        }
    }
    Ok(())
}

fn write_very_long_variable_records<W: Write>(
    writer: &mut W,
    col: &ColumnSpec,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    let segments = long_string_segment_sizes(col.string_len);
    if segments.is_empty() {
        return Ok(());
    }

    // Base segment uses the column short name and can carry label metadata.
    write_very_long_segment_record(
        writer,
        &col.short_name,
        segments[0],
        col.label.as_ref(),
        col.format_type,
        255,
        col.format_decimals,
        encoding,
    )?;

    let stem = &col.short_name[..col.short_name.len().min(5)];
    for (seg_idx, seg_size) in segments.iter().enumerate().skip(1) {
        let ghost = make_long_string_ghost_name(stem, seg_idx)?;
        write_very_long_segment_record(
            writer,
            &ghost,
            *seg_size,
            None,
            col.format_type,
            (*seg_size).min(255) as u8,
            col.format_decimals,
            encoding,
        )?;
    }
    Ok(())
}

fn write_very_long_segment_record<W: Write>(
    writer: &mut W,
    name: &str,
    seg_size: usize,
    label: Option<&String>,
    format_type: u8,
    format_width: u8,
    format_decimals: u8,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    write_u32(writer, SAV_RECORD_VARIABLE)?;
    write_i32(writer, seg_size as i32)?;
    write_i32(writer, if label.is_some() { 1 } else { 0 })?;
    write_i32(writer, 0)?;
    let fmt = encode_format(format_type, format_width, format_decimals);
    write_i32(writer, fmt)?;
    write_i32(writer, fmt)?;
    write_name(writer, name)?;
    if let Some(lbl) = label {
        // Very long strings can still carry a variable label on the base segment.
        write_variable_label(writer, lbl, encoding)?;
    }
    let seg_width = seg_size.div_ceil(8);
    if seg_width > 1 {
        for _ in 1..seg_width {
            write_variable_continuation(writer)?;
        }
    }
    Ok(())
}

fn write_variable_record<W: Write>(
    writer: &mut W,
    col: &ColumnSpec,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    write_u32(writer, SAV_RECORD_VARIABLE)?;
    let typ = if col.var_type == VarType::Numeric {
        0
    } else {
        col.string_len as i32
    };
    write_i32(writer, typ)?;
    let has_label = if col.label.is_some() { 1 } else { 0 };
    write_i32(writer, has_label)?;
    write_i32(writer, 0)?;
    let fmt = encode_format(col.format_type, col.format_width, col.format_decimals);
    write_i32(writer, fmt)?;
    write_i32(writer, fmt)?;
    write_name(writer, &col.short_name)?;
    if let Some(label) = &col.label {
        write_variable_label(writer, label, encoding)?;
    }
    Ok(())
}

fn write_variable_continuation<W: Write>(writer: &mut W) -> Result<()> {
    write_u32(writer, SAV_RECORD_VARIABLE)?;
    write_i32(writer, -1)?;
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
    write_i32(writer, SAV_CONTINUATION_FORMAT)?;
    write_i32(writer, SAV_CONTINUATION_FORMAT)?;
    write_name(writer, "")?;
    Ok(())
}

fn write_variable_label<W: Write>(
    writer: &mut W,
    label: &str,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    let (bytes, _, had_errors) = encoding.encode(label);
    if had_errors {
        return Err(Error::ParseError(
            "variable label not representable in target encoding".to_string(),
        ));
    }
    let len = bytes.len().min(255);
    write_u32(writer, len as u32)?;
    let padded = ((len + 3) / 4) * 4;
    let mut buf = vec![0u8; padded];
    buf[..len].copy_from_slice(&bytes[..len]);
    writer.write_all(&buf)?;
    Ok(())
}

fn write_value_labels<W: Write>(
    writer: &mut W,
    columns: &[ColumnSpec],
    labels: &SpssValueLabels,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    for col in columns {
        let Some(map) = labels.get(&col.name) else {
            continue;
        };
        if col.var_type == VarType::Str {
            return Err(Error::ParseError(
                "string value labels not supported in SPSS writer".to_string(),
            ));
        }
        if map.is_empty() {
            continue;
        }
        write_u32(writer, SAV_RECORD_VALUE_LABEL)?;
        write_u32(writer, map.len() as u32)?;
        for (value, label) in map {
            writer.write_all(&value.to_f64().to_le_bytes())?;
            let (bytes, _, had_errors) = encoding.encode(label);
            if had_errors {
                return Err(Error::ParseError(
                    "value label not representable in target encoding".to_string(),
                ));
            }
            let len = bytes.len().min(255);
            writer.write_all(&[len as u8])?;
            let padded = ((len + 8) / 8) * 8 - 1;
            let mut buf = vec![0u8; padded];
            buf[..len].copy_from_slice(&bytes[..len]);
            writer.write_all(&buf)?;
        }
        write_u32(writer, SAV_RECORD_VALUE_LABEL_VARS)?;
        write_u32(writer, 1)?;
        write_u32(writer, (col.offset + 1) as u32)?;
    }
    Ok(())
}

fn write_long_var_names_record<W: Write>(
    writer: &mut W,
    columns: &[ColumnSpec],
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    let mut tuples: Vec<Vec<u8>> = Vec::new();
    for col in columns {
        if col.name == col.short_name {
            continue;
        }
        let long = sanitize_long_name_for_record(&col.name);
        if long.is_empty() {
            continue;
        }
        let (bytes, _, had_errors) = encoding.encode(&long);
        if had_errors {
            return Err(Error::ParseError(
                "long variable name not representable in target encoding".to_string(),
            ));
        }
        let mut tuple = Vec::with_capacity(col.short_name.len() + bytes.len() + 1);
        tuple.extend_from_slice(col.short_name.as_bytes());
        tuple.push(b'=');
        tuple.extend_from_slice(&bytes);
        tuples.push(tuple);
    }
    if tuples.is_empty() {
        return Ok(());
    }
    let entries_len: usize = tuples.iter().map(|t| t.len()).sum::<usize>() + tuples.len() - 1;
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_LONG_VAR_NAME)?;
    write_u32(writer, 1)?;
    write_u32(writer, entries_len as u32)?;
    for (idx, tuple) in tuples.iter().enumerate() {
        if idx > 0 {
            writer.write_all(b"\t")?;
        }
        writer.write_all(tuple)?;
    }
    Ok(())
}

fn write_very_long_string_record<W: Write>(writer: &mut W, columns: &[ColumnSpec]) -> Result<()> {
    let mut entries: Vec<u8> = Vec::new();
    for col in columns {
        if col.var_type != VarType::Str || col.string_len <= 255 {
            continue;
        }
        entries.extend_from_slice(col.short_name.as_bytes());
        entries.push(b'=');
        let len_str = (col.string_len % 100_000).to_string();
        entries.extend_from_slice(len_str.as_bytes());
        entries.push(0);
        entries.push(b'\t');
    }
    if entries.is_empty() {
        return Ok(());
    }
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_VERY_LONG_STR)?;
    write_u32(writer, 1)?;
    write_u32(writer, entries.len() as u32)?;
    writer.write_all(&entries)?;
    Ok(())
}

fn write_integer_info_record<W: Write>(
    writer: &mut W,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_INTEGER_INFO)?;
    write_u32(writer, 4)?;
    write_u32(writer, 8)?;

    // Match ReadStat defaults for broad importer compatibility.
    write_i32(writer, 20)?; // version_major
    write_i32(writer, 0)?; // version_minor
    write_i32(writer, 0)?; // version_revision
    write_i32(writer, -1)?; // machine_code
    write_i32(writer, SAV_FLOATING_POINT_REP_IEEE)?;
    write_i32(writer, 1)?; // compression_code
    let endianness = if cfg!(target_endian = "little") {
        SAV_ENDIANNESS_LITTLE
    } else {
        SAV_ENDIANNESS_BIG
    };
    write_i32(writer, endianness)?;
    let char_code = if encoding == encoding_rs::UTF_8 {
        65001
    } else {
        1252
    };
    write_i32(writer, char_code)?;
    Ok(())
}

fn write_floating_point_info_record<W: Write>(writer: &mut W) -> Result<()> {
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_FP_INFO)?;
    write_u32(writer, 8)?;
    write_u32(writer, 3)?;
    writer.write_all(&SAV_MISSING_DOUBLE.to_le_bytes())?;
    writer.write_all(&SAV_HIGHEST_DOUBLE.to_le_bytes())?;
    writer.write_all(&SAV_LOWEST_DOUBLE.to_le_bytes())?;
    Ok(())
}

fn write_variable_display_record<W: Write>(writer: &mut W, columns: &[ColumnSpec]) -> Result<()> {
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_VAR_DISPLAY)?;
    write_u32(writer, 4)?;
    write_u32(
        writer,
        (columns.iter().map(|c| c.width).sum::<usize>() * 3) as u32,
    )?;
    for col in columns {
        let measure = if col.var_type == VarType::Str {
            SAV_MEASURE_NOMINAL
        } else {
            SAV_MEASURE_SCALE
        };
        let display_width = if col.var_type == VarType::Str {
            col.string_len.min(255).max(1) as i32
        } else {
            8
        };
        let alignment = if col.var_type == VarType::Str {
            SAV_ALIGNMENT_LEFT
        } else {
            SAV_ALIGNMENT_RIGHT
        };
        for _ in 0..col.width {
            write_i32(writer, measure)?;
            write_i32(writer, display_width)?;
            write_i32(writer, alignment)?;
        }
    }
    Ok(())
}

fn write_number_of_cases_record<W: Write>(writer: &mut W, row_count: u64) -> Result<()> {
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_NUMBER_OF_CASES)?;
    write_u32(writer, 8)?;
    write_u32(writer, 2)?;
    writer.write_all(&1u64.to_le_bytes())?;
    writer.write_all(&row_count.to_le_bytes())?;
    Ok(())
}

fn write_encoding_record<W: Write>(
    writer: &mut W,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    if encoding == encoding_rs::WINDOWS_1252 {
        return Ok(());
    }
    let label = encoding.name();
    let bytes = label.as_bytes();
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_CHAR_ENCODING)?;
    write_u32(writer, 1)?;
    write_u32(writer, bytes.len() as u32)?;
    writer.write_all(bytes)?;
    Ok(())
}

fn write_dict_termination<W: Write>(writer: &mut W) -> Result<()> {
    write_u32(writer, SAV_RECORD_DICT_TERMINATION)?;
    write_u32(writer, 0)?;
    Ok(())
}

fn write_data<W: Write>(
    writer: &mut W,
    df: &DataFrame,
    columns: &[ColumnSpec],
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    let mut cols: Vec<&Series> = Vec::with_capacity(columns.len());
    let mut str_cols: Vec<Option<&StringChunked>> = Vec::with_capacity(columns.len());
    let mut str_bufs: Vec<Option<Vec<u8>>> = Vec::with_capacity(columns.len());
    for col in columns {
        let series = df
            .column(&col.name)
            .map_err(|e| Error::ParseError(e.to_string()))?;
        let series = series.as_materialized_series();
        cols.push(series);
        if col.var_type == VarType::Str {
            let ca = series.str().map_err(|e| Error::ParseError(e.to_string()))?;
            str_cols.push(Some(ca));
            str_bufs.push(Some(vec![b' '; col.width * 8]));
        } else {
            str_cols.push(None);
            str_bufs.push(None);
        }
    }

    for row_idx in 0..df.height() {
        for (col_idx, col) in columns.iter().enumerate() {
            let series = cols[col_idx];
            match col.var_type {
                VarType::Numeric => {
                    let value = series
                        .get(row_idx)
                        .map_err(|e| Error::ParseError(e.to_string()))?;
                    if value.is_null() {
                        let bytes = SAV_MISSING_DOUBLE.to_le_bytes();
                        writer.write_all(&bytes)?;
                    } else {
                        let v = anyvalue_to_f64(value)
                            .ok_or(Error::ParseError("unsupported numeric type".to_string()))?;
                        writer.write_all(&v.to_le_bytes())?;
                    }
                }
                VarType::Str => {
                    let ca = str_cols[col_idx].ok_or_else(|| {
                        Error::ParseError("missing utf8 accessor for string column".to_string())
                    })?;
                    let buf = str_bufs[col_idx].as_mut().ok_or_else(|| {
                        Error::ParseError("missing scratch buffer for string column".to_string())
                    })?;
                    buf.fill(b' ');
                    if let Some(s) = ca.get(row_idx) {
                        let (bytes, _, had_errors) = encoding.encode(s);
                        if had_errors {
                            return Err(Error::ParseError(
                                "string not representable in target encoding".to_string(),
                            ));
                        }
                        write_spss_string_value(buf, bytes.as_ref(), col.string_len);
                    }
                    writer.write_all(&buf)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spss::reader::SpssReader;
    use crate::spss::types::{ValueLabelKey, VarType};
    use polars::frame::row::Row;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn collect_files(dir: &Path, exts: &[&str]) -> Vec<PathBuf> {
        let mut out = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name().and_then(|s| s.to_str()) == Some("too_big") {
                        continue;
                    }
                    out.extend(collect_files(&path, exts));
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
        out.sort();
        out
    }

    fn spss_files() -> Vec<PathBuf> {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("spss")
            .join("data");
        collect_files(&base, &["sav", "zsav"])
    }

    fn temp_path(prefix: &str, ext: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let pid = std::process::id();
        path.push(format!("{prefix}_{pid}_{nanos}.{ext}"));
        path
    }

    fn assert_df_equal(left: &DataFrame, right: &DataFrame) -> PolarsResult<()> {
        if left.height() != right.height() || left.width() != right.width() {
            return Err(PolarsError::ComputeError("dataframe shape mismatch".into()));
        }
        if left.schema() != right.schema() {
            return Err(PolarsError::ComputeError(
                "dataframe schema mismatch".into(),
            ));
        }
        let cols = left.get_column_names_owned();
        for i in 0..left.height() {
            let l = left.get_row(i)?;
            let r = right.get_row(i)?;
            if !rows_equal(&l, &r) {
                let mut details = String::new();
                for (idx, (lv, rv)) in l.0.iter().zip(r.0.iter()).enumerate() {
                    if !anyvalue_equal(lv, rv) {
                        details.push_str(&format!(
                            "col {} ({}): left={:?} right={:?}\n",
                            idx,
                            cols.get(idx).map(|s| s.as_str()).unwrap_or("?"),
                            lv,
                            rv
                        ));
                    }
                }
                return Err(PolarsError::ComputeError(
                    format!("row mismatch at {}\n{}", i, details).into(),
                ));
            }
        }
        Ok(())
    }

    fn rows_equal(left: &Row, right: &Row) -> bool {
        left.0.len() == right.0.len()
            && left
                .0
                .iter()
                .zip(right.0.iter())
                .all(|(l, r)| anyvalue_equal(l, r))
    }

    fn anyvalue_equal(left: &AnyValue, right: &AnyValue) -> bool {
        use AnyValue::*;
        match (left, right) {
            (Null, Null) => true,
            (Float32(l), Float32(r)) => {
                if l.is_nan() && r.is_nan() {
                    true
                } else {
                    l == r
                }
            }
            (Float64(l), Float64(r)) => {
                if l.is_nan() && r.is_nan() {
                    true
                } else {
                    l == r
                }
            }
            (String(l), String(r)) => trim_trailing_nul(l) == trim_trailing_nul(r),
            (Null, String(r)) if r.is_empty() => true,
            (String(l), Null) if l.is_empty() => true,
            _ => left == right,
        }
    }

    fn trim_trailing_nul(s: &str) -> &str {
        s.trim_end_matches('\0')
    }

    fn build_labels(reader: &SpssReader) -> (SpssValueLabels, SpssVariableLabels, bool) {
        let mut value_labels: SpssValueLabels = HashMap::new();
        let mut variable_labels: SpssVariableLabels = HashMap::new();
        let mut supported = true;
        let metadata = reader.metadata();

        for var in &metadata.variables {
            if let Some(label) = var.label.clone() {
                variable_labels.insert(var.name.clone(), label);
            }
            let Some(label_name) = var.value_label.as_ref() else {
                continue;
            };
            let Some(label_def) = metadata.value_labels.iter().find(|v| v.name == *label_name)
            else {
                continue;
            };
            if var.var_type == VarType::Str {
                supported = false;
                continue;
            }
            let mut map: SpssValueLabelMap = HashMap::new();
            for (key, label) in &label_def.mapping {
                match key {
                    ValueLabelKey::Double(v) => {
                        map.insert(SpssValueLabelKey::from_f64(*v), label.clone());
                    }
                    ValueLabelKey::Str(_) => {
                        supported = false;
                    }
                }
            }
            if !map.is_empty() {
                value_labels.insert(var.name.clone(), map);
            }
        }

        (value_labels, variable_labels, supported)
    }

    #[test]
    fn test_spss_roundtrip_with_labels_all_files() {
        let files = spss_files();
        if files.is_empty() {
            return;
        }

        for path in files {
            let reader = match SpssReader::open(&path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("SKIP: {:?} open failed: {}", path, e);
                    continue;
                }
            };

            let df_base = match reader.read().value_labels_as_strings(false).finish() {
                Ok(df) => df,
                Err(e) => {
                    eprintln!("SKIP: {:?} read failed: {}", path, e);
                    continue;
                }
            };

            let (value_labels, variable_labels, supported) = build_labels(&reader);
            if !supported {
                eprintln!(
                    "SKIP: {:?} has unsupported value labels (string labels or labeled strings)",
                    path
                );
                continue;
            }
            if value_labels.is_empty() && variable_labels.is_empty() {
                continue;
            }

            let out_path = temp_path("spss_roundtrip_labels", "sav");
            let writer = SpssWriter::new(&out_path)
                .with_value_labels(value_labels.clone())
                .with_variable_labels(variable_labels.clone());
            if let Err(e) = writer.write_df(&df_base) {
                eprintln!("SKIP: {:?} write (labels) failed: {}", path, e);
                let _ = std::fs::remove_file(&out_path);
                continue;
            }

            let reader_out = match SpssReader::open(&out_path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("SKIP: {:?} open (labels) failed: {}", out_path, e);
                    let _ = std::fs::remove_file(&out_path);
                    continue;
                }
            };
            let roundtrip = reader_out
                .read()
                .value_labels_as_strings(false)
                .finish()
                .unwrap();
            assert_df_equal(&df_base, &roundtrip).unwrap();

            let df_labels = reader_out
                .read()
                .value_labels_as_strings(true)
                .finish()
                .unwrap();
            for col in value_labels.keys() {
                let dtype = df_labels
                    .column(col)
                    .map(|s| s.dtype())
                    .unwrap_or(&DataType::String);
                assert_eq!(
                    dtype,
                    &DataType::String,
                    "expected labeled column {} to be String after roundtrip",
                    col
                );
            }

            let _ = std::fs::remove_file(&out_path);
        }
    }

    #[test]
    fn test_spss_roundtrip_very_long_string_preserves_suffix() {
        let long = format!("{}{}", "x".repeat(3000), "_end");
        let col = Series::new("longstr".into(), &[long.as_str()]);
        let df = DataFrame::new(1, vec![col.into()]).unwrap();
        let out_path = temp_path("spss_roundtrip_long_string", "sav");
        SpssWriter::new(&out_path).write_df(&df).unwrap();
        let out = SpssReader::open(&out_path)
            .unwrap()
            .read()
            .finish()
            .unwrap();
        let got = out
            .column("longstr")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .get(0)
            .unwrap_or("");
        assert_eq!(got.len(), long.len(), "long string length changed");
        assert!(
            got.ends_with("_end"),
            "expected suffix _end, got tail: {:?}",
            &got[got.len().saturating_sub(16)..]
        );
        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_spss_lazy_scan_very_long_string_preserves_suffix() {
        let long = format!("{}{}", "x".repeat(3000), "_end");
        let col = Series::new("longstr".into(), &[long.as_str()]);
        let df = DataFrame::new(1, vec![col.into()]).unwrap();
        let out_path = temp_path("spss_scan_long_string", "sav");
        SpssWriter::new(&out_path).write_df(&df).unwrap();

        let out = crate::spss::scan_sav(out_path.clone(), crate::ScanOptions::default())
            .unwrap()
            .collect()
            .unwrap();
        let got = out
            .column("longstr")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .get(0)
            .unwrap_or("");
        assert_eq!(got.len(), long.len(), "lazy long string length changed");
        assert!(
            got.ends_with("_end"),
            "expected suffix _end, got tail: {:?}",
            &got[got.len().saturating_sub(16)..]
        );
        let _ = std::fs::remove_file(&out_path);
    }
}

fn anyvalue_to_f64(v: AnyValue) -> Option<f64> {
    match v {
        AnyValue::Float64(v) => Some(v),
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::Int16(v) => Some(v as f64),
        AnyValue::Int8(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        AnyValue::UInt16(v) => Some(v as f64),
        AnyValue::UInt8(v) => Some(v as f64),
        AnyValue::Boolean(v) => Some(if v { 1.0 } else { 0.0 }),
        AnyValue::Date(v) => {
            let secs = (v as i64) * 86_400 + SPSS_SEC_SHIFT;
            Some(secs as f64)
        }
        AnyValue::Datetime(v, unit, _) => {
            let ms = match unit {
                TimeUnit::Milliseconds => v,
                TimeUnit::Microseconds => v / 1_000,
                TimeUnit::Nanoseconds => v / 1_000_000,
            };
            let secs = ms / 1_000;
            Some((secs + SPSS_SEC_SHIFT) as f64)
        }
        AnyValue::Time(v) => {
            let secs = v / 1_000_000_000;
            Some(secs as f64)
        }
        _ => None,
    }
}

fn write_name<W: Write>(writer: &mut W, name: &str) -> Result<()> {
    let mut buf = [b' '; 8];
    let bytes = name.as_bytes();
    let copy_len = bytes.len().min(8);
    buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
    writer.write_all(&buf)?;
    Ok(())
}

fn write_u32<W: Write>(writer: &mut W, v: u32) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_i32<W: Write>(writer: &mut W, v: i32) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn encode_format(format_type: u8, width: u8, decimals: u8) -> i32 {
    ((format_type as i32) << 16) | ((width as i32) << 8) | decimals as i32
}

fn long_string_segment_count(string_len: usize) -> usize {
    if string_len <= 255 {
        1
    } else {
        string_len.div_ceil(252)
    }
}

fn long_string_segment_sizes(string_len: usize) -> Vec<usize> {
    if string_len <= 255 {
        return vec![string_len.max(1)];
    }
    let n_segments = long_string_segment_count(string_len);
    let mut out = vec![255; n_segments.saturating_sub(1)];
    out.push(string_len - (n_segments - 1) * 252);
    out
}

fn make_long_string_ghost_name(stem: &str, segment_index: usize) -> Result<String> {
    // ReadStat convention: first 5 chars of short name + base36(segment index).
    let idx = segment_index % 36;
    let suffix = if idx < 10 {
        (b'0' + (idx as u8)) as char
    } else {
        (b'A' + ((idx - 10) as u8)) as char
    };
    let mut out = String::with_capacity(6);
    out.push_str(stem);
    out.push(suffix);
    if out.len() > 8 {
        return Err(Error::ParseError(
            "failed to build SPSS long-string ghost name".to_string(),
        ));
    }
    Ok(out)
}

fn write_spss_string_value(buf: &mut [u8], value: &[u8], declared_len: usize) {
    let copy_len = value.len().min(declared_len);
    if declared_len <= 255 {
        buf[..copy_len].copy_from_slice(&value[..copy_len]);
        return;
    }

    // ReadStat SAV long-string layout stores up to 255 payload bytes per 256-byte chunk.
    let mut row_offset = 0usize;
    let mut val_offset = 0usize;
    while copy_len.saturating_sub(val_offset) > 255 {
        buf[row_offset..row_offset + 255].copy_from_slice(&value[val_offset..val_offset + 255]);
        row_offset += 256;
        val_offset += 255;
    }
    let remaining = copy_len.saturating_sub(val_offset);
    if remaining > 0 {
        buf[row_offset..row_offset + remaining]
            .copy_from_slice(&value[val_offset..val_offset + remaining]);
    }
}
