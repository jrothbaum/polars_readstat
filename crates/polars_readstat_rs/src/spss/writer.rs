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
const SUBTYPE_LONG_VAR_NAME: u32 = 13;
const SUBTYPE_CHAR_ENCODING: u32 = 20;
const SPSS_SEC_SHIFT: i64 = 12_219_379_200;

const SAV_MISSING_DOUBLE: u64 = 0xFFEFFFFFFFFFFFFF;

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
        write_long_var_names_record(&mut writer, &columns, encoding)?;
        write_encoding_record(&mut writer, encoding)?;
        if let Some(labels) = value_labels.as_ref() {
            write_value_labels(&mut writer, &columns, labels, encoding)?;
        }
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
    format_type: Option<u8>,
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
            let (var_type, string_len, width, format_type) = dtype_to_spss(col, df)?;
            let label = variable_labels.and_then(|labels| labels.get(&col.name).cloned());
            cols.push(ColumnSpec {
                name: col.name.clone(),
                short_name: short_names[idx].clone(),
                var_type,
                string_len,
                width,
                offset,
                format_type,
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
        let (var_type, string_len, width, format_type) = infer_series(series)?;
        let label = variable_labels.and_then(|labels| labels.get(&name).cloned());
        cols.push(ColumnSpec {
            name,
            short_name: short_names[idx].clone(),
            var_type,
            string_len,
            width,
            offset,
            format_type,
            label,
        });
        offset += width;
    }
    Ok(cols)
}

fn dtype_to_spss(
    col: &SpssWriteColumn,
    df: &DataFrame,
) -> Result<(VarType, usize, usize, Option<u8>)> {
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
            Ok((var_type, string_len, width, None))
        }
        DataType::Date => Ok((VarType::Numeric, 0, 1, Some(20))),
        DataType::Datetime(_, _) => Ok((VarType::Numeric, 0, 1, Some(22))),
        DataType::Time => Ok((VarType::Numeric, 0, 1, Some(21))),
        _ => Ok((VarType::Numeric, 0, 1, None)),
    }
}

fn infer_series(series: &Series) -> Result<(VarType, usize, usize, Option<u8>)> {
    match series.dtype() {
        DataType::String => {
            let width = max_string_width(series)?;
            let (var_type, string_len, width) = string_layout(width)?;
            Ok((var_type, string_len, width, None))
        }
        DataType::Date => Ok((VarType::Numeric, 0, 1, Some(20))),
        DataType::Datetime(_, _) => Ok((VarType::Numeric, 0, 1, Some(22))),
        DataType::Time => Ok((VarType::Numeric, 0, 1, Some(21))),
        _ => Ok((VarType::Numeric, 0, 1, None)),
    }
}

fn string_layout(len: usize) -> Result<(VarType, usize, usize)> {
    if len > 255 {
        return Err(Error::ParseError(
            "SPSS writer does not support strings > 255 bytes".to_string(),
        ));
    }
    let width = (len.max(1) + 7) / 8;
    Ok((VarType::Str, len.max(1), width))
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
        write_variable_record(writer, col, encoding)?;
        if col.width > 1 {
            for _ in 1..col.width {
                write_variable_continuation(writer)?;
            }
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
    let fmt = col.format_type.map(|v| (v as i32) << 16).unwrap_or(0);
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
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
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
    let mut entries: Vec<u8> = Vec::new();
    for col in columns {
        if col.name == col.short_name {
            continue;
        }
        let long = sanitize_long_name_for_record(&col.name);
        if long.is_empty() {
            continue;
        }
        entries.extend_from_slice(col.short_name.as_bytes());
        entries.push(b'=');
        let (bytes, _, had_errors) = encoding.encode(&long);
        if had_errors {
            return Err(Error::ParseError(
                "long variable name not representable in target encoding".to_string(),
            ));
        }
        entries.extend_from_slice(&bytes);
        entries.push(b'\t');
    }
    if entries.is_empty() {
        return Ok(());
    }
    write_u32(writer, SAV_RECORD_HAS_DATA)?;
    write_u32(writer, SUBTYPE_LONG_VAR_NAME)?;
    write_u32(writer, 1)?;
    write_u32(writer, entries.len() as u32)?;
    writer.write_all(&entries)?;
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
                        let copy_len = bytes.len().min(col.string_len);
                        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
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
