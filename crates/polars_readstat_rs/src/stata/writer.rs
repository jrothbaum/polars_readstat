use crate::stata::compress::{compress_df, CompressOptions};
use crate::stata::error::{Error, Result};
use polars::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const DTA_VERSION_COMPAT: u16 = 118;
const DTA_VERSION_WIDE: u16 = 119;
const DTA_118_MAX_VARS: usize = 32_767;
const DTA_MAX_STR: usize = 2045;
const DTA_VAR_NAME_LEN: usize = 129;
const DTA_FMT_ENTRY_LEN: usize = 57;
const DTA_LBLLIST_ENTRY_LEN: usize = 129;
const DTA_VAR_LABEL_ENTRY_LEN: usize = 321;

const DTA_DEFAULT_DISPLAY_WIDTH_BYTE: i32 = 8;
const DTA_DEFAULT_DISPLAY_WIDTH_INT16: i32 = 8;
const DTA_DEFAULT_DISPLAY_WIDTH_INT32: i32 = 12;
const DTA_DEFAULT_DISPLAY_WIDTH_FLOAT: i32 = 9;
const DTA_DEFAULT_DISPLAY_WIDTH_DOUBLE: i32 = 10;
const DTA_DEFAULT_DISPLAY_WIDTH_STRING: i32 = 9;

const DTA_113_MAX_INT8: i8 = 0x64;
const DTA_113_MAX_INT16: i16 = 0x7fe4;
const DTA_113_MAX_INT32: i32 = 0x7fffffe4;
const DTA_113_MAX_FLOAT: u32 = 0x7effffff;
const DTA_113_MAX_DOUBLE: u64 = 0x7fdfffffffffffff;

const DTA_113_MISSING_INT8: i8 = 0x65;
const DTA_113_MISSING_INT16: i16 = 0x7fe5;
const DTA_113_MISSING_INT32: i32 = 0x7fffffe5;
const DTA_113_MISSING_FLOAT: u32 = 0x7f000000;
const DTA_113_MISSING_DOUBLE: u64 = 0x7fe0000000000000;

const DTA_113_MIN_INT8: i8 = -0x7f;
const DTA_113_MIN_INT16: i16 = -0x7fff;
const DTA_113_MIN_INT32: i32 = -0x7fffffff;

const DTA_117_TYPE_CODE_INT8: u16 = 0xfffa;
const DTA_117_TYPE_CODE_INT16: u16 = 0xfff9;
const DTA_117_TYPE_CODE_INT32: u16 = 0xfff8;
const DTA_117_TYPE_CODE_FLOAT: u16 = 0xfff7;
const DTA_117_TYPE_CODE_DOUBLE: u16 = 0xfff6;
const DTA_117_TYPE_CODE_STRL: u16 = 0x8000;
const DTA_VALUE_LABEL_PADDING_LEN: usize = 3;
const DTA_PARALLEL_CHUNK_ROWS: usize = 4096;

pub type ValueLabelMap = BTreeMap<i32, String>;
pub type ValueLabels = HashMap<String, ValueLabelMap>;
pub type VariableLabels = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct StataWriteSchema {
    pub columns: Vec<StataWriteColumn>,
    pub row_count: Option<usize>,
    pub value_labels: Option<ValueLabels>,
    pub variable_labels: Option<VariableLabels>,
}

#[derive(Debug, Clone)]
pub struct StataWriteColumn {
    pub name: String,
    pub dtype: DataType,
    pub string_width_bytes: Option<usize>,
}

pub fn pandas_make_stata_column_names(names: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(names.len());
    let mut converted = Vec::with_capacity(names.len());
    for name in names {
        let (mut col, mut changed) = maybe_convert_name(name);
        if col.len() > 32 {
            col.truncate(32);
            changed = true;
        }
        if converted.contains(&col) {
            col = make_unique(&col, &converted);
            changed = true;
        }
        if changed {
            out.push(col.clone());
        } else {
            out.push(col.clone());
        }
        converted.push(col);
    }
    out
}

pub fn pandas_rename_df(df: &DataFrame) -> Result<DataFrame> {
    let names = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let new_names = pandas_make_stata_column_names(&names);
    let mut out = df.clone();
    out.set_column_names(&new_names).map_err(Error::Polars)?;
    Ok(out)
}

pub fn pandas_prepare_df_for_stata(df: &DataFrame) -> Result<DataFrame> {
    let df = pandas_rename_df(df)?;
    let mut cols = Vec::with_capacity(df.width());
    for col in df.columns() {
        let series = col.as_materialized_series();
        let target = target_dtype_for_series(series)?;
        let out = if &target != series.dtype() {
            series.cast(&target).map_err(Error::Polars)?
        } else {
            series.clone()
        };
        cols.push(out.into_column());
    }
    DataFrame::new_infer_height(cols).map_err(Error::Polars)
}

fn pandas_rename_schema(schema: &StataWriteSchema) -> StataWriteSchema {
    let names = schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();
    let new_names = pandas_make_stata_column_names(&names);
    let mut columns = schema.columns.clone();
    for (col, new) in columns.iter_mut().zip(new_names.iter()) {
        col.name = new.clone();
    }
    let value_labels = schema.value_labels.as_ref().map(|labels| {
        let name_map = build_name_map(&names, &new_names);
        rename_value_labels(labels, &name_map)
    });
    let variable_labels = schema.variable_labels.as_ref().map(|labels| {
        let name_map = build_name_map(&names, &new_names);
        rename_variable_labels(labels, &name_map)
    });
    StataWriteSchema {
        columns,
        row_count: schema.row_count,
        value_labels,
        variable_labels,
    }
}

pub struct StataWriter {
    path: PathBuf,
    schema: Option<StataWriteSchema>,
    compress: Option<CompressOptions>,
    value_labels: Option<ValueLabels>,
    variable_labels: Option<VariableLabels>,
    n_threads: Option<usize>,
}

impl StataWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let n_threads = std::thread::available_parallelism()
            .map(|n| n.get().min(4))
            .unwrap_or(4);
        Self {
            path: path.as_ref().to_path_buf(),
            schema: None,
            compress: None,
            value_labels: None,
            variable_labels: None,
            n_threads: Some(n_threads),
        }
    }

    pub fn with_schema(mut self, schema: StataWriteSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_compress(mut self, compress: bool) -> Self {
        self.compress = if compress {
            Some(CompressOptions::default())
        } else {
            None
        };
        self
    }

    pub fn with_compress_options(mut self, opts: CompressOptions) -> Self {
        self.compress = Some(opts);
        self
    }

    pub fn with_value_labels(mut self, labels: ValueLabels) -> Self {
        self.value_labels = Some(labels);
        self
    }

    pub fn with_variable_labels(mut self, labels: VariableLabels) -> Self {
        self.variable_labels = Some(labels);
        self
    }

    pub fn with_n_threads(mut self, n: usize) -> Self {
        self.n_threads = Some(n);
        self
    }

    pub fn write_df(&self, df: &DataFrame) -> Result<()> {
        let original_names = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let new_names = pandas_make_stata_column_names(&original_names);
        let name_map = build_name_map(&original_names, &new_names);
        let df = match &self.compress {
            Some(opts) => {
                let compressed = compress_df(df, opts.clone())?;
                pandas_prepare_df_for_stata(&compressed)?
            }
            None => pandas_prepare_df_for_stata(df)?,
        };
        let schema = self.schema.as_ref().map(pandas_rename_schema);
        let value_labels = merge_value_labels(
            schema.as_ref().and_then(|s| s.value_labels.clone()),
            self.value_labels.clone(),
        );
        let value_labels = value_labels.map(|labels| rename_value_labels(&labels, &name_map));
        let variable_labels = merge_variable_labels(
            schema.as_ref().and_then(|s| s.variable_labels.clone()),
            self.variable_labels.clone(),
        );
        let variable_labels =
            variable_labels.map(|labels| rename_variable_labels(&labels, &name_map));
        let prepared = PreparedWrite::from_df(
            &df,
            schema.as_ref(),
            value_labels.as_ref(),
            variable_labels.as_ref(),
        )?;
        let file = File::create(&self.path)?;
        let writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        write_dta(writer, &prepared, Some(&df), self.n_threads)?;
        Ok(())
    }

    pub fn write_batches<I>(&self, batches: I, schema: StataWriteSchema) -> Result<()>
    where
        I: IntoIterator<Item = DataFrame>,
    {
        let original_names = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let new_names = pandas_make_stata_column_names(&original_names);
        let name_map = build_name_map(&original_names, &new_names);
        let schema = pandas_rename_schema(&schema);
        let value_labels =
            merge_value_labels(schema.value_labels.clone(), self.value_labels.clone());
        let value_labels = value_labels.map(|labels| rename_value_labels(&labels, &name_map));
        let variable_labels =
            merge_variable_labels(schema.variable_labels.clone(), self.variable_labels.clone());
        let variable_labels =
            variable_labels.map(|labels| rename_variable_labels(&labels, &name_map));
        let prepared =
            PreparedWrite::from_schema(&schema, value_labels.as_ref(), variable_labels.as_ref())?;
        if prepared.row_count.is_none() {
            return Err(Error::ParseError(
                "row_count must be provided for batch writing".to_string(),
            ));
        }
        if prepared.has_strl {
            return Err(Error::ParseError(
                "batch writing with strL is not supported without a pre-pass".to_string(),
            ));
        }
        let file = File::create(&self.path)?;
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        write_dta_header_and_map(&mut writer, &prepared)?;
        write_dta_descriptors(&mut writer, &prepared)?;
        write_dta_variable_labels(&mut writer, &prepared)?;
        write_dta_characteristics(&mut writer)?;
        let _ = write_dta_data_batches(&mut writer, &prepared, batches)?;
        write_dta_strls(&mut writer, &prepared)?;
        write_dta_value_labels(&mut writer, &prepared)?;
        write_dta_footer(&mut writer)?;
        Ok(())
    }

    pub fn write_batches_streaming<I>(&self, batches: I, schema: StataWriteSchema) -> Result<()>
    where
        I: IntoIterator<Item = DataFrame>,
    {
        let original_names = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let new_names = pandas_make_stata_column_names(&original_names);
        let name_map = build_name_map(&original_names, &new_names);
        let schema = pandas_rename_schema(&schema);
        let value_labels =
            merge_value_labels(schema.value_labels.clone(), self.value_labels.clone());
        let value_labels = value_labels.map(|labels| rename_value_labels(&labels, &name_map));
        let variable_labels =
            merge_variable_labels(schema.variable_labels.clone(), self.variable_labels.clone());
        let variable_labels =
            variable_labels.map(|labels| rename_variable_labels(&labels, &name_map));
        let prepared =
            PreparedWrite::from_schema(&schema, value_labels.as_ref(), variable_labels.as_ref())?;
        if prepared.has_strl {
            return Err(Error::ParseError(
                "streaming batch writing with strL is not supported without a pre-pass".to_string(),
            ));
        }

        let file = File::create(&self.path)?;
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        let patch_offsets = write_dta_header_with_placeholders(&mut writer, &prepared)?;
        write_dta_descriptors(&mut writer, &prepared)?;
        write_dta_variable_labels(&mut writer, &prepared)?;
        write_dta_characteristics(&mut writer)?;
        let rows_written = write_dta_data_batches(&mut writer, &prepared, batches)?;
        write_dta_strls(&mut writer, &prepared)?;
        write_dta_value_labels(&mut writer, &prepared)?;
        write_dta_footer(&mut writer)?;
        patch_header_and_map(&mut writer, &prepared, rows_written, patch_offsets)?;
        writer.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    index: usize,
    name: String,
    dtype: DataType,
    kind: ColumnKind,
    format: Option<String>,
}

#[derive(Debug, Clone)]
enum ColumnKind {
    Int8,
    Int16,
    Int32,
    Float32,
    Float64,
    Str { width: usize },
    StrL,
}

#[derive(Debug, Clone)]
struct StrlEntry {
    v: u32,
    o: u64,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ValueLabelTable {
    name: String,
    table: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PreparedWrite {
    dta_version: u16,
    columns: Vec<ColumnSpec>,
    row_count: Option<usize>,
    record_len: usize,
    typlist: Vec<u16>,
    varlist: Vec<u8>,
    srtlist_len: usize,
    fmtlist: Vec<u8>,
    lbllist: Vec<u8>,
    var_labels: Vec<u8>,
    strls: Vec<StrlEntry>,
    has_strl: bool,
    value_labels: Vec<ValueLabelTable>,
}

impl PreparedWrite {
    fn from_schema(
        schema: &StataWriteSchema,
        value_labels: Option<&ValueLabels>,
        variable_labels: Option<&VariableLabels>,
    ) -> Result<Self> {
        let columns = infer_columns(None, Some(schema))?;
        Self::build(
            columns,
            schema.row_count,
            vec![],
            value_labels,
            variable_labels,
        )
    }

    fn from_df(
        df: &DataFrame,
        schema: Option<&StataWriteSchema>,
        value_labels: Option<&ValueLabels>,
        variable_labels: Option<&VariableLabels>,
    ) -> Result<Self> {
        let columns = infer_columns(Some(df), schema)?;
        let row_count = Some(df.height());
        let strls = build_strls(df, &columns)?;
        Self::build(columns, row_count, strls, value_labels, variable_labels)
    }

    fn build(
        columns: Vec<ColumnSpec>,
        row_count: Option<usize>,
        strls: Vec<StrlEntry>,
        value_labels: Option<&ValueLabels>,
        variable_labels: Option<&VariableLabels>,
    ) -> Result<Self> {
        let nvar = columns.len();
        if nvar == 0 {
            return Err(Error::ParseError("no columns to write".to_string()));
        }
        if nvar > u32::MAX as usize {
            return Err(Error::ParseError(format!(
                "too many columns for Stata writer: {}",
                nvar
            )));
        }

        validate_column_names_unique(&columns)?;
        let dta_version = choose_dta_version(nvar);
        let typlist = columns
            .iter()
            .map(type_code_for_column)
            .collect::<Result<Vec<_>>>()?;
        let record_len = columns.iter().map(storage_width_for_column).sum::<usize>();

        let varlist_len = DTA_VAR_NAME_LEN * nvar;
        let mut varlist = vec![0u8; varlist_len];
        for (i, col) in columns.iter().enumerate() {
            validate_name(&col.name)?;
            let slot = &mut varlist[i * DTA_VAR_NAME_LEN..(i + 1) * DTA_VAR_NAME_LEN];
            let name_bytes = col.name.as_bytes();
            let copy_len = name_bytes.len().min(DTA_VAR_NAME_LEN);
            slot[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
        }

        let srtlist_entry_len = if dta_version >= DTA_VERSION_WIDE { 4 } else { 2 };
        let srtlist_len = (nvar + 1) * srtlist_entry_len;
        let fmtlist_len = DTA_FMT_ENTRY_LEN * nvar;
        let mut fmtlist = vec![0u8; fmtlist_len];
        for (i, col) in columns.iter().enumerate() {
            let format = format_for_column(col);
            let slot = &mut fmtlist[i * DTA_FMT_ENTRY_LEN..(i + 1) * DTA_FMT_ENTRY_LEN];
            let fmt_bytes = format.as_bytes();
            let copy_len = fmt_bytes.len().min(DTA_FMT_ENTRY_LEN);
            slot[..copy_len].copy_from_slice(&fmt_bytes[..copy_len]);
        }

        let (lbllist, value_label_tables) = build_value_labels(&columns, value_labels)?;

        let var_labels = build_variable_labels(&columns, variable_labels)?;

        let has_strl = columns.iter().any(|c| matches!(c.kind, ColumnKind::StrL));

        Ok(Self {
            dta_version,
            columns,
            row_count,
            record_len,
            typlist,
            varlist,
            srtlist_len,
            fmtlist,
            lbllist,
            var_labels,
            strls,
            has_strl,
            value_labels: value_label_tables,
        })
    }
}

fn choose_dta_version(nvar: usize) -> u16 {
    if nvar > DTA_118_MAX_VARS {
        DTA_VERSION_WIDE
    } else {
        DTA_VERSION_COMPAT
    }
}

fn infer_columns(
    df: Option<&DataFrame>,
    schema: Option<&StataWriteSchema>,
) -> Result<Vec<ColumnSpec>> {
    let mut columns = Vec::new();
    if let Some(schema) = schema {
        for col in &schema.columns {
            let width = if matches!(col.dtype, DataType::String) && col.string_width_bytes.is_none()
            {
                if let Some(df) = df {
                    let column = df.column(&col.name).map_err(|e| Error::Polars(e))?;
                    Some(max_string_width(column)?)
                } else {
                    None
                }
            } else {
                col.string_width_bytes
            };
            let kind = kind_from_dtype(col.dtype.clone(), width)?;
            let format = match col.dtype {
                DataType::Date => Some("%td".to_string()),
                DataType::Datetime(_, _) => Some("%tc".to_string()),
                DataType::Time => Some("%tcHH:MM:SS".to_string()),
                _ => None,
            };
            columns.push(ColumnSpec {
                index: columns.len(),
                name: col.name.clone(),
                dtype: col.dtype.clone(),
                kind,
                format,
            });
        }
        return Ok(columns);
    }

    let df =
        df.ok_or_else(|| Error::ParseError("DataFrame required to infer schema".to_string()))?;
    for column in df.columns() {
        let series = column.as_materialized_series();
        let dtype = series.dtype().clone();
        let width = if matches!(dtype, DataType::String) {
            Some(max_string_width(column)?)
        } else {
            None
        };
        let kind = if matches!(dtype, DataType::String)
            && (string_has_nul(series)? || string_has_trailing_space(series)?)
        {
            ColumnKind::StrL
        } else {
            kind_from_dtype(dtype.clone(), width)?
        };
        let format = match dtype {
            DataType::Date => Some("%td".to_string()),
            DataType::Datetime(_, _) => Some("%tc".to_string()),
            DataType::Time => Some("%tcHH:MM:SS".to_string()),
            _ => None,
        };
        columns.push(ColumnSpec {
            index: columns.len(),
            name: series.name().to_string(),
            dtype,
            kind,
            format,
        });
    }
    Ok(columns)
}

fn build_name_map(old: &[String], new: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (o, n) in old.iter().zip(new.iter()) {
        map.insert(o.clone(), n.clone());
    }
    map
}

fn rename_value_labels(labels: &ValueLabels, name_map: &HashMap<String, String>) -> ValueLabels {
    let mut out = HashMap::new();
    for (name, mapping) in labels {
        let key = name_map.get(name).cloned().unwrap_or_else(|| name.clone());
        out.insert(key, mapping.clone());
    }
    out
}

fn rename_variable_labels(
    labels: &VariableLabels,
    name_map: &HashMap<String, String>,
) -> VariableLabels {
    let mut out = HashMap::new();
    for (name, label) in labels {
        let key = name_map.get(name).cloned().unwrap_or_else(|| name.clone());
        out.insert(key, label.clone());
    }
    out
}

fn merge_value_labels(
    base: Option<ValueLabels>,
    extra: Option<ValueLabels>,
) -> Option<ValueLabels> {
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
    base: Option<VariableLabels>,
    extra: Option<VariableLabels>,
) -> Option<VariableLabels> {
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

fn kind_from_dtype(dtype: DataType, width: Option<usize>) -> Result<ColumnKind> {
    Ok(match dtype {
        DataType::Int8 | DataType::Boolean => ColumnKind::Int8,
        DataType::Int16 => ColumnKind::Int16,
        DataType::Int32 | DataType::Date => ColumnKind::Int32,
        DataType::Float32 => ColumnKind::Float32,
        DataType::Float64 | DataType::Datetime(_, _) | DataType::Time => ColumnKind::Float64,
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ColumnKind::Float64,
        DataType::String => {
            let width = width.unwrap_or(1).max(1);
            if width > DTA_MAX_STR {
                ColumnKind::StrL
            } else {
                ColumnKind::Str { width }
            }
        }
        other => {
            return Err(Error::UnsupportedType(format!("{:?}", other)));
        }
    })
}

fn build_value_labels(
    columns: &[ColumnSpec],
    value_labels: Option<&ValueLabels>,
) -> Result<(Vec<u8>, Vec<ValueLabelTable>)> {
    let nvar = columns.len();
    let mut lbllist = vec![0u8; DTA_LBLLIST_ENTRY_LEN * nvar];
    let mut tables = Vec::new();

    let Some(value_labels) = value_labels else {
        return Ok((lbllist, tables));
    };

    for (i, col) in columns.iter().enumerate() {
        let Some(mapping) = value_labels.get(&col.name) else {
            continue;
        };
        if mapping.is_empty() {
            continue;
        }
        let label_name = col.name.clone();
        let slot = &mut lbllist[i * DTA_LBLLIST_ENTRY_LEN..(i + 1) * DTA_LBLLIST_ENTRY_LEN];
        let name_bytes = label_name.as_bytes();
        let copy_len = name_bytes.len().min(DTA_LBLLIST_ENTRY_LEN);
        slot[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

        let table = build_value_label_table(mapping)?;
        tables.push(ValueLabelTable {
            name: label_name,
            table,
        });
    }

    Ok((lbllist, tables))
}

fn build_variable_labels(
    columns: &[ColumnSpec],
    variable_labels: Option<&VariableLabels>,
) -> Result<Vec<u8>> {
    let nvar = columns.len();
    let mut var_labels = vec![0u8; DTA_VAR_LABEL_ENTRY_LEN * nvar];
    let Some(variable_labels) = variable_labels else {
        return Ok(var_labels);
    };

    for (i, col) in columns.iter().enumerate() {
        let Some(label) = variable_labels.get(&col.name) else {
            continue;
        };
        let slot = &mut var_labels[i * DTA_VAR_LABEL_ENTRY_LEN..(i + 1) * DTA_VAR_LABEL_ENTRY_LEN];
        let bytes = label.as_bytes();
        let copy_len = bytes.len().min(DTA_VAR_LABEL_ENTRY_LEN);
        slot[..copy_len].copy_from_slice(&bytes[..copy_len]);
    }
    Ok(var_labels)
}

fn build_value_label_table(mapping: &ValueLabelMap) -> Result<Vec<u8>> {
    let mut offsets = Vec::with_capacity(mapping.len());
    let mut values = Vec::with_capacity(mapping.len());
    let mut text = Vec::new();

    for (value, label) in mapping.iter() {
        if *value > DTA_113_MAX_INT32 || *value < DTA_113_MIN_INT32 {
            return Err(Error::NumericOutOfRange);
        }
        let offset = text.len() as u32;
        offsets.push(offset);
        values.push(*value);
        let mut bytes = label.as_bytes().to_vec();
        if bytes.iter().any(|b| *b == 0) {
            bytes.retain(|b| *b != 0);
        }
        text.extend_from_slice(&bytes);
        text.push(0);
    }

    let n = mapping.len() as u32;
    let txtlen = text.len() as u32;
    let mut table = Vec::with_capacity(8 + (n as usize) * 8 + text.len());
    table.extend_from_slice(&n.to_le_bytes());
    table.extend_from_slice(&txtlen.to_le_bytes());
    for off in offsets {
        table.extend_from_slice(&off.to_le_bytes());
    }
    for value in values {
        table.extend_from_slice(&value.to_le_bytes());
    }
    table.extend_from_slice(&text);
    Ok(table)
}

fn max_string_width(column: &Column) -> Result<usize> {
    let series = column.as_materialized_series();
    let utf8 = series.str().map_err(|e| Error::Polars(e))?;
    let mut max_len = 0usize;
    for opt in utf8.into_iter() {
        if let Some(s) = opt {
            max_len = max_len.max(s.as_bytes().len());
        }
    }
    Ok(max_len.max(1))
}

fn string_has_nul(series: &Series) -> Result<bool> {
    let utf8 = series.str().map_err(|e| Error::Polars(e))?;
    for opt in utf8.into_iter() {
        if let Some(s) = opt {
            if s.as_bytes().iter().any(|b| *b == 0) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn string_has_trailing_space(series: &Series) -> Result<bool> {
    let utf8 = series.str().map_err(|e| Error::Polars(e))?;
    for opt in utf8.into_iter() {
        if let Some(s) = opt {
            if s.ends_with(' ') {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn type_code_for_column(col: &ColumnSpec) -> Result<u16> {
    Ok(match col.kind {
        ColumnKind::Int8 => DTA_117_TYPE_CODE_INT8,
        ColumnKind::Int16 => DTA_117_TYPE_CODE_INT16,
        ColumnKind::Int32 => DTA_117_TYPE_CODE_INT32,
        ColumnKind::Float32 => DTA_117_TYPE_CODE_FLOAT,
        ColumnKind::Float64 => DTA_117_TYPE_CODE_DOUBLE,
        ColumnKind::Str { width } => width as u16,
        ColumnKind::StrL => DTA_117_TYPE_CODE_STRL,
    })
}

fn storage_width_for_column(col: &ColumnSpec) -> usize {
    match col.kind {
        ColumnKind::Int8 => 1,
        ColumnKind::Int16 => 2,
        ColumnKind::Int32 => 4,
        ColumnKind::Float32 => 4,
        ColumnKind::Float64 => 8,
        ColumnKind::Str { width } => width,
        ColumnKind::StrL => 8,
    }
}

fn format_for_column(col: &ColumnSpec) -> String {
    if let Some(fmt) = &col.format {
        return fmt.clone();
    }
    match col.kind {
        ColumnKind::Str { .. } | ColumnKind::StrL => {
            format!("%-{}s", DTA_DEFAULT_DISPLAY_WIDTH_STRING)
        }
        ColumnKind::Int8 => format!("%{}.0g", DTA_DEFAULT_DISPLAY_WIDTH_BYTE),
        ColumnKind::Int16 => format!("%{}.0g", DTA_DEFAULT_DISPLAY_WIDTH_INT16),
        ColumnKind::Int32 => format!("%{}.0g", DTA_DEFAULT_DISPLAY_WIDTH_INT32),
        ColumnKind::Float32 => format!("%{}.0g", DTA_DEFAULT_DISPLAY_WIDTH_FLOAT),
        ColumnKind::Float64 => format!("%{}.0g", DTA_DEFAULT_DISPLAY_WIDTH_DOUBLE),
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > DTA_VAR_NAME_LEN {
        return Err(Error::InvalidName(name.to_string()));
    }
    Ok(())
}

fn maybe_convert_name(name: &str) -> (String, bool) {
    let mut changed = false;
    let mut col = name.to_string();
    if !valid_name(&col) {
        changed = true;
        col = replace_invalid_chars(&col);
        if col.is_empty() {
            col = "_".to_string();
        }
        if is_reserved_word(&col) || starts_with_number(&col) {
            col = format!("_{}", col);
        }
    }
    (col, changed)
}

fn valid_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    if is_reserved_word(name) {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    for ch in chars {
        if !(ch == '_' || ch.is_ascii_alphanumeric()) {
            return false;
        }
    }
    true
}

fn starts_with_number(name: &str) -> bool {
    name.chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
}

fn replace_invalid_chars(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn is_reserved_word(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    if lower == "strl" {
        return true;
    }
    if lower.starts_with("str") && lower[3..].parse::<usize>().is_ok() {
        return true;
    }
    const RESERVED_WORDS: &[&str] = &[
        "aggregate",
        "array",
        "boolean",
        "break",
        "byte",
        "case",
        "catch",
        "class",
        "colvector",
        "complex",
        "const",
        "continue",
        "default",
        "delegate",
        "delete",
        "do",
        "double",
        "else",
        "eltypedef",
        "end",
        "enum",
        "explicit",
        "export",
        "external",
        "float",
        "for",
        "friend",
        "function",
        "global",
        "goto",
        "if",
        "inline",
        "int",
        "local",
        "long",
        "null",
        "pragma",
        "protected",
        "quad",
        "rowvector",
        "short",
        "typedef",
        "typename",
        "virtual",
        "_all",
        "_n",
        "_skip",
        "_b",
        "_pi",
        "in",
        "_pred",
        "_coef",
        "_rc",
        "using",
        "_cons",
        "_se",
        "with",
    ];
    RESERVED_WORDS.contains(&lower.as_str())
}

fn make_unique(name: &str, existing: &[String]) -> String {
    let mut dup_id = 0usize;
    let mut candidate = name.to_string();
    while existing.contains(&candidate) {
        candidate = format!("_{}{}", dup_id, name);
        if candidate.len() > 32 {
            candidate.truncate(32);
        }
        dup_id += 1;
    }
    candidate
}

fn build_strls(df: &DataFrame, columns: &[ColumnSpec]) -> Result<Vec<StrlEntry>> {
    let mut strl_cols = Vec::new();

    for col in columns {
        if !matches!(col.kind, ColumnKind::StrL) {
            continue;
        }

        let column = df.column(&col.name).map_err(Error::Polars)?;
        let series = column.as_materialized_series();
        let utf8 = series.str().map_err(Error::Polars)?.clone();
        strl_cols.push((col.index, utf8));
    }

    let mut entries = Vec::new();
    for row_idx in 0..df.height() {
        for (col_index, utf8) in &strl_cols {
            if let Some(s) = utf8.get(row_idx) {
                // Add null terminator for Stata compatibility
                // Stata's native reader expects strL strings to be null-terminated
                let mut data = s.as_bytes().to_vec();
                data.push(0);
                entries.push(StrlEntry {
                    v: (*col_index + 1) as u32,
                    o: (row_idx + 1) as u64,
                    data,
                });
            }
        }
    }

    Ok(entries)
}

#[derive(Debug, Clone, Copy)]
struct HeaderPatchOffsets {
    nobs_offset: u64,
    map_values_offset: u64,
    map_offset: u64,
}

fn write_dta<W: Write>(
    mut writer: W,
    prepared: &PreparedWrite,
    df: Option<&DataFrame>,
    n_threads: Option<usize>,
) -> Result<()> {
    write_dta_header_and_map(&mut writer, prepared)?;
    write_dta_descriptors(&mut writer, prepared)?;
    write_dta_variable_labels(&mut writer, prepared)?;
    write_dta_characteristics(&mut writer)?;
    if let Some(df) = df {
        write_dta_data_df_parallel(&mut writer, prepared, df, n_threads)?;
    }
    write_dta_strls(&mut writer, prepared)?;
    write_dta_value_labels(&mut writer, prepared)?;
    write_dta_footer(&mut writer)?;
    Ok(())
}

fn write_dta_header_and_map<W: Write>(writer: &mut W, prepared: &PreparedWrite) -> Result<()> {
    write_tag(writer, "<stata_dta>")?;
    write_tag(writer, "<header>")?;
    write_string(writer, &format!("<release>{}</release>", prepared.dta_version))?;
    write_tag(writer, "<byteorder>")?;
    write_string(writer, "LSF")?;
    write_tag(writer, "</byteorder>")?;
    write_tag(writer, "<K>")?;
    if prepared.dta_version >= DTA_VERSION_WIDE {
        let nvars = u32::try_from(prepared.columns.len())
            .map_err(|_| Error::ParseError("too many columns for Stata 119 header".to_string()))?;
        write_u32(writer, nvars)?;
    } else {
        let nvars = u16::try_from(prepared.columns.len())
            .map_err(|_| Error::ParseError("too many columns for Stata 118 header".to_string()))?;
        write_u16(writer, nvars)?;
    }
    write_tag(writer, "</K>")?;
    write_tag(writer, "<N>")?;
    let nobs = prepared.row_count.unwrap_or(0) as u64;
    write_u64(writer, nobs)?;
    write_tag(writer, "</N>")?;
    write_tag(writer, "<label>")?;
    write_u16(writer, 0)?;
    write_tag(writer, "</label>")?;
    write_tag(writer, "<timestamp>")?;
    write_u8(writer, 0)?;
    write_tag(writer, "</timestamp>")?;
    write_tag(writer, "</header>")?;

    let offset_map = current_offset_after_header(prepared.dta_version);
    let map = build_map(prepared, offset_map);
    write_tag(writer, "<map>")?;
    for v in map {
        write_u64(writer, v)?;
    }
    write_tag(writer, "</map>")?;
    Ok(())
}

fn write_dta_header_with_placeholders<W: Write + Seek>(
    writer: &mut W,
    prepared: &PreparedWrite,
) -> Result<HeaderPatchOffsets> {
    write_tag(writer, "<stata_dta>")?;
    write_tag(writer, "<header>")?;
    write_string(writer, &format!("<release>{}</release>", prepared.dta_version))?;
    write_tag(writer, "<byteorder>")?;
    write_string(writer, "LSF")?;
    write_tag(writer, "</byteorder>")?;
    write_tag(writer, "<K>")?;
    if prepared.dta_version >= DTA_VERSION_WIDE {
        let nvars = u32::try_from(prepared.columns.len()).map_err(|_| {
            Error::ParseError("too many columns for Stata 119 header".to_string())
        })?;
        write_u32(writer, nvars)?;
    } else {
        let nvars = u16::try_from(prepared.columns.len()).map_err(|_| {
            Error::ParseError("too many columns for Stata 118 header".to_string())
        })?;
        write_u16(writer, nvars)?;
    }
    write_tag(writer, "</K>")?;
    write_tag(writer, "<N>")?;
    let nobs_offset = writer.stream_position()?;
    write_u64(writer, 0)?;
    write_tag(writer, "</N>")?;
    write_tag(writer, "<label>")?;
    write_u16(writer, 0)?;
    write_tag(writer, "</label>")?;
    write_tag(writer, "<timestamp>")?;
    write_u8(writer, 0)?;
    write_tag(writer, "</timestamp>")?;
    write_tag(writer, "</header>")?;

    let map_offset = writer.stream_position()?;
    write_tag(writer, "<map>")?;
    let map_values_offset = writer.stream_position()?;
    for _ in 0..14 {
        write_u64(writer, 0)?;
    }
    write_tag(writer, "</map>")?;

    Ok(HeaderPatchOffsets {
        nobs_offset,
        map_values_offset,
        map_offset,
    })
}

fn patch_header_and_map<W: Write + Seek>(
    writer: &mut W,
    prepared: &PreparedWrite,
    rows_written: usize,
    offsets: HeaderPatchOffsets,
) -> Result<()> {
    let rows_written_u64 = u64::try_from(rows_written)
        .map_err(|_| Error::ParseError("row_count exceeds u64".to_string()))?;
    let end_pos = writer.stream_position()?;

    writer.seek(SeekFrom::Start(offsets.nobs_offset))?;
    write_u64(writer, rows_written_u64)?;

    let mut patched = prepared.clone();
    patched.row_count = Some(rows_written);
    let map = build_map(&patched, offsets.map_offset);
    writer.seek(SeekFrom::Start(offsets.map_values_offset))?;
    for v in map {
        write_u64(writer, v)?;
    }

    writer.seek(SeekFrom::Start(end_pos))?;
    Ok(())
}

fn current_offset_after_header(dta_version: u16) -> u64 {
    let k_len = if dta_version >= DTA_VERSION_WIDE { 4 } else { 2 };
    let header_len = "<stata_dta>".len()
        + "<header>".len()
        + format!("<release>{}</release>", dta_version).len()
        + "<byteorder>".len()
        + "LSF".len()
        + "</byteorder>".len()
        + "<K>".len()
        + k_len
        + "</K>".len()
        + "<N>".len()
        + 8
        + "</N>".len()
        + "<label>".len()
        + 2
        + "</label>".len()
        + "<timestamp>".len()
        + 1
        + "</timestamp>".len()
        + "</header>".len();
    header_len as u64
}

fn build_map(prepared: &PreparedWrite, map_offset: u64) -> [u64; 14] {
    let mut map = [0u64; 14];
    let m_map = measure_map();
    let m_typlist = measure_typlist(prepared);
    let m_varlist = measure_varlist(prepared);
    let m_srtlist = measure_srtlist(prepared);
    let m_fmtlist = measure_fmtlist(prepared);
    let m_lbllist = measure_lbllist(prepared);
    let m_varlabels = measure_varlabels(prepared);
    let m_char = measure_characteristics();
    let m_data = measure_data(prepared);
    let m_strls = measure_strls(prepared);
    let m_val = measure_value_labels(prepared);
    map[0] = 0;
    map[1] = map_offset;
    map[2] = map[1] + m_map;
    map[3] = map[2] + m_typlist;
    map[4] = map[3] + m_varlist;
    map[5] = map[4] + m_srtlist;
    map[6] = map[5] + m_fmtlist;
    map[7] = map[6] + m_lbllist;
    map[8] = map[7] + m_varlabels;
    map[9] = map[8] + m_char;
    map[10] = map[9] + m_data;
    map[11] = map[10] + m_strls;
    map[12] = map[11] + m_val;
    map[13] = map[12] + "</stata_dta>".len() as u64;
    map
}

fn measure_map() -> u64 {
    ("<map>".len() + 14 * 8 + "</map>".len()) as u64
}

fn measure_typlist(prepared: &PreparedWrite) -> u64 {
    ("<variable_types>".len() + prepared.typlist.len() * 2 + "</variable_types>".len()) as u64
}

fn measure_varlist(prepared: &PreparedWrite) -> u64 {
    ("<varnames>".len() + prepared.varlist.len() + "</varnames>".len()) as u64
}

fn measure_srtlist(prepared: &PreparedWrite) -> u64 {
    ("<sortlist>".len() + prepared.srtlist_len + "</sortlist>".len()) as u64
}

fn measure_fmtlist(prepared: &PreparedWrite) -> u64 {
    ("<formats>".len() + prepared.fmtlist.len() + "</formats>".len()) as u64
}

fn measure_lbllist(prepared: &PreparedWrite) -> u64 {
    ("<value_label_names>".len() + prepared.lbllist.len() + "</value_label_names>".len()) as u64
}

fn measure_varlabels(prepared: &PreparedWrite) -> u64 {
    ("<variable_labels>".len() + prepared.var_labels.len() + "</variable_labels>".len()) as u64
}

fn measure_characteristics() -> u64 {
    ("<characteristics>".len() + "</characteristics>".len()) as u64
}

fn measure_data(prepared: &PreparedWrite) -> u64 {
    let rows = prepared.row_count.unwrap_or(0) as u64;
    ("<data>".len() as u64) + (prepared.record_len as u64) * rows + "</data>".len() as u64
}

fn measure_strls(prepared: &PreparedWrite) -> u64 {
    let mut len = 0u64;
    for entry in &prepared.strls {
        len += 3 + 17 + entry.data.len() as u64;
    }
    "<strls>".len() as u64 + len + "</strls>".len() as u64
}

fn measure_value_labels(prepared: &PreparedWrite) -> u64 {
    let mut len = "<value_labels>".len() + "</value_labels>".len();
    for table in &prepared.value_labels {
        len += "<lbl>".len();
        len += 4; // len field
        len += DTA_LBLLIST_ENTRY_LEN; // label name
        len += DTA_VALUE_LABEL_PADDING_LEN;
        len += table.table.len();
        len += "</lbl>".len();
    }
    len as u64
}

fn write_dta_descriptors<W: Write>(writer: &mut W, prepared: &PreparedWrite) -> Result<()> {
    write_tag(writer, "<variable_types>")?;
    for code in &prepared.typlist {
        write_u16(writer, *code)?;
    }
    write_tag(writer, "</variable_types>")?;

    write_tag(writer, "<varnames>")?;
    writer.write_all(&prepared.varlist)?;
    write_tag(writer, "</varnames>")?;

    write_tag(writer, "<sortlist>")?;
    writer.write_all(&vec![0u8; prepared.srtlist_len])?;
    write_tag(writer, "</sortlist>")?;

    write_tag(writer, "<formats>")?;
    writer.write_all(&prepared.fmtlist)?;
    write_tag(writer, "</formats>")?;

    write_tag(writer, "<value_label_names>")?;
    writer.write_all(&prepared.lbllist)?;
    write_tag(writer, "</value_label_names>")?;

    Ok(())
}

fn write_dta_variable_labels<W: Write>(writer: &mut W, prepared: &PreparedWrite) -> Result<()> {
    write_tag(writer, "<variable_labels>")?;
    writer.write_all(&prepared.var_labels)?;
    write_tag(writer, "</variable_labels>")?;
    Ok(())
}

fn write_dta_characteristics<W: Write>(writer: &mut W) -> Result<()> {
    write_tag(writer, "<characteristics>")?;
    write_tag(writer, "</characteristics>")?;
    Ok(())
}

fn write_dta_data_df_parallel<W: Write>(
    writer: &mut W,
    prepared: &PreparedWrite,
    df: &DataFrame,
    n_threads: Option<usize>,
) -> Result<()> {
    use rayon::prelude::*;

    write_tag(writer, "<data>")?;

    let mut cols: Vec<&Series> = Vec::with_capacity(prepared.columns.len());
    for col in &prepared.columns {
        let column = df.column(&col.name).map_err(Error::Polars)?;
        cols.push(column.as_materialized_series());
    }

    let nrows = df.height();
    let record_len = prepared.record_len;
    let chunk_rows = DTA_PARALLEL_CHUNK_ROWS.max(1);
    let n_chunks = (nrows + chunk_rows - 1) / chunk_rows;
    let col_widths: Vec<usize> = prepared
        .columns
        .iter()
        .map(storage_width_for_column)
        .collect();
    let mut col_offsets = Vec::with_capacity(prepared.columns.len());
    let mut acc = 0usize;
    for w in &col_widths {
        col_offsets.push(acc);
        acc += *w;
    }
    let build_buffers = || -> Result<Vec<(usize, Vec<u8>)>> {
        (0..n_chunks)
            .into_par_iter()
            .map(|chunk_idx| -> Result<(usize, Vec<u8>)> {
                let start = chunk_idx * chunk_rows;
                let end = (start + chunk_rows).min(nrows);
                let rows = end - start;
                let mut buf = vec![0u8; rows * record_len];
                for (row_offset, row_idx) in (start..end).enumerate() {
                    let base = row_offset * record_len;
                    let row_slice = &mut buf[base..base + record_len];
                    row_slice.fill(0);
                    for (col_idx, col_spec) in prepared.columns.iter().enumerate() {
                        let series = &cols[col_idx];
                        let offset = col_offsets[col_idx];
                        let width = col_widths[col_idx];
                        let cell = &mut row_slice[offset..offset + width];
                        write_cell(cell, series, col_spec, row_idx)?;
                    }
                }
                Ok((chunk_idx, buf))
            })
            .collect::<Result<Vec<_>>>()
    };

    let mut buffers = if let Some(n) = n_threads {
        if n == 0 {
            return Err(Error::ParseError("n_threads must be >= 1".to_string()));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build()
            .map_err(|e| Error::ParseError(format!("failed to build thread pool: {e}")))?;
        pool.install(build_buffers)?
    } else {
        build_buffers()?
    };

    buffers.sort_by_key(|(idx, _)| *idx);
    for (_, buf) in buffers {
        writer.write_all(&buf)?;
    }

    write_tag(writer, "</data>")?;
    Ok(())
}

fn write_dta_data_batches<W: Write, I>(
    writer: &mut W,
    prepared: &PreparedWrite,
    batches: I,
) -> Result<usize>
where
    I: IntoIterator<Item = DataFrame>,
{
    write_tag(writer, "<data>")?;
    let mut row_buf = vec![0u8; prepared.record_len];
    let mut rows_written = 0usize;
    for batch in batches {
        let mut cols: Vec<&Series> = Vec::with_capacity(prepared.columns.len());
        for col in &prepared.columns {
            let column = batch.column(&col.name).map_err(|e| Error::Polars(e))?;
            cols.push(column.as_materialized_series());
        }
        for row_idx in 0..batch.height() {
            row_buf.fill(0);
            let mut offset = 0usize;
            for (col_idx, col_spec) in prepared.columns.iter().enumerate() {
                let series = cols[col_idx];
                let width = storage_width_for_column(col_spec);
                let slice = &mut row_buf[offset..offset + width];
                write_cell(slice, series, col_spec, row_idx)?;
                offset += width;
            }
            writer.write_all(&row_buf)?;
            rows_written += 1;
        }
    }
    write_tag(writer, "</data>")?;
    if let Some(expected) = prepared.row_count {
        if expected != rows_written {
            return Err(Error::ParseError(
                "row_count mismatch for batch writing".to_string(),
            ));
        }
    }
    Ok(rows_written)
}

fn write_cell(buf: &mut [u8], series: &Series, spec: &ColumnSpec, row_idx: usize) -> Result<()> {
    match spec.kind {
        ColumnKind::Int8 => write_i8(buf, series, row_idx),
        ColumnKind::Int16 => write_i16(buf, series, row_idx),
        ColumnKind::Int32 => write_i32(buf, series, row_idx),
        ColumnKind::Float32 => write_f32(buf, series, row_idx),
        ColumnKind::Float64 => write_f64(buf, series, row_idx, spec),
        ColumnKind::Str { width } => write_str(buf, series, row_idx, width),
        ColumnKind::StrL => write_strl_ref(buf, series, row_idx, spec),
    }
}

fn write_i8(buf: &mut [u8], series: &Series, row_idx: usize) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        buf[0] = DTA_113_MISSING_INT8 as u8;
        return Ok(());
    }
    let v = anyvalue_to_i64(value).ok_or(Error::NumericOutOfRange)?;
    if v > DTA_113_MAX_INT8 as i64 || v < DTA_113_MIN_INT8 as i64 {
        return Err(Error::NumericOutOfRange);
    }
    buf[0] = (v as i8) as u8;
    Ok(())
}

fn write_i16(buf: &mut [u8], series: &Series, row_idx: usize) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        buf[..2].copy_from_slice(&DTA_113_MISSING_INT16.to_le_bytes());
        return Ok(());
    }
    let v = anyvalue_to_i64(value).ok_or(Error::NumericOutOfRange)?;
    if v > DTA_113_MAX_INT16 as i64 || v < DTA_113_MIN_INT16 as i64 {
        return Err(Error::NumericOutOfRange);
    }
    buf[..2].copy_from_slice(&(v as i16).to_le_bytes());
    Ok(())
}

fn write_i32(buf: &mut [u8], series: &Series, row_idx: usize) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        buf[..4].copy_from_slice(&DTA_113_MISSING_INT32.to_le_bytes());
        return Ok(());
    }
    let mut v = anyvalue_to_i64(value).ok_or(Error::NumericOutOfRange)?;
    if matches!(series.dtype(), DataType::Date) {
        v += 3653;
    }
    if v > DTA_113_MAX_INT32 as i64 || v < DTA_113_MIN_INT32 as i64 {
        return Err(Error::NumericOutOfRange);
    }
    buf[..4].copy_from_slice(&(v as i32).to_le_bytes());
    Ok(())
}

fn write_f32(buf: &mut [u8], series: &Series, row_idx: usize) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        buf[..4].copy_from_slice(&DTA_113_MISSING_FLOAT.to_le_bytes());
        return Ok(());
    }
    let v = anyvalue_to_f64(value).unwrap_or(0.0) as f32;
    let max_flt = f32::from_bits(DTA_113_MAX_FLOAT);
    if v > max_flt {
        return Err(Error::NumericOutOfRange);
    }
    buf[..4].copy_from_slice(&v.to_bits().to_le_bytes());
    Ok(())
}

fn write_f64(buf: &mut [u8], series: &Series, row_idx: usize, spec: &ColumnSpec) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        buf[..8].copy_from_slice(&DTA_113_MISSING_DOUBLE.to_le_bytes());
        return Ok(());
    }
    let mut v = anyvalue_to_f64(value).unwrap_or(0.0);
    if matches!(spec.dtype, DataType::Datetime(_, _)) {
        let unit = match spec.dtype {
            DataType::Datetime(unit, _) => unit,
            _ => TimeUnit::Milliseconds,
        };
        let offset_ms = 3653i64 * 86_400_000i64;
        let base = match unit {
            TimeUnit::Milliseconds => v as i64,
            TimeUnit::Microseconds => (v as i64) / 1_000,
            TimeUnit::Nanoseconds => (v as i64) / 1_000_000,
        };
        v = (base + offset_ms) as f64;
    } else if matches!(spec.dtype, DataType::Time) {
        v = (v as i64 / 1_000_000) as f64;
    }
    let max_dbl = f64::from_bits(DTA_113_MAX_DOUBLE);
    if v > max_dbl {
        return Err(Error::NumericOutOfRange);
    }
    buf[..8].copy_from_slice(&v.to_bits().to_le_bytes());
    Ok(())
}

fn write_str(buf: &mut [u8], series: &Series, row_idx: usize, width: usize) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        return Ok(());
    }
    let s = anyvalue_to_str(&value).unwrap_or("");
    let bytes = s.as_bytes();
    if bytes.len() > width {
        return Err(Error::StringTooLong(series.name().to_string()));
    }
    buf[..bytes.len()].copy_from_slice(bytes);
    Ok(())
}

fn write_strl_ref(
    buf: &mut [u8],
    series: &Series,
    row_idx: usize,
    spec: &ColumnSpec,
) -> Result<()> {
    let value = series.get(row_idx)?;
    if anyvalue_is_null(&value) {
        buf.fill(0);
        return Ok(());
    }
    let v = (spec.index + 1) as u16;
    let o = (row_idx + 1) as u64;
    buf[..2].copy_from_slice(&v.to_le_bytes());
    let o_bytes = o.to_le_bytes();
    buf[2..8].copy_from_slice(&o_bytes[..6]);
    Ok(())
}

fn anyvalue_is_null(v: &AnyValue) -> bool {
    matches!(v, AnyValue::Null)
}

fn anyvalue_to_i64(v: AnyValue) -> Option<i64> {
    match v {
        AnyValue::Int8(v) => Some(v as i64),
        AnyValue::Int16(v) => Some(v as i64),
        AnyValue::Int32(v) => Some(v as i64),
        AnyValue::Int64(v) => Some(v),
        AnyValue::UInt8(v) => Some(v as i64),
        AnyValue::UInt16(v) => Some(v as i64),
        AnyValue::UInt32(v) => Some(v as i64),
        AnyValue::UInt64(v) => {
            if v > i64::MAX as u64 {
                None
            } else {
                Some(v as i64)
            }
        }
        AnyValue::Boolean(v) => Some(if v { 1 } else { 0 }),
        AnyValue::Date(v) => Some(v as i64),
        _ => None,
    }
}

fn anyvalue_to_f64(v: AnyValue) -> Option<f64> {
    match v {
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Float64(v) => Some(v),
        AnyValue::Int8(v) => Some(v as f64),
        AnyValue::Int16(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::UInt8(v) => Some(v as f64),
        AnyValue::UInt16(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::Boolean(v) => Some(if v { 1.0 } else { 0.0 }),
        AnyValue::Date(v) => Some(v as f64),
        AnyValue::Datetime(v, _, _) => Some(v as f64),
        AnyValue::DatetimeOwned(v, _, _) => Some(v as f64),
        AnyValue::Time(v) => Some(v as f64),
        _ => None,
    }
}

fn anyvalue_to_str<'a>(v: &'a AnyValue<'a>) -> Option<&'a str> {
    match v {
        AnyValue::String(s) => Some(*s),
        _ => None,
    }
}

fn target_dtype_for_series(series: &Series) -> Result<DataType> {
    match series.dtype() {
        DataType::Boolean => Ok(DataType::Int8),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let (min, max, overflow) = min_max_i64(series)?;
            if overflow {
                return Ok(DataType::Float64);
            }
            if min >= DTA_113_MIN_INT8 as i64 && max <= DTA_113_MAX_INT8 as i64 {
                Ok(DataType::Int8)
            } else if min >= DTA_113_MIN_INT16 as i64 && max <= DTA_113_MAX_INT16 as i64 {
                Ok(DataType::Int16)
            } else if min >= DTA_113_MIN_INT32 as i64 && max <= DTA_113_MAX_INT32 as i64 {
                Ok(DataType::Int32)
            } else {
                Ok(DataType::Float64)
            }
        }
        DataType::Float32 => Ok(DataType::Float32),
        DataType::Float64 => Ok(DataType::Float64),
        _ => Ok(series.dtype().clone()),
    }
}

fn min_max_i64(series: &Series) -> Result<(i64, i64, bool)> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut overflow = false;
    for idx in 0..series.len() {
        let value = series.get(idx)?;
        if anyvalue_is_null(&value) {
            continue;
        }
        match value {
            AnyValue::UInt64(v) if v > i64::MAX as u64 => {
                overflow = true;
                continue;
            }
            AnyValue::Int128(_) => {
                overflow = true;
                continue;
            }
            _ => {}
        }
        if let Some(v) = anyvalue_to_i64(value) {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
    }
    if min == i64::MAX {
        Ok((0, 0, overflow))
    } else {
        Ok((min, max, overflow))
    }
}


fn write_dta_strls<W: Write>(writer: &mut W, prepared: &PreparedWrite) -> Result<()> {
    write_tag(writer, "<strls>")?;
    for entry in &prepared.strls {
        write_string(writer, "GSO")?;
        write_u32(writer, entry.v)?;
        write_u64(writer, entry.o)?;
        write_u8(writer, 0x82)?;
        write_i32_le(writer, entry.data.len() as i32)?;
        writer.write_all(&entry.data)?;
    }
    write_tag(writer, "</strls>")?;
    Ok(())
}

fn write_dta_value_labels<W: Write>(writer: &mut W, prepared: &PreparedWrite) -> Result<()> {
    write_tag(writer, "<value_labels>")?;
    for table in &prepared.value_labels {
        write_tag(writer, "<lbl>")?;
        write_u32(writer, table.table.len() as u32)?;
        write_label_name(writer, &table.name)?;
        writer.write_all(&vec![0u8; DTA_VALUE_LABEL_PADDING_LEN])?;
        writer.write_all(&table.table)?;
        write_tag(writer, "</lbl>")?;
    }
    write_tag(writer, "</value_labels>")?;
    Ok(())
}

fn write_dta_footer<W: Write>(writer: &mut W) -> Result<()> {
    write_tag(writer, "</stata_dta>")?;
    Ok(())
}

fn write_tag<W: Write>(writer: &mut W, tag: &str) -> Result<()> {
    writer.write_all(tag.as_bytes())?;
    Ok(())
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<()> {
    writer.write_all(s.as_bytes())?;
    Ok(())
}

fn write_label_name<W: Write>(writer: &mut W, name: &str) -> Result<()> {
    let mut buf = vec![0u8; DTA_LBLLIST_ENTRY_LEN];
    let bytes = name.as_bytes();
    let copy_len = bytes.len().min(DTA_LBLLIST_ENTRY_LEN);
    buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
    writer.write_all(&buf)?;
    Ok(())
}

fn write_u8<W: Write>(writer: &mut W, v: u8) -> Result<()> {
    writer.write_all(&[v])?;
    Ok(())
}

fn write_u16<W: Write>(writer: &mut W, v: u16) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u32<W: Write>(writer: &mut W, v: u32) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u64<W: Write>(writer: &mut W, v: u64) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_i32_le<W: Write>(writer: &mut W, v: i32) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn validate_column_names_unique(cols: &[ColumnSpec]) -> Result<()> {
    let mut seen = HashSet::new();
    for c in cols {
        let name = c.name.to_ascii_lowercase();
        if !seen.insert(name) {
            return Err(Error::InvalidName(c.name.clone()));
        }
    }
    Ok(())
}
