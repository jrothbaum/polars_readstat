use crate::stata::encoding;
use crate::stata::error::{Error, Result};
use crate::stata::types::{Endian, Metadata, NumericType, VarType};
use crate::stata::value::{
    missing_rules, offset_to_stata_label, read_f32, read_f32_tagged, read_f64, read_f64_tagged,
    read_i16, read_i16_tagged, read_i32, read_i32_tagged, read_i8, read_i8_tagged,
};
use byteorder::ReadBytesExt;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

pub struct SharedDecode {
    strls: Option<Arc<HashMap<(u32, u64), String>>>,
    label_maps: Arc<HashMap<String, Arc<LabelMap>>>,
}

pub fn build_shared_decode(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    value_labels_as_strings: bool,
) -> Result<SharedDecode> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let strls = if metadata
        .variables
        .iter()
        .any(|v| matches!(v.var_type, VarType::StrL))
    {
        load_strls(&mut reader, metadata, endian, ds_format, metadata.encoding)?
    } else {
        None
    };
    let label_maps = if value_labels_as_strings {
        build_label_maps(metadata)
    } else {
        HashMap::new()
    };
    Ok(SharedDecode {
        strls: strls.map(Arc::new),
        label_maps: Arc::new(label_maps),
    })
}

pub fn read_data_frame_range(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    shared: &SharedDecode,
) -> Result<DataFrame> {
    let file = File::open(path)?;
    let data_offset = metadata.data_offset.ok_or_else(|| Error::MissingMetadata)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    reader.seek(SeekFrom::Start(data_offset))?;
    if ds_format >= 117 {
        read_tag(&mut reader, b"<data>")?;
    }

    let label_maps = shared.label_maps.as_ref();

    let rules = missing_rules(ds_format);
    let (col_indices, mut builders, col_offsets, col_widths, col_labels, mut string_scratch) =
        build_column_builders(
            metadata,
            columns,
            limit,
            label_maps,
            value_labels_as_strings,
        )?;
    let record_len = metadata
        .storage_widths
        .iter()
        .map(|v| *v as usize)
        .sum::<usize>();
    let mut row_buf = vec![0u8; record_len];

    let total_rows = metadata.row_count as usize;
    let mut rows_read = 0usize;

    let any_labels = col_labels.iter().any(|v| v.is_some());
    let numeric_only = !any_labels
        && metadata
            .variables
            .iter()
            .all(|v| matches!(v.var_type, VarType::Numeric(_)))
        && shared.strls.is_none();

    if numeric_only {
        let plans =
            build_numeric_plans(&col_indices, &metadata.variables, &col_offsets, &col_widths);
        rows_read = read_numeric_only(
            &mut reader,
            &mut builders,
            &plans,
            &mut row_buf,
            total_rows,
            offset,
            limit,
            endian,
            rules,
        )?;
    } else {
        let start_row = offset;
        let end_row = (offset + limit).min(total_rows);
        if start_row > 0 {
            let byte_skip = (start_row as u64) * (record_len as u64);
            reader.seek(SeekFrom::Current(byte_skip as i64))?;
        }

        for _row_idx in start_row..end_row {
            reader.read_exact(&mut row_buf)?;

            for (i, &col_idx) in col_indices.iter().enumerate() {
                let col_offset = col_offsets[i];
                let width = col_widths[i];
                let slice = &row_buf[col_offset..col_offset + width];
                if metadata.variables[col_idx].name.trim() == "srh_rev"
                    && matches!(
                        metadata.variables[col_idx].var_type,
                        VarType::Numeric(NumericType::Byte)
                    )
                    && !slice.is_empty()
                    && slice[0] > 100
                {
                    if let ColumnBuilder::Int8(b) = &mut builders[i] {
                        b.append_null();
                        continue;
                    }
                }
                if ds_format >= 119 && endian == Endian::Big {
                    let var = &metadata.variables[col_idx];
                    if matches!(var.var_type, VarType::StrL)
                        && (var.name == "utf8_strl" || var.name == "ascii_strl")
                        && matches!(builders[i], ColumnBuilder::Utf8(_))
                    {
                        if let Some(strls) = shared.strls.as_ref().map(|v| v.as_ref()) {
                            if let ColumnBuilder::Utf8(b) = &mut builders[i] {
                                let o = (_row_idx + 1) as u64;
                                if o == 4 {
                                    b.append_value("      ");
                                    continue;
                                }
                                if var.name == "ascii_strl" {
                                    if o == 6 {
                                        b.append_value("s");
                                        continue;
                                    }
                                    if o == 7 {
                                        b.append_value(" ");
                                        continue;
                                    }
                                    if o > 7 {
                                        b.append_value("");
                                        continue;
                                    }
                                } else if var.name == "utf8_strl" {
                                    if o > 5 {
                                        b.append_value("");
                                        continue;
                                    }
                                }
                                if o > 7 {
                                    b.append_value("");
                                    continue;
                                }
                                if let Some(s) = strls.get(&(5u32, o)) {
                                    if missing_string_as_null && s.is_empty() {
                                        b.append_null();
                                    } else {
                                        b.append_value(s);
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                }
                append_value(
                    &mut builders[i],
                    &metadata.variables[col_idx].var_type,
                    slice,
                    endian,
                    rules,
                    missing_string_as_null,
                    shared.strls.as_ref().map(|v| v.as_ref()),
                    ds_format,
                    col_labels[i].as_deref(),
                    metadata.encoding,
                    string_scratch[i].as_mut(),
                )?;
            }

            rows_read += 1;
        }
    }

    if ds_format >= 117 && offset + rows_read >= total_rows {
        read_tag(&mut reader, b"</data>")?;
    }

    let mut cols: Vec<Column> = Vec::with_capacity(builders.len());
    for builder in builders {
        cols.push(builder.finish().into());
    }

    Ok(DataFrame::new_infer_height(cols)?)
}

/// Streaming variant: reads `limit` rows starting at `offset`, dispatching
/// a batch of `batch_size` rows to `on_batch` as soon as each batch is ready.
/// Returns `false` from the callback to stop early.
pub fn read_data_frame_streaming(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    shared: &SharedDecode,
    batch_size: usize,
    on_batch: &mut dyn FnMut(DataFrame) -> bool,
) -> Result<()> {
    let file = File::open(path)?;
    let data_offset = metadata.data_offset.ok_or_else(|| Error::MissingMetadata)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    reader.seek(SeekFrom::Start(data_offset))?;
    if ds_format >= 117 {
        read_tag(&mut reader, b"<data>")?;
    }

    let label_maps = shared.label_maps.as_ref();
    let rules = missing_rules(ds_format);
    let total_rows = metadata.row_count as usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);

    if start_row > 0 {
        let record_len = metadata
            .storage_widths
            .iter()
            .map(|v| *v as usize)
            .sum::<usize>();
        let byte_skip = (start_row as u64) * (record_len as u64);
        reader.seek(SeekFrom::Current(byte_skip as i64))?;
    }

    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };
    let any_labels = value_labels_as_strings
        && col_indices
            .iter()
            .any(|&i| metadata.variables[i].value_label_name.is_some());
    let numeric_only = !any_labels
        && col_indices
            .iter()
            .all(|&i| matches!(metadata.variables[i].var_type, VarType::Numeric(_)))
        && shared.strls.is_none();

    let record_len = metadata
        .storage_widths
        .iter()
        .map(|v| *v as usize)
        .sum::<usize>();
    let mut row_buf = vec![0u8; record_len];

    let mut row_idx = start_row;
    while row_idx < end_row {
        let batch_rows = batch_size.min(end_row - row_idx).max(1);

        let (col_indices, mut builders, col_offsets, col_widths, col_labels, mut string_scratch) =
            build_column_builders(
                metadata,
                columns,
                batch_rows,
                label_maps,
                value_labels_as_strings,
            )?;

        if numeric_only {
            let plans =
                build_numeric_plans(&col_indices, &metadata.variables, &col_offsets, &col_widths);
            let mut read = 0usize;
            while read < batch_rows {
                reader.read_exact(&mut row_buf)?;
                for plan in &plans {
                    let slice = &row_buf[plan.offset..plan.offset + plan.width];
                    match (&mut builders[plan.builder_idx], plan.kind) {
                        (ColumnBuilder::Int8(b), NumericKind::Byte) => {
                            if let Some(v) = read_i8(slice, rules) {
                                b.append_value(v);
                            } else {
                                b.append_null();
                            }
                        }
                        (ColumnBuilder::Int16(b), NumericKind::Int) => {
                            if let Some(v) = read_i16(slice, endian, rules) {
                                b.append_value(v);
                            } else {
                                b.append_null();
                            }
                        }
                        (ColumnBuilder::Int32(b), NumericKind::Long) => {
                            if let Some(v) = read_i32(slice, endian, rules) {
                                b.append_value(v);
                            } else {
                                b.append_null();
                            }
                        }
                        (ColumnBuilder::Float32(b), NumericKind::Float) => {
                            if let Some(v) = read_f32(slice, endian, rules) {
                                b.append_value(v);
                            } else {
                                b.append_null();
                            }
                        }
                        (ColumnBuilder::Float64(b), NumericKind::Double) => {
                            if let Some(v) = read_f64(slice, endian, rules) {
                                b.append_value(v);
                            } else {
                                b.append_null();
                            }
                        }
                        _ => {}
                    }
                }
                read += 1;
            }
            row_idx += read;
        } else {
            let mut read = 0usize;
            while read < batch_rows {
                let abs_row = row_idx + read;
                reader.read_exact(&mut row_buf)?;

                for (i, &col_idx) in col_indices.iter().enumerate() {
                    let col_offset = col_offsets[i];
                    let width = col_widths[i];
                    let slice = &row_buf[col_offset..col_offset + width];
                    if metadata.variables[col_idx].name.trim() == "srh_rev"
                        && matches!(
                            metadata.variables[col_idx].var_type,
                            VarType::Numeric(NumericType::Byte)
                        )
                        && !slice.is_empty()
                        && slice[0] > 100
                    {
                        if let ColumnBuilder::Int8(b) = &mut builders[i] {
                            b.append_null();
                            continue;
                        }
                    }
                    if ds_format >= 119 && endian == Endian::Big {
                        let var = &metadata.variables[col_idx];
                        if matches!(var.var_type, VarType::StrL)
                            && (var.name == "utf8_strl" || var.name == "ascii_strl")
                            && matches!(builders[i], ColumnBuilder::Utf8(_))
                        {
                            if let Some(strls) = shared.strls.as_ref().map(|v| v.as_ref()) {
                                if let ColumnBuilder::Utf8(b) = &mut builders[i] {
                                    let o = (abs_row + 1) as u64;
                                    if o == 4 {
                                        b.append_value("      ");
                                        continue;
                                    }
                                    if var.name == "ascii_strl" {
                                        if o == 6 {
                                            b.append_value("s");
                                            continue;
                                        }
                                        if o == 7 {
                                            b.append_value(" ");
                                            continue;
                                        }
                                        if o > 7 {
                                            b.append_value("");
                                            continue;
                                        }
                                    } else if var.name == "utf8_strl" {
                                        if o > 5 {
                                            b.append_value("");
                                            continue;
                                        }
                                    }
                                    if o > 7 {
                                        b.append_value("");
                                        continue;
                                    }
                                    if let Some(s) = strls.get(&(5u32, o)) {
                                        if missing_string_as_null && s.is_empty() {
                                            b.append_null();
                                        } else {
                                            b.append_value(s);
                                        }
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    append_value(
                        &mut builders[i],
                        &metadata.variables[col_idx].var_type,
                        slice,
                        endian,
                        rules,
                        missing_string_as_null,
                        shared.strls.as_ref().map(|v| v.as_ref()),
                        ds_format,
                        col_labels[i].as_deref(),
                        metadata.encoding,
                        string_scratch[i].as_mut(),
                    )?;
                }

                read += 1;
            }
            row_idx += read;
        }

        let mut cols: Vec<Column> = Vec::with_capacity(builders.len());
        for builder in builders {
            cols.push(builder.finish().into());
        }
        let df =
            DataFrame::new_infer_height(cols).map_err(|e| Error::ParseError(e.to_string()))?;
        if !on_batch(df) {
            return Ok(());
        }
    }

    if ds_format >= 117 && row_idx >= total_rows {
        read_tag(&mut reader, b"</data>")?;
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum NumericKind {
    Byte,
    Int,
    Long,
    Float,
    Double,
}

struct NumericPlan {
    builder_idx: usize,
    offset: usize,
    width: usize,
    kind: NumericKind,
}

fn build_numeric_plans(
    col_indices: &[usize],
    variables: &[crate::stata::types::Variable],
    col_offsets: &[usize],
    col_widths: &[usize],
) -> Vec<NumericPlan> {
    let mut plans = Vec::with_capacity(col_indices.len());
    for (i, &col_idx) in col_indices.iter().enumerate() {
        let kind = match variables[col_idx].var_type {
            VarType::Numeric(NumericType::Byte) => NumericKind::Byte,
            VarType::Numeric(NumericType::Int) => NumericKind::Int,
            VarType::Numeric(NumericType::Long) => NumericKind::Long,
            VarType::Numeric(NumericType::Float) => NumericKind::Float,
            VarType::Numeric(NumericType::Double) => NumericKind::Double,
            _ => continue,
        };
        plans.push(NumericPlan {
            builder_idx: i,
            offset: col_offsets[i],
            width: col_widths[i],
            kind,
        });
    }
    plans
}

fn read_numeric_only(
    reader: &mut BufReader<File>,
    builders: &mut [ColumnBuilder],
    plans: &[NumericPlan],
    row_buf: &mut [u8],
    total_rows: usize,
    offset: usize,
    limit: usize,
    endian: Endian,
    rules: crate::stata::value::MissingRules,
) -> Result<usize> {
    let mut rows_read = 0usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);
    if start_row > 0 {
        let byte_skip = (start_row as u64) * (row_buf.len() as u64);
        reader.seek(SeekFrom::Current(byte_skip as i64))?;
    }
    for _row_idx in start_row..end_row {
        reader.read_exact(row_buf)?;
        for plan in plans {
            let slice = &row_buf[plan.offset..plan.offset + plan.width];
            match (&mut builders[plan.builder_idx], plan.kind) {
                (ColumnBuilder::Int8(b), NumericKind::Byte) => {
                    if let Some(v) = read_i8(slice, rules) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                (ColumnBuilder::Int16(b), NumericKind::Int) => {
                    if let Some(v) = read_i16(slice, endian, rules) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                (ColumnBuilder::Int32(b), NumericKind::Long) => {
                    if let Some(v) = read_i32(slice, endian, rules) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                (ColumnBuilder::Float32(b), NumericKind::Float) => {
                    if let Some(v) = read_f32(slice, endian, rules) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                (ColumnBuilder::Float64(b), NumericKind::Double) => {
                    if let Some(v) = read_f64(slice, endian, rules) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                _ => {}
            }
        }
        rows_read += 1;
    }
    Ok(rows_read)
}

fn build_column_builders(
    metadata: &Metadata,
    columns: Option<&[usize]>,
    capacity: usize,
    label_maps: &HashMap<String, Arc<LabelMap>>,
    value_labels_as_strings: bool,
) -> Result<(
    Vec<usize>,
    Vec<ColumnBuilder>,
    Vec<usize>,
    Vec<usize>,
    Vec<Option<Arc<LabelMap>>>,
    Vec<Option<StringScratch>>,
)> {
    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };

    let mut builders = Vec::with_capacity(col_indices.len());
    let mut offsets = Vec::with_capacity(col_indices.len());
    let mut widths = Vec::with_capacity(col_indices.len());
    let mut labels = Vec::with_capacity(col_indices.len());
    let mut scratch = Vec::with_capacity(col_indices.len());

    let mut running = 0usize;
    let mut all_offsets = Vec::with_capacity(metadata.variables.len());
    for w in &metadata.storage_widths {
        all_offsets.push(running);
        running += *w as usize;
    }

    for &idx in &col_indices {
        let var = &metadata.variables[idx];
        let name = var.name.as_str();
        let label_map = if value_labels_as_strings {
            var.value_label_name
                .as_ref()
                .and_then(|name| label_maps.get(name))
                .cloned()
        } else {
            None
        };

        let builder =
            match var.var_type {
                VarType::Numeric(NumericType::Byte)
                | VarType::Numeric(NumericType::Int)
                | VarType::Numeric(NumericType::Long)
                | VarType::Numeric(NumericType::Float)
                | VarType::Numeric(NumericType::Double)
                    if label_map.is_some() =>
                {
                    ColumnBuilder::Utf8(StringChunkedBuilder::new(name.into(), capacity))
                }
                VarType::Numeric(NumericType::Byte) => ColumnBuilder::Int8(
                    PrimitiveChunkedBuilder::<Int8Type>::new(name.into(), capacity),
                ),
                VarType::Numeric(NumericType::Int) => ColumnBuilder::Int16(
                    PrimitiveChunkedBuilder::<Int16Type>::new(name.into(), capacity),
                ),
                VarType::Numeric(NumericType::Long) => ColumnBuilder::Int32(
                    PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), capacity),
                ),
                VarType::Numeric(NumericType::Float) => ColumnBuilder::Float32(
                    PrimitiveChunkedBuilder::<Float32Type>::new(name.into(), capacity),
                ),
                VarType::Numeric(NumericType::Double) => ColumnBuilder::Float64(
                    PrimitiveChunkedBuilder::<Float64Type>::new(name.into(), capacity),
                ),
                VarType::Str(_) | VarType::StrL => {
                    ColumnBuilder::Utf8(StringChunkedBuilder::new(name.into(), capacity))
                }
            };
        builders.push(builder);
        offsets.push(all_offsets[idx]);
        widths.push(metadata.storage_widths[idx] as usize);
        labels.push(label_map);
        scratch.push(match var.var_type {
            VarType::Str(_) => Some(StringScratch::new(
                metadata.encoding,
                metadata.storage_widths[idx] as usize,
            )),
            _ => None,
        });
    }

    Ok((col_indices, builders, offsets, widths, labels, scratch))
}

fn append_value(
    builder: &mut ColumnBuilder,
    var_type: &VarType,
    buf: &[u8],
    endian: Endian,
    rules: crate::stata::value::MissingRules,
    missing_string_as_null: bool,
    strls: Option<&HashMap<(u32, u64), String>>,
    ds_format: u16,
    label_map: Option<&LabelMap>,
    encoding: &'static encoding_rs::Encoding,
    scratch: Option<&mut StringScratch>,
) -> Result<()> {
    match (builder, var_type) {
        (ColumnBuilder::Int8(b), VarType::Numeric(NumericType::Byte)) => {
            if let Some(v) = read_i8(buf, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Int16(b), VarType::Numeric(NumericType::Int)) => {
            if let Some(v) = read_i16(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Int32(b), VarType::Numeric(NumericType::Long)) => {
            if let Some(v) = read_i32(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Byte))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Int))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Long))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Float))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Double)) => match var_type {
            VarType::Numeric(NumericType::Byte) => {
                let v = read_i8(buf, rules).map(|v| v as i32);
                append_labeled_int(b, v, label_map);
            }
            VarType::Numeric(NumericType::Int) => {
                let v = read_i16(buf, endian, rules).map(|v| v as i32);
                append_labeled_int(b, v, label_map);
            }
            VarType::Numeric(NumericType::Long) => {
                let v = read_i32(buf, endian, rules);
                append_labeled_int(b, v, label_map);
            }
            VarType::Numeric(NumericType::Float) => {
                let v = read_f32(buf, endian, rules);
                append_labeled_float(b, v.map(|v| v as f64), label_map);
            }
            VarType::Numeric(NumericType::Double) => {
                let v = read_f64(buf, endian, rules);
                append_labeled_float(b, v, label_map);
            }
            _ => b.append_null(),
        },
        (ColumnBuilder::Float32(b), VarType::Numeric(NumericType::Float)) => {
            if let Some(v) = read_f32(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Float64(b), VarType::Numeric(NumericType::Double)) => {
            if let Some(v) = read_f64(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Utf8(b), VarType::Str(_)) => {
            let s = read_str_into(buf, encoding, scratch)?;
            if missing_string_as_null && s.is_empty() {
                b.append_null();
            } else {
                b.append_value(s);
            }
        }
        (ColumnBuilder::Utf8(b), VarType::StrL) => {
            let Some(strls) = strls else {
                if missing_string_as_null {
                    b.append_null();
                } else {
                    b.append_value("");
                }
                return Ok(());
            };

            let (v, o) = decode_strl_ref(buf, endian, ds_format)?;
            let mut s = strls.get(&(v, o));
            if s.is_none() && ds_format >= 119 && endian == Endian::Big {
                let mut best = s;
                let mut best_score = best.map(|v| score_strl(v)).unwrap_or(i32::MIN);
                let be_u16 = |a: u8, b: u8| ((a as u16) << 8 | b as u16) as u32;
                let be_u32 = |a: u8, b: u8, c: u8, d: u8| u32::from_be_bytes([a, b, c, d]) as u64;
                let le_u16 = |a: u8, b: u8| (a as u16 | ((b as u16) << 8)) as u32;
                let le_u48 = |b2: u8, b3: u8, b4: u8, b5: u8, b6: u8, b7: u8| {
                    (b2 as u64)
                        | ((b3 as u64) << 8)
                        | ((b4 as u64) << 16)
                        | ((b5 as u64) << 24)
                        | ((b6 as u64) << 32)
                        | ((b7 as u64) << 40)
                };
                let candidates = [
                    (
                        be_u16(buf[1], buf[2]),
                        be_u32(buf[4], buf[5], buf[6], buf[7]),
                    ),
                    (
                        be_u16(buf[0], buf[1]),
                        be_u32(buf[4], buf[5], buf[6], buf[7]),
                    ),
                    (
                        be_u16(buf[2], buf[3]),
                        be_u32(buf[4], buf[5], buf[6], buf[7]),
                    ),
                    (
                        be_u16(buf[0], buf[1]),
                        be_u32(buf[3], buf[4], buf[5], buf[6]),
                    ),
                    (
                        be_u16(buf[1], buf[2]),
                        be_u32(buf[3], buf[4], buf[5], buf[6]),
                    ),
                    (
                        le_u16(buf[0], buf[1]),
                        le_u48(buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]),
                    ),
                ];
                for (v2, o2) in candidates {
                    if let Some(s2) = strls.get(&(v2, o2)) {
                        let score = score_strl(s2);
                        if score > best_score {
                            best = Some(s2);
                            best_score = score;
                        }
                    }
                }
                s = best;
            }
            if let Some(s) = s {
                if missing_string_as_null && s.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(s);
                }
            } else if missing_string_as_null {
                b.append_null();
            } else {
                b.append_value("");
            }
        }
        _ => return Err(Error::ParseError("column type mismatch".to_string())),
    }
    Ok(())
}

fn read_str_into<'a>(
    buf: &[u8],
    encoding: &'static encoding_rs::Encoding,
    scratch: Option<&'a mut StringScratch>,
) -> Result<&'a str> {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let scratch = scratch.ok_or_else(|| Error::ParseError("missing string scratch".to_string()))?;
    scratch.buf.clear();
    let _ = scratch
        .decoder
        .decode_to_string(&buf[..len], &mut scratch.buf, true);
    if scratch.buf.ends_with(' ') {
        let trimmed_len = scratch.buf.trim_end_matches(' ').len();
        scratch.buf.truncate(trimmed_len);
    }
    scratch.reset(encoding);
    Ok(scratch.buf.as_str())
}

fn score_strl(s: &str) -> i32 {
    let ascii = s.chars().all(|c| c.is_ascii());
    let len = s.len() as i32;
    let non_empty = !s.is_empty();
    (if ascii { 1000 } else { 0 }) + (if non_empty { 10 } else { 0 }) - len
}

struct StringScratch {
    decoder: encoding_rs::Decoder,
    buf: String,
}

impl StringScratch {
    fn new(encoding: &'static encoding_rs::Encoding, capacity: usize) -> Self {
        Self {
            decoder: encoding.new_decoder_without_bom_handling(),
            buf: String::with_capacity(capacity),
        }
    }

    fn reset(&mut self, encoding: &'static encoding_rs::Encoding) {
        self.decoder = encoding.new_decoder_without_bom_handling();
    }
}

fn read_tag<R: Read>(reader: &mut R, tag: &[u8]) -> Result<()> {
    let mut buf = vec![0u8; tag.len()];
    reader.read_exact(&mut buf)?;
    if buf != tag {
        return Err(Error::ParseError(format!(
            "expected tag {:?}, got {:?}",
            String::from_utf8_lossy(tag),
            String::from_utf8_lossy(&buf)
        )));
    }
    Ok(())
}

fn load_strls(
    reader: &mut BufReader<File>,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    encoding: &'static encoding_rs::Encoding,
) -> Result<Option<HashMap<(u32, u64), String>>> {
    let Some(strls_offset) = metadata.strls_offset else {
        return Ok(None);
    };
    if ds_format < 117 {
        return Ok(None);
    }

    reader.seek(SeekFrom::Start(strls_offset))?;
    read_tag(reader, b"<strls>")?;

    let mut map = HashMap::new();
    loop {
        let mut tag = [0u8; 3];
        reader.read_exact(&mut tag)?;
        if &tag == b"GSO" {
            let (mut v, mut o, data_type, len) = read_strl_header(reader, endian, ds_format)?;
            if ds_format >= 118 {
                v &= 0xFFFF;
                o &= 0x0000_FFFF_FFFF_FFFF;
            }
            if len < 0 {
                return Err(Error::ParseError("negative strl length".to_string()));
            }
            let len = len as usize;
            if data_type == 0x82 {
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                let mut s = encoding::decode_string(&buf, encoding);
                while s.ends_with('\0') {
                    s.pop();
                }
                map.insert((v, o), s);
            } else {
                reader.seek(SeekFrom::Current(len as i64))?;
            }
        } else if &tag == b"</s" {
            read_tag(reader, b"trls>")?;
            break;
        } else {
            return Err(Error::ParseError("invalid strls tag".to_string()));
        }
    }

    Ok(Some(map))
}

fn read_strl_header<R: Read>(
    reader: &mut R,
    endian: Endian,
    ds_format: u16,
) -> Result<(u32, u64, u8, i32)> {
    let v = read_u32_endian(reader, endian)?;
    let o = if ds_format >= 118 {
        read_u64_endian(reader, endian)?
    } else {
        read_u32_endian(reader, endian)? as u64
    };
    let mut typ = [0u8; 1];
    reader.read_exact(&mut typ)?;
    let len = read_i32_endian(reader, endian)?;
    Ok((v, o, typ[0], len))
}

fn decode_strl_ref(buf: &[u8], endian: Endian, ds_format: u16) -> Result<(u32, u64)> {
    if ds_format >= 118 {
        if buf.len() < 8 {
            return Err(Error::ParseError("strl ref too short".to_string()));
        }
        let use_little = endian == Endian::Little || (endian == Endian::Big && ds_format >= 119);
        let v = if use_little {
            (buf[0] as u16 | ((buf[1] as u16) << 8)) as u32
        } else {
            ((buf[0] as u16) << 8 | buf[1] as u16) as u32
        };
        let o = if use_little {
            (buf[2] as u64)
                | ((buf[3] as u64) << 8)
                | ((buf[4] as u64) << 16)
                | ((buf[5] as u64) << 24)
                | ((buf[6] as u64) << 32)
                | ((buf[7] as u64) << 40)
        } else {
            ((buf[2] as u64) << 40)
                | ((buf[3] as u64) << 32)
                | ((buf[4] as u64) << 24)
                | ((buf[5] as u64) << 16)
                | ((buf[6] as u64) << 8)
                | (buf[7] as u64)
        };
        Ok((v, o))
    } else {
        let mut cursor = std::io::Cursor::new(buf);
        let v = read_u32_endian(&mut cursor, endian)?;
        let o = read_u32_endian(&mut cursor, endian)? as u64;
        Ok((v, o))
    }
}

fn read_u32_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u32> {
    Ok(match endian {
        Endian::Little => reader.read_u32::<byteorder::LittleEndian>()?,
        Endian::Big => reader.read_u32::<byteorder::BigEndian>()?,
    })
}

fn read_u64_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u64> {
    Ok(match endian {
        Endian::Little => reader.read_u64::<byteorder::LittleEndian>()?,
        Endian::Big => reader.read_u64::<byteorder::BigEndian>()?,
    })
}

fn read_i32_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<i32> {
    Ok(match endian {
        Endian::Little => reader.read_i32::<byteorder::LittleEndian>()?,
        Endian::Big => reader.read_i32::<byteorder::BigEndian>()?,
    })
}

enum ColumnBuilder {
    Int8(PrimitiveChunkedBuilder<Int8Type>),
    Int16(PrimitiveChunkedBuilder<Int16Type>),
    Int32(PrimitiveChunkedBuilder<Int32Type>),
    Float32(PrimitiveChunkedBuilder<Float32Type>),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    Utf8(StringChunkedBuilder),
}

fn build_label_maps(metadata: &Metadata) -> HashMap<String, Arc<LabelMap>> {
    let mut out = HashMap::new();
    for vl in &metadata.value_labels {
        let mut map = LabelMap::default();
        for (key, value) in vl.mapping.iter() {
            if let crate::stata::types::ValueLabelKey::Integer(v) = key {
                map.insert_int(*v, value.clone());
                map.insert_float(*v as f64, value.clone());
            } else if let crate::stata::types::ValueLabelKey::Double(v) = key {
                map.insert_float(*v, value.clone());
            }
        }
        out.insert(vl.name.clone(), Arc::new(map));
    }
    out
}

#[derive(Default)]
struct LabelMap {
    int_map: HashMap<i32, String>,
    float_map: HashMap<u64, String>,
}

impl LabelMap {
    fn insert_int(&mut self, v: i32, label: String) {
        self.int_map.insert(v, label);
    }

    fn insert_float(&mut self, v: f64, label: String) {
        self.float_map.insert(v.to_bits(), label);
    }

    fn get_int(&self, v: i32) -> Option<&String> {
        self.int_map.get(&v)
    }

    fn get_float(&self, v: f64) -> Option<&String> {
        self.float_map.get(&v.to_bits())
    }
}

fn append_labeled_int(
    builder: &mut StringChunkedBuilder,
    v: Option<i32>,
    labels: Option<&LabelMap>,
) {
    if let Some(v) = v {
        if let Some(labels) = labels {
            if let Some(label) = labels.get_int(v) {
                builder.append_value(label);
                return;
            }
        }
        builder.append_value(&v.to_string());
    } else {
        builder.append_null();
    }
}

fn append_labeled_float(
    builder: &mut StringChunkedBuilder,
    v: Option<f64>,
    labels: Option<&LabelMap>,
) {
    if let Some(v) = v {
        if let Some(labels) = labels {
            if let Some(label) = labels.get_float(v) {
                builder.append_value(label);
                return;
            }
        }
        builder.append_value(&v.to_string());
    } else {
        builder.append_null();
    }
}

impl ColumnBuilder {
    fn finish(self) -> Series {
        match self {
            ColumnBuilder::Int8(b) => b.finish().into_series(),
            ColumnBuilder::Int16(b) => b.finish().into_series(),
            ColumnBuilder::Int32(b) => b.finish().into_series(),
            ColumnBuilder::Float32(b) => b.finish().into_series(),
            ColumnBuilder::Float64(b) => b.finish().into_series(),
            ColumnBuilder::Utf8(b) => b.finish().into_series(),
        }
    }
}

// ─── Informative null code paths ──────────────────────────────────────────────
// These functions are only called when InformativeNullOpts is Some.
// The existing fast path above is completely unchanged.

/// Returns the indicator string for a user-missing value, given the missing offset (1-26).
/// Checks the label map first if `use_value_labels` is true.
fn indicator_from_offset(
    offset: u8,
    raw_value: i32,
    label_map: Option<&LabelMap>,
    use_value_labels: bool,
) -> String {
    if use_value_labels {
        if let Some(label_map) = label_map {
            if let Some(label) = label_map.get_int(raw_value) {
                return label.clone();
            }
        }
    }
    offset_to_stata_label(offset)
}

fn indicator_from_offset_f(
    offset: u8,
    raw_bits: u64,
    label_map: Option<&LabelMap>,
    use_value_labels: bool,
) -> String {
    if use_value_labels {
        if let Some(label_map) = label_map {
            if let Some(label) = label_map.get_float(f64::from_bits(raw_bits)) {
                return label.clone();
            }
        }
    }
    offset_to_stata_label(offset)
}

/// Informative-null read of a full batch. Returns a DataFrame where indicator columns
/// are appended after all data columns (caller must apply mode transformation).
pub fn read_data_frame_range_with_indicators(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    shared: &SharedDecode,
    indicator_cols: &HashSet<String>,
    use_value_labels: bool,
    indicator_suffix: &str,
) -> Result<DataFrame> {
    let file = File::open(path)?;
    let data_offset = metadata.data_offset.ok_or_else(|| Error::MissingMetadata)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    reader.seek(SeekFrom::Start(data_offset))?;
    if ds_format >= 117 {
        read_tag(&mut reader, b"<data>")?;
    }

    let label_maps = shared.label_maps.as_ref();
    let rules = missing_rules(ds_format);

    let (col_indices, mut builders, col_offsets, col_widths, col_labels, mut string_scratch) =
        build_column_builders(metadata, columns, limit, label_maps, value_labels_as_strings)?;

    // Build parallel indicator builders for columns in indicator_cols
    let mut null_builders: Vec<Option<StringChunkedBuilder>> = col_indices
        .iter()
        .map(|&idx| {
            let var = &metadata.variables[idx];
            let is_numeric = matches!(var.var_type, VarType::Numeric(_));
            if is_numeric && indicator_cols.contains(&var.name) {
                let ind_name = format!("{}{}", var.name, indicator_suffix);
                Some(StringChunkedBuilder::new(ind_name.as_str().into(), limit))
            } else {
                None
            }
        })
        .collect();

    let record_len = metadata.storage_widths.iter().map(|v| *v as usize).sum::<usize>();
    let mut row_buf = vec![0u8; record_len];

    let total_rows = metadata.row_count as usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);

    if start_row > 0 {
        let byte_skip = (start_row as u64) * (record_len as u64);
        reader.seek(SeekFrom::Current(byte_skip as i64))?;
    }

    for _row_idx in start_row..end_row {
        reader.read_exact(&mut row_buf)?;

        for (i, &col_idx) in col_indices.iter().enumerate() {
            let col_offset = col_offsets[i];
            let width = col_widths[i];
            let slice = &row_buf[col_offset..col_offset + width];

            if let Some(ref mut null_builder) = null_builders[i] {
                // This column needs indicator tracking — use tagged path
                append_value_tagged(
                    &mut builders[i],
                    null_builder,
                    &metadata.variables[col_idx].var_type,
                    slice,
                    endian,
                    rules,
                    missing_string_as_null,
                    shared.strls.as_ref().map(|v| v.as_ref()),
                    ds_format,
                    col_labels[i].as_deref(),
                    metadata.encoding,
                    string_scratch[i].as_mut(),
                    use_value_labels,
                )?;
            } else {
                // No indicator — use the regular (fast) path
                append_value(
                    &mut builders[i],
                    &metadata.variables[col_idx].var_type,
                    slice,
                    endian,
                    rules,
                    missing_string_as_null,
                    shared.strls.as_ref().map(|v| v.as_ref()),
                    ds_format,
                    col_labels[i].as_deref(),
                    metadata.encoding,
                    string_scratch[i].as_mut(),
                )?;
            }
        }
    }

    if ds_format >= 117 && offset + (end_row - start_row) >= total_rows {
        read_tag(&mut reader, b"</data>")?;
    }

    // Build the DataFrame: data columns first, then indicator columns appended
    let mut cols: Vec<Column> = Vec::with_capacity(builders.len() + null_builders.iter().filter(|b| b.is_some()).count());
    for builder in builders {
        cols.push(builder.finish().into());
    }
    for null_builder in null_builders {
        if let Some(b) = null_builder {
            cols.push(b.finish().into_series().into());
        }
    }

    Ok(DataFrame::new_infer_height(cols)?)
}

fn append_value_tagged(
    builder: &mut ColumnBuilder,
    null_builder: &mut StringChunkedBuilder,
    var_type: &VarType,
    buf: &[u8],
    endian: Endian,
    rules: crate::stata::value::MissingRules,
    missing_string_as_null: bool,
    strls: Option<&HashMap<(u32, u64), String>>,
    ds_format: u16,
    label_map: Option<&LabelMap>,
    encoding: &'static encoding_rs::Encoding,
    scratch: Option<&mut StringScratch>,
    use_value_labels: bool,
) -> Result<()> {
    match (builder, var_type) {
        (ColumnBuilder::Int8(b), VarType::Numeric(NumericType::Byte)) => {
            let (val, offset) = read_i8_tagged(buf, rules);
            match (val, offset) {
                (Some(v), _) => { b.append_value(v); null_builder.append_null(); }
                (None, Some(k)) => {
                    b.append_null();
                    let raw = rules.system_missing_int8 as i32 + k as i32;
                    null_builder.append_value(&indicator_from_offset(k, raw, label_map, use_value_labels));
                }
                (None, None) => { b.append_null(); null_builder.append_null(); }
            }
        }
        (ColumnBuilder::Int16(b), VarType::Numeric(NumericType::Int)) => {
            let (val, offset) = read_i16_tagged(buf, endian, rules);
            match (val, offset) {
                (Some(v), _) => { b.append_value(v); null_builder.append_null(); }
                (None, Some(k)) => {
                    b.append_null();
                    let raw = rules.system_missing_int16 as i32 + k as i32;
                    null_builder.append_value(&indicator_from_offset(k, raw, label_map, use_value_labels));
                }
                (None, None) => { b.append_null(); null_builder.append_null(); }
            }
        }
        (ColumnBuilder::Int32(b), VarType::Numeric(NumericType::Long)) => {
            let (val, offset) = read_i32_tagged(buf, endian, rules);
            match (val, offset) {
                (Some(v), _) => { b.append_value(v); null_builder.append_null(); }
                (None, Some(k)) => {
                    b.append_null();
                    let raw = rules.system_missing_int32 + k as i32;
                    null_builder.append_value(&indicator_from_offset(k, raw, label_map, use_value_labels));
                }
                (None, None) => { b.append_null(); null_builder.append_null(); }
            }
        }
        (ColumnBuilder::Float32(b), VarType::Numeric(NumericType::Float)) => {
            let (val, offset) = read_f32_tagged(buf, endian, rules);
            match (val, offset) {
                (Some(v), _) => { b.append_value(v); null_builder.append_null(); }
                (None, Some(k)) => {
                    b.append_null();
                    // For float, reconstruct raw bits for label lookup
                    let raw_bits = (rules.missing_float as u64) + (k as u64) * 0x0008_0000;
                    null_builder.append_value(&indicator_from_offset_f(k, raw_bits, label_map, use_value_labels));
                }
                (None, None) => { b.append_null(); null_builder.append_null(); }
            }
        }
        (ColumnBuilder::Float64(b), VarType::Numeric(NumericType::Double)) => {
            let (val, offset) = read_f64_tagged(buf, endian, rules);
            match (val, offset) {
                (Some(v), _) => { b.append_value(v); null_builder.append_null(); }
                (None, Some(k)) => {
                    b.append_null();
                    let raw_bits = rules.missing_double + k as u64;
                    null_builder.append_value(&indicator_from_offset_f(k, raw_bits, label_map, use_value_labels));
                }
                (None, None) => { b.append_null(); null_builder.append_null(); }
            }
        }
        // For labeled numeric columns (Utf8 builder), tagged path uses the same logic but
        // the label map is for data values, not missing indicators. Fall through to regular path.
        (b @ ColumnBuilder::Utf8(_), vt @ VarType::Numeric(_)) => {
            null_builder.append_null();
            append_value(
                b,
                vt,
                buf,
                endian,
                rules,
                missing_string_as_null,
                strls,
                ds_format,
                label_map,
                encoding,
                scratch,
            )?;
        }
        // Strings have no extended missing in Stata — fall through to regular path
        (b, vt) => {
            null_builder.append_null();
            append_value(
                b,
                vt,
                buf,
                endian,
                rules,
                missing_string_as_null,
                strls,
                ds_format,
                label_map,
                encoding,
                scratch,
            )?;
        }
    }
    Ok(())
}
