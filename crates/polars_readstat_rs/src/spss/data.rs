use crate::spss::error::{Error, Result};
use crate::spss::types::{Endian, FormatClass, Metadata, VarType, Variable};
use flate2::read::ZlibDecoder;
use polars::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

const SAV_MISSING_DOUBLE: u64 = 0xFFEFFFFFFFFFFFFF;
const SAV_LOWEST_DOUBLE: u64 = 0xFFEFFFFFFFFFFFFE;
const SAV_HIGHEST_DOUBLE: u64 = 0x7FEFFFFFFFFFFFFF;
const SPSS_SEC_SHIFT: i64 = 12_219_379_200;
const SEC_PER_DAY: i64 = 86_400;
const SEC_MILLISECOND: i64 = 1_000;
const SEC_NANOSECOND: i64 = 1_000_000_000;

static PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static PROFILE_NUM_NS: AtomicU64 = AtomicU64::new(0);
static PROFILE_STR_NS: AtomicU64 = AtomicU64::new(0);
static PROFILE_DECODE_NS: AtomicU64 = AtomicU64::new(0);
static PROFILE_LABEL_NS: AtomicU64 = AtomicU64::new(0);
static PROFILE_NUM_CT: AtomicU64 = AtomicU64::new(0);
static PROFILE_STR_CT: AtomicU64 = AtomicU64::new(0);

fn profile_enabled() -> bool {
    *PROFILE_ENABLED.get_or_init(|| std::env::var("SPSS_PROFILE").ok().is_some())
}

fn add_ns(target: &AtomicU64, elapsed: std::time::Duration) {
    target.fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
}

#[allow(dead_code)]
pub(crate) fn profile_reset() {
    if !profile_enabled() {
        return;
    }
    PROFILE_NUM_NS.store(0, Ordering::Relaxed);
    PROFILE_STR_NS.store(0, Ordering::Relaxed);
    PROFILE_DECODE_NS.store(0, Ordering::Relaxed);
    PROFILE_LABEL_NS.store(0, Ordering::Relaxed);
    PROFILE_NUM_CT.store(0, Ordering::Relaxed);
    PROFILE_STR_CT.store(0, Ordering::Relaxed);
}

#[allow(dead_code)]
pub(crate) fn profile_print() {
    if !profile_enabled() {
        return;
    }
    let num_ns = PROFILE_NUM_NS.load(Ordering::Relaxed);
    let str_ns = PROFILE_STR_NS.load(Ordering::Relaxed);
    let decode_ns = PROFILE_DECODE_NS.load(Ordering::Relaxed);
    let label_ns = PROFILE_LABEL_NS.load(Ordering::Relaxed);
    let num_ct = PROFILE_NUM_CT.load(Ordering::Relaxed);
    let str_ct = PROFILE_STR_CT.load(Ordering::Relaxed);
    let total_ms = (num_ns + str_ns) as f64 / 1_000_000.0;
    eprintln!(
        "SPSS_PROFILE total_ms={:.3} num_ct={} str_ct={} num_ms={:.3} str_ms={:.3} decode_ms={:.3} label_ms={:.3}",
        total_ms,
        num_ct,
        str_ct,
        num_ns as f64 / 1_000_000.0,
        str_ns as f64 / 1_000_000.0,
        decode_ns as f64 / 1_000_000.0,
        label_ns as f64 / 1_000_000.0,
    );
}

/// Streaming variant: reads `limit` rows starting at `offset`, dispatching
/// a batch of `batch_size` rows to `on_batch` as soon as each batch is ready.
/// Returns `false` from the callback to stop early.
/// For compression=1 (SAV) this maintains the decompressor state across batches
/// so the file is read in a single sequential pass with no re-seeking.
/// For compression=2 (ZSAV) it falls back to reading the full range first.
pub fn read_data_frame_streaming(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    compression: i32,
    bias: f64,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    batch_size: usize,
    on_batch: &mut dyn FnMut(DataFrame) -> bool,
) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let data_offset = metadata
        .data_offset
        .ok_or_else(|| Error::ParseError("missing data offset".to_string()))?;
    let record_len = metadata.variables.iter().map(|v| v.width * 8).sum::<usize>();
    reader.seek(SeekFrom::Start(data_offset))?;

    let total_rows = metadata.row_count as usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);

    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };

    let needed_label_names = if value_labels_as_strings && columns.is_some() {
        let mut names = HashSet::new();
        for &idx in &col_indices {
            if let Some(name) = metadata.variables[idx].value_label.as_ref() {
                names.insert(name.clone());
            }
        }
        Some(names)
    } else {
        None
    };
    let label_maps = if value_labels_as_strings {
        build_label_maps(metadata, needed_label_names.as_ref())
    } else {
        std::collections::HashMap::new()
    };
    // Plans are created once and reused across batches (they hold Arc refs to label maps).
    let mut plans = Vec::with_capacity(col_indices.len());
    for &idx in &col_indices {
        let var = &metadata.variables[idx];
        let label_map = var
            .value_label
            .as_ref()
            .and_then(|n| label_maps.get(n))
            .cloned();
        let missing_set = if var.missing_strings.is_empty() {
            None
        } else {
            Some(var.missing_strings.iter().cloned().collect::<HashSet<_>>())
        };
        plans.push(ColumnPlan::new(
            var,
            var.offset * 8,
            var.width * 8,
            label_map,
            missing_set,
            missing_string_as_null,
        ));
    }

    // Inline helper: create a fresh set of column builders for one batch.
    let make_builders = |cap: usize| -> Vec<ColumnBuilder> {
        col_indices
            .iter()
            .map(|&idx| {
                let var = &metadata.variables[idx];
                let name = var.name.as_str();
                let has_label = var
                    .value_label
                    .as_ref()
                    .map(|n| label_maps.contains_key(n))
                    .unwrap_or(false);
                match (var.var_type, has_label && value_labels_as_strings) {
                    (VarType::Numeric, true) => ColumnBuilder::Utf8 {
                        builder: StringChunkedBuilder::new(name.into(), cap),
                        num_cache: Some(NumericStringCache::new()),
                    },
                    (VarType::Numeric, false) => match var.format_class {
                        Some(FormatClass::Date) => ColumnBuilder::Date(
                            PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), cap),
                        ),
                        Some(FormatClass::DateTime) => ColumnBuilder::DateTime(
                            PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), cap),
                        ),
                        Some(FormatClass::Time) => ColumnBuilder::Time(
                            PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), cap),
                        ),
                        None => ColumnBuilder::Float64(
                            PrimitiveChunkedBuilder::<Float64Type>::new(name.into(), cap),
                        ),
                    },
                    (VarType::Str, _) => ColumnBuilder::Utf8 {
                        builder: StringChunkedBuilder::new(name.into(), cap),
                        num_cache: None,
                    },
                }
            })
            .collect()
    };

    // Inline helper: finish builders into a DataFrame.
    let finish_batch = |builders: Vec<ColumnBuilder>| -> Result<DataFrame> {
        let cols: Vec<_> = builders.into_iter().map(|b| b.finish().into()).collect();
        DataFrame::new_infer_height(cols).map_err(|e| Error::ParseError(e.to_string()))
    };

    let mut row_buf = vec![0u8; record_len];

    if compression == 0 {
        // Uncompressed: O(1) seek directly to start_row.
        if start_row > 0 {
            reader.seek(SeekFrom::Current((start_row as i64) * (record_len as i64)))?;
        }
        let mut row_idx = start_row;
        while row_idx < end_row {
            let batch_rows = batch_size.min(end_row - row_idx);
            let mut builders = make_builders(batch_rows);
            let np = build_numeric_plans(&plans, &builders);
            let mut added = 0usize;
            while added < batch_rows {
                reader.read_exact(&mut row_buf)?;
                if let Some(np) = np.as_deref() {
                    append_numeric_row(&mut builders, &plans, np, &row_buf, endian)?;
                } else {
                    append_row(&mut builders, &plans, &row_buf, endian, metadata.encoding)?;
                }
                row_idx += 1;
                added += 1;
            }
            if added == 0 {
                break;
            }
            let df = finish_batch(builders)?;
            if !on_batch(df) {
                return Ok(());
            }
        }
    } else if compression == 1 {
        // SAV byte-run compression: maintain decompressor state across batches so
        // the file is read in a single forward pass (no re-seeking per batch).
        let mut decompressor = SavRowDecompressor::new(endian, bias);
        let mut row_idx = 0usize;

        // Consume rows before start_row without storing them.
        while row_idx < start_row {
            let status = decompressor.read_row(&mut reader, &mut row_buf, record_len)?;
            if status == DecompressStatus::FinishedAll {
                return Ok(());
            }
            row_idx += 1;
        }

        // Dispatch batches.
        while row_idx < end_row {
            let batch_rows = batch_size.min(end_row - row_idx);
            let mut builders = make_builders(batch_rows);
            let np = build_numeric_plans(&plans, &builders);
            let mut added = 0usize;
            while added < batch_rows {
                let status = decompressor.read_row(&mut reader, &mut row_buf, record_len)?;
                if status == DecompressStatus::FinishedAll {
                    break;
                }
                if let Some(np) = np.as_deref() {
                    append_numeric_row(&mut builders, &plans, np, &row_buf, endian)?;
                } else {
                    append_row(&mut builders, &plans, &row_buf, endian, metadata.encoding)?;
                }
                row_idx += 1;
                added += 1;
            }
            if added == 0 {
                break;
            }
            let df = finish_batch(builders)?;
            if !on_batch(df) {
                return Ok(());
            }
        }
    } else {
        // compression == 2 (ZSAV) or unknown: read the full range at once, then
        // slice into batches. Block-level streaming for ZSAV is left as a future
        // improvement.
        let df = read_data_frame_with_reader(
            &mut reader,
            metadata,
            endian,
            compression,
            bias,
            columns,
            offset,
            limit,
            missing_string_as_null,
            value_labels_as_strings,
        )?;
        let height = df.height();
        let mut off = 0usize;
        while off < height {
            let take = batch_size.min(height - off);
            let batch = df.slice(off as i64, take);
            if !on_batch(batch) {
                return Ok(());
            }
            off += take;
        }
    }

    Ok(())
}

pub fn read_data_frame_with_reader(
    reader: &mut BufReader<File>,
    metadata: &Metadata,
    endian: Endian,
    compression: i32,
    bias: f64,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
) -> Result<DataFrame> {
    let data_offset = metadata
        .data_offset
        .ok_or_else(|| Error::ParseError("missing data offset".to_string()))?;
    let record_len = metadata
        .variables
        .iter()
        .map(|v| v.width * 8)
        .sum::<usize>();

    reader.seek(SeekFrom::Start(data_offset))?;

    let total_rows = metadata.row_count as usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);

    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };

    let needed_label_names = if value_labels_as_strings && columns.is_some() {
        let mut names = HashSet::new();
        for &idx in &col_indices {
            if let Some(name) = metadata.variables[idx].value_label.as_ref() {
                names.insert(name.clone());
            }
        }
        Some(names)
    } else {
        None
    };

    let label_maps = if value_labels_as_strings {
        build_label_maps(metadata, needed_label_names.as_ref())
    } else {
        std::collections::HashMap::new()
    };

    let mut builders = Vec::with_capacity(col_indices.len());
    let mut plans = Vec::with_capacity(col_indices.len());

    for &idx in &col_indices {
        let var = &metadata.variables[idx];
        let name = var.name.as_str();
        let label_map = var
            .value_label
            .as_ref()
            .and_then(|name| label_maps.get(name))
            .cloned();
        let builder = match (var.var_type, label_map.is_some() && value_labels_as_strings) {
            (VarType::Numeric, true) => ColumnBuilder::Utf8 {
                builder: StringChunkedBuilder::new(name.into(), limit),
                num_cache: Some(NumericStringCache::new()),
            },
            (VarType::Numeric, false) => match var.format_class {
                Some(FormatClass::Date) => ColumnBuilder::Date(
                    PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), limit),
                ),
                Some(FormatClass::DateTime) => ColumnBuilder::DateTime(PrimitiveChunkedBuilder::<
                    Int64Type,
                >::new(
                    name.into(), limit
                )),
                Some(FormatClass::Time) => ColumnBuilder::Time(
                    PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), limit),
                ),
                None => ColumnBuilder::Float64(PrimitiveChunkedBuilder::<Float64Type>::new(
                    name.into(),
                    limit,
                )),
            },
            (VarType::Str, _) => ColumnBuilder::Utf8 {
                builder: StringChunkedBuilder::new(name.into(), limit),
                num_cache: None,
            },
        };
        let missing_set = if var.missing_strings.is_empty() {
            None
        } else {
            Some(var.missing_strings.iter().cloned().collect::<HashSet<_>>())
        };
        let plan = ColumnPlan::new(
            var,
            var.offset * 8,
            var.width * 8,
            label_map,
            missing_set,
            missing_string_as_null,
        );
        builders.push(builder);
        plans.push(plan);
    }

    let numeric_plans = build_numeric_plans(&plans, &builders);

    if compression == 0 {
        if start_row > 0 {
            let byte_skip = (start_row as u64) * (record_len as u64);
            reader.seek(SeekFrom::Current(byte_skip as i64))?;
        }
        let mut row_buf = vec![0u8; record_len];
        for _row_idx in start_row..end_row {
            reader.read_exact(&mut row_buf)?;
            if let Some(numeric_plans) = numeric_plans.as_deref() {
                append_numeric_row(&mut builders, &plans, numeric_plans, &row_buf, endian)?;
            } else {
                append_row(&mut builders, &plans, &row_buf, endian, metadata.encoding)?;
            }
        }
    } else if compression == 1 {
        let mut row_buf = vec![0u8; record_len];
        let mut decompressor = SavRowDecompressor::new(endian, bias);
        let mut row_idx = 0usize;
        while row_idx < end_row {
            let status = decompressor.read_row(reader, &mut row_buf, record_len)?;
            if status == DecompressStatus::FinishedAll {
                break;
            }
            if row_idx >= start_row {
                if let Some(numeric_plans) = numeric_plans.as_deref() {
                    append_numeric_row(&mut builders, &plans, numeric_plans, &row_buf, endian)?;
                } else {
                    append_row(&mut builders, &plans, &row_buf, endian, metadata.encoding)?;
                }
            }
            row_idx += 1;
        }
    } else if compression == 2 {
        let mut row_buf = vec![0u8; record_len];
        read_zsav_data(
            reader,
            endian,
            bias,
            record_len,
            start_row,
            end_row,
            &plans,
            &mut builders,
            numeric_plans.as_deref(),
            &mut row_buf,
            metadata.encoding,
        )?;
    } else {
        return Err(Error::Unsupported(format!(
            "SPSS compression {} not supported",
            compression
        )));
    }

    let mut cols = Vec::with_capacity(builders.len());
    for b in builders {
        cols.push(b.finish().into());
    }
    DataFrame::new_infer_height(cols).map_err(|e| Error::ParseError(e.to_string()))
}

#[allow(dead_code)]
pub fn read_data_columns_uncompressed(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
) -> Result<Vec<Series>> {
    let data_offset = metadata
        .data_offset
        .ok_or_else(|| Error::ParseError("missing data offset".to_string()))?;
    let record_len = metadata
        .variables
        .iter()
        .map(|v| v.width * 8)
        .sum::<usize>();

    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    reader.seek(SeekFrom::Start(data_offset))?;

    let total_rows = metadata.row_count as usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);

    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };

    let needed_label_names = if value_labels_as_strings && columns.is_some() {
        let mut names = HashSet::new();
        for &idx in &col_indices {
            if let Some(name) = metadata.variables[idx].value_label.as_ref() {
                names.insert(name.clone());
            }
        }
        Some(names)
    } else {
        None
    };

    let label_maps = if value_labels_as_strings {
        build_label_maps(metadata, needed_label_names.as_ref())
    } else {
        std::collections::HashMap::new()
    };

    let mut builders = Vec::with_capacity(col_indices.len());
    let mut plans = Vec::with_capacity(col_indices.len());

    for &idx in &col_indices {
        let var = &metadata.variables[idx];
        let name = var.name.as_str();
        let label_map = var
            .value_label
            .as_ref()
            .and_then(|name| label_maps.get(name))
            .cloned();
        let builder = match (var.var_type, label_map.is_some() && value_labels_as_strings) {
            (VarType::Numeric, true) => ColumnBuilder::Utf8 {
                builder: StringChunkedBuilder::new(name.into(), limit),
                num_cache: Some(NumericStringCache::new()),
            },
            (VarType::Numeric, false) => match var.format_class {
                Some(FormatClass::Date) => ColumnBuilder::Date(
                    PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), limit),
                ),
                Some(FormatClass::DateTime) => ColumnBuilder::DateTime(PrimitiveChunkedBuilder::<
                    Int64Type,
                >::new(
                    name.into(), limit
                )),
                Some(FormatClass::Time) => ColumnBuilder::Time(
                    PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), limit),
                ),
                None => ColumnBuilder::Float64(PrimitiveChunkedBuilder::<Float64Type>::new(
                    name.into(),
                    limit,
                )),
            },
            (VarType::Str, _) => ColumnBuilder::Utf8 {
                builder: StringChunkedBuilder::new(name.into(), limit),
                num_cache: None,
            },
        };
        let missing_set = if var.missing_strings.is_empty() {
            None
        } else {
            Some(var.missing_strings.iter().cloned().collect::<HashSet<_>>())
        };
        let plan = ColumnPlan::new(
            var,
            var.offset * 8,
            var.width * 8,
            label_map,
            missing_set,
            missing_string_as_null,
        );
        builders.push(builder);
        plans.push(plan);
    }

    if start_row > 0 {
        let byte_skip = (start_row as u64) * (record_len as u64);
        reader.seek(SeekFrom::Current(byte_skip as i64))?;
    }
    let mut row_buf = vec![0u8; record_len];
    for _row_idx in start_row..end_row {
        reader.read_exact(&mut row_buf)?;
        append_row(&mut builders, &plans, &row_buf, endian, metadata.encoding)?;
    }

    let mut cols = Vec::with_capacity(builders.len());
    for b in builders {
        cols.push(b.finish().into());
    }
    Ok(cols)
}

fn append_row(
    builders: &mut [ColumnBuilder],
    plans: &[ColumnPlan],
    row_buf: &[u8],
    endian: Endian,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    for (i, plan) in plans.iter().enumerate() {
        let slice = &row_buf[plan.offset..plan.offset + plan.width];
        append_value(&mut builders[i], plan, slice, endian, encoding)?;
    }
    Ok(())
}

struct NumericPlan {
    builder_idx: usize,
    plan_idx: usize,
}

fn build_numeric_plans(
    plans: &[ColumnPlan],
    builders: &[ColumnBuilder],
) -> Option<Vec<NumericPlan>> {
    if plans.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(plans.len());
    for (i, plan) in plans.iter().enumerate() {
        if !matches!(plan.var_type, VarType::Numeric) {
            return None;
        }
        if !matches!(builders.get(i), Some(ColumnBuilder::Float64(_))) {
            return None;
        }
        out.push(NumericPlan {
            builder_idx: i,
            plan_idx: i,
        });
    }
    Some(out)
}

fn append_numeric_row(
    builders: &mut [ColumnBuilder],
    plans: &[ColumnPlan],
    numeric_plans: &[NumericPlan],
    row_buf: &[u8],
    endian: Endian,
) -> Result<()> {
    for plan in numeric_plans {
        let col_plan = &plans[plan.plan_idx];
        let slice = &row_buf[col_plan.offset..col_plan.offset + col_plan.width];
        match &mut builders[plan.builder_idx] {
            ColumnBuilder::Float64(b) => {
                let bytes: [u8; 8] = slice[..8]
                    .try_into()
                    .map_err(|_| Error::ParseError("short numeric value".to_string()))?;
                let v = match endian {
                    Endian::Little => f64::from_le_bytes(bytes),
                    Endian::Big => f64::from_be_bytes(bytes),
                };
                let bits = v.to_bits();
                if is_missing_numeric(col_plan, v, bits) {
                    b.append_null();
                } else {
                    b.append_value(apply_format_class(v, col_plan.format_class));
                }
            }
            _ => return Err(Error::ParseError("column type mismatch".to_string())),
        }
    }
    Ok(())
}

fn append_value(
    builder: &mut ColumnBuilder,
    plan: &ColumnPlan,
    buf: &[u8],
    endian: Endian,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    match (builder, plan.var_type) {
        (ColumnBuilder::Float64(b), VarType::Numeric) => {
            let t0 = if profile_enabled() {
                Some(Instant::now())
            } else {
                None
            };
            let bytes: [u8; 8] = buf[..8]
                .try_into()
                .map_err(|_| Error::ParseError("short numeric value".to_string()))?;
            let v = match endian {
                Endian::Little => f64::from_le_bytes(bytes),
                Endian::Big => f64::from_be_bytes(bytes),
            };
            let bits = v.to_bits();
            if is_missing_numeric(plan, v, bits) {
                b.append_null();
            } else {
                b.append_value(apply_format_class(v, plan.format_class));
            }
            if let Some(t0) = t0 {
                add_ns(&PROFILE_NUM_NS, t0.elapsed());
                PROFILE_NUM_CT.fetch_add(1, Ordering::Relaxed);
            }
        }
        (ColumnBuilder::Date(b), VarType::Numeric) => {
            let bytes: [u8; 8] = buf[..8]
                .try_into()
                .map_err(|_| Error::ParseError("short numeric value".to_string()))?;
            let v = match endian {
                Endian::Little => f64::from_le_bytes(bytes),
                Endian::Big => f64::from_be_bytes(bytes),
            };
            let bits = v.to_bits();
            if is_missing_numeric(plan, v, bits) {
                b.append_null();
            } else {
                b.append_value(apply_format_class_date(v));
            }
        }
        (ColumnBuilder::DateTime(b), VarType::Numeric) => {
            let bytes: [u8; 8] = buf[..8]
                .try_into()
                .map_err(|_| Error::ParseError("short numeric value".to_string()))?;
            let v = match endian {
                Endian::Little => f64::from_le_bytes(bytes),
                Endian::Big => f64::from_be_bytes(bytes),
            };
            let bits = v.to_bits();
            if is_missing_numeric(plan, v, bits) {
                b.append_null();
            } else {
                b.append_value(apply_format_class_datetime(v));
            }
        }
        (ColumnBuilder::Time(b), VarType::Numeric) => {
            let bytes: [u8; 8] = buf[..8]
                .try_into()
                .map_err(|_| Error::ParseError("short numeric value".to_string()))?;
            let v = match endian {
                Endian::Little => f64::from_le_bytes(bytes),
                Endian::Big => f64::from_be_bytes(bytes),
            };
            let bits = v.to_bits();
            if is_missing_numeric(plan, v, bits) {
                b.append_null();
            } else {
                b.append_value(apply_format_class_time(v));
            }
        }
        (ColumnBuilder::Utf8 { builder, num_cache }, VarType::Numeric) => {
            let t0 = if profile_enabled() {
                Some(Instant::now())
            } else {
                None
            };
            let bytes: [u8; 8] = buf[..8]
                .try_into()
                .map_err(|_| Error::ParseError("short numeric value".to_string()))?;
            let v = match endian {
                Endian::Little => f64::from_le_bytes(bytes),
                Endian::Big => f64::from_be_bytes(bytes),
            };
            let bits = v.to_bits();
            if is_missing_numeric(plan, v, bits) {
                builder.append_null();
            } else if let Some(labels) = plan.label_map.as_deref() {
                let t_label = if profile_enabled() {
                    Some(Instant::now())
                } else {
                    None
                };
                let label = labels.get_float_bits(bits);
                if let Some(t_label) = t_label {
                    add_ns(&PROFILE_LABEL_NS, t_label.elapsed());
                }
                if let Some(label) = label {
                    builder.append_value(label);
                } else {
                    if let Some(cache) = num_cache {
                        if cache.has_last && cache.last_bits == bits {
                            builder.append_value(&cache.last_value);
                        } else {
                            cache.last_value = v.to_string();
                            cache.last_bits = bits;
                            cache.has_last = true;
                            builder.append_value(&cache.last_value);
                        }
                    } else {
                        builder.append_value(&v.to_string());
                    }
                }
            } else {
                if let Some(cache) = num_cache {
                    if cache.has_last && cache.last_bits == bits {
                        builder.append_value(&cache.last_value);
                    } else {
                        cache.last_value = v.to_string();
                        cache.last_bits = bits;
                        cache.has_last = true;
                        builder.append_value(&cache.last_value);
                    }
                } else {
                    builder.append_value(&v.to_string());
                }
            }
            if let Some(t0) = t0 {
                add_ns(&PROFILE_NUM_NS, t0.elapsed());
                PROFILE_NUM_CT.fetch_add(1, Ordering::Relaxed);
            }
        }
        (ColumnBuilder::Utf8 { builder, .. }, VarType::Str) => {
            let t0 = if profile_enabled() {
                Some(Instant::now())
            } else {
                None
            };
            if plan.missing_string_as_null && buf.iter().all(|b| *b == b' ' || *b == 0) {
                builder.append_null();
                return Ok(());
            }

            let mut end = if plan.string_len_bytes > 0 {
                plan.string_len_bytes.min(buf.len())
            } else {
                buf.len()
            };
            let raw = &buf[..end];

            let mut filtered_storage;
            let raw = if encoding == encoding_rs::UTF_8 {
                if raw.iter().any(|b| *b == 0) {
                    filtered_storage = Vec::with_capacity(raw.len());
                    for &b in raw {
                        if b != 0 {
                            filtered_storage.push(b);
                        }
                    }
                    filtered_storage.as_slice()
                } else {
                    raw
                }
            } else {
                raw
            };

            end = raw.len();
            while end > 0 && (raw[end - 1] == b' ' || raw[end - 1] == 0) {
                end -= 1;
            }
            let t_decode = if profile_enabled() {
                Some(Instant::now())
            } else {
                None
            };
            let s_owned;
            let s = if encoding == encoding_rs::UTF_8 {
                let slice = &raw[..end];
                match std::str::from_utf8(slice) {
                    Ok(s) => s,
                    Err(err) => {
                        let valid = err.valid_up_to();
                        std::str::from_utf8(&slice[..valid]).unwrap_or("")
                    }
                }
            } else {
                let decoded = encoding.decode_without_bom_handling(&raw[..end]).0;
                s_owned = decoded.into_owned();
                s_owned.as_str()
            };
            if let Some(t_decode) = t_decode {
                add_ns(&PROFILE_DECODE_NS, t_decode.elapsed());
            }

            if plan.fast_no_checks {
                builder.append_value(s);
                return Ok(());
            }

            let is_missing = (plan.missing_string_as_null && s.is_empty())
                || plan
                    .missing_set
                    .as_ref()
                    .map_or(false, |set| set.contains(s));
            if is_missing {
                builder.append_null();
            } else if let Some(labels) = plan.label_map.as_deref() {
                let t_label = if profile_enabled() {
                    Some(Instant::now())
                } else {
                    None
                };
                let label = labels.get_str(s);
                if let Some(t_label) = t_label {
                    add_ns(&PROFILE_LABEL_NS, t_label.elapsed());
                }
                if let Some(label) = label {
                    builder.append_value(label);
                } else {
                    builder.append_value(s);
                }
            } else {
                builder.append_value(s);
            }
            if let Some(t0) = t0 {
                add_ns(&PROFILE_STR_NS, t0.elapsed());
                PROFILE_STR_CT.fetch_add(1, Ordering::Relaxed);
            }
        }
        _ => return Err(Error::ParseError("column type mismatch".to_string())),
    }
    Ok(())
}

fn is_missing_numeric(plan: &ColumnPlan, v: f64, bits: u64) -> bool {
    if bits == SAV_MISSING_DOUBLE
        || bits == SAV_LOWEST_DOUBLE
        || bits == SAV_HIGHEST_DOUBLE
        || v.is_nan()
    {
        return true;
    }
    if plan.missing_doubles.is_empty() {
        return false;
    }
    if plan.missing_range {
        if plan.missing_doubles.len() >= 2 {
            let low = plan.missing_doubles[0].min(plan.missing_doubles[1]);
            let high = plan.missing_doubles[0].max(plan.missing_doubles[1]);
            if v >= low && v <= high {
                return true;
            }
        }
        if plan.missing_doubles.len() >= 3 {
            if bits == plan.missing_double_bits.get(2).copied().unwrap_or(0) {
                return true;
            }
        }
        false
    } else {
        plan.missing_double_bits.iter().any(|b| *b == bits)
    }
}

/// Returns the user-declared missing indicator string for a numeric SPSS value, or `None`
/// if the value is system-missing (or not missing at all).
///
/// - System missing (`SAV_MISSING_DOUBLE`), LOWEST, HIGHEST, NaN → `None`
/// - Discrete user-declared missing → label (if available) or `v.to_string()`
/// - Range user-declared missing → label (if available) or `"MISSING"`
fn missing_numeric_indicator(plan: &ColumnPlan, v: f64, bits: u64) -> Option<String> {
    // System-missing sentinel values or NaN → no indicator (plain null)
    if bits == SAV_MISSING_DOUBLE
        || bits == SAV_LOWEST_DOUBLE
        || bits == SAV_HIGHEST_DOUBLE
        || v.is_nan()
    {
        return None;
    }
    if plan.missing_doubles.is_empty() {
        return None; // no user-declared missings at all
    }
    if plan.missing_range {
        if plan.missing_doubles.len() >= 2 {
            let low = plan.missing_doubles[0].min(plan.missing_doubles[1]);
            let high = plan.missing_doubles[0].max(plan.missing_doubles[1]);
            if v >= low && v <= high {
                if let Some(label_map) = plan.label_map.as_deref() {
                    if let Some(label) = label_map.get_float_bits(bits) {
                        return Some(label.clone());
                    }
                }
                return Some("MISSING".to_string());
            }
        }
        // Third (discrete) value in range-mode
        if plan.missing_doubles.len() >= 3
            && bits == plan.missing_double_bits.get(2).copied().unwrap_or(0)
        {
            if let Some(label_map) = plan.label_map.as_deref() {
                if let Some(label) = label_map.get_float_bits(bits) {
                    return Some(label.clone());
                }
            }
            return Some(v.to_string());
        }
        None
    } else {
        if plan.missing_double_bits.iter().any(|b| *b == bits) {
            if let Some(label_map) = plan.label_map.as_deref() {
                if let Some(label) = label_map.get_float_bits(bits) {
                    return Some(label.clone());
                }
            }
            return Some(v.to_string());
        }
        None
    }
}

/// Compute the informative-null indicator string for a single column value from raw row bytes.
/// Returns `None` if not a user-declared missing (or if no indicator tracking is configured).
fn compute_col_indicator(
    plan: &ColumnPlan,
    buf: &[u8],
    endian: Endian,
    encoding: &'static encoding_rs::Encoding,
) -> Option<String> {
    match plan.var_type {
        VarType::Numeric => {
            if buf.len() < 8 {
                return None;
            }
            let bytes: [u8; 8] = buf[..8].try_into().ok()?;
            let v = match endian {
                Endian::Little => f64::from_le_bytes(bytes),
                Endian::Big => f64::from_be_bytes(bytes),
            };
            let bits = v.to_bits();
            if is_missing_numeric(plan, v, bits) {
                missing_numeric_indicator(plan, v, bits)
            } else {
                None
            }
        }
        VarType::Str => {
            let missing_set = plan.missing_set.as_ref()?;
            // Trim trailing spaces and nulls
            let mut end = buf.len();
            while end > 0 && (buf[end - 1] == b' ' || buf[end - 1] == 0) {
                end -= 1;
            }
            if end == 0 {
                return None; // empty → system-missing-like, no indicator
            }
            let s: String = if encoding == encoding_rs::UTF_8 {
                let filtered: Vec<u8> =
                    buf[..end].iter().filter(|&&b| b != 0).copied().collect();
                String::from_utf8_lossy(&filtered).into_owned()
            } else {
                encoding
                    .decode_without_bom_handling(&buf[..end])
                    .0
                    .into_owned()
            };
            if missing_set.contains(s.as_str()) {
                if let Some(label_map) = plan.label_map.as_deref() {
                    if let Some(label) = label_map.get_str(&s) {
                        return Some(label.clone());
                    }
                }
                Some(s)
            } else {
                None
            }
        }
    }
}

/// Like `append_row` but also appends to per-column indicator builders.
/// `ind_builders[i]` is `Some(builder)` when column i is tracked for informative nulls.
fn append_row_with_indicators(
    builders: &mut [ColumnBuilder],
    ind_builders: &mut [Option<StringChunkedBuilder>],
    plans: &[ColumnPlan],
    row_buf: &[u8],
    endian: Endian,
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    for (i, plan) in plans.iter().enumerate() {
        let slice = &row_buf[plan.offset..plan.offset + plan.width];
        if let Some(ind_b) = ind_builders[i].as_mut() {
            let indicator = compute_col_indicator(plan, slice, endian, encoding);
            match indicator {
                Some(s) => ind_b.append_value(&s),
                None => ind_b.append_null(),
            }
        }
        append_value(&mut builders[i], plan, slice, endian, encoding)?;
    }
    Ok(())
}

/// Like `read_zsav_data` but routes rows through `append_row_with_indicators`.
fn read_zsav_data_with_indicators<R: Read + Seek>(
    reader: &mut R,
    endian: Endian,
    bias: f64,
    start_row: usize,
    end_row: usize,
    plans: &[ColumnPlan],
    builders: &mut [ColumnBuilder],
    ind_builders: &mut [Option<StringChunkedBuilder>],
    row_buf: &mut [u8],
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    let zheader_ofs = reader.stream_position()?;
    let zheader = read_zheader(reader, endian)?;
    if zheader.zheader_ofs != zheader_ofs {
        return Err(Error::ParseError("invalid zsav header offset".to_string()));
    }

    reader.seek(SeekFrom::Start(zheader.ztrailer_ofs))?;
    let ztrailer = read_ztrailer(reader, endian)?;
    let n_blocks = ztrailer.n_blocks as usize;

    let mut entries = Vec::with_capacity(n_blocks);
    for _ in 0..n_blocks {
        entries.push(read_ztrailer_entry(reader, endian)?);
    }

    let mut stream = SavRowStream::new(endian, bias);
    let mut row_idx = 0usize;
    let mut out_offset = 0usize;

    for entry in entries {
        reader.seek(SeekFrom::Start(entry.compressed_ofs as u64))?;
        let mut compressed = vec![0u8; entry.compressed_size as usize];
        reader.read_exact(&mut compressed)?;

        let mut decoder = ZlibDecoder::new(&compressed[..]);
        let mut uncompressed = Vec::with_capacity(entry.uncompressed_size as usize);
        decoder.read_to_end(&mut uncompressed)?;
        if uncompressed.len() != entry.uncompressed_size as usize {
            return Err(Error::ParseError("zsav block size mismatch".to_string()));
        }

        let mut input_offset = 0usize;
        while input_offset <= uncompressed.len() {
            let status = stream.decompress_from_slice(
                &uncompressed,
                &mut input_offset,
                row_buf,
                &mut out_offset,
            )?;
            match status {
                StreamStatus::NeedData => break,
                StreamStatus::FinishedAll => return Ok(()),
                StreamStatus::FinishedRow => {
                    if row_idx >= start_row && row_idx < end_row {
                        append_row_with_indicators(
                            builders,
                            ind_builders,
                            plans,
                            row_buf,
                            endian,
                            encoding,
                        )?;
                    }
                    row_idx += 1;
                    out_offset = 0;
                    if row_idx >= end_row {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Read a range of rows into a DataFrame that includes informative-null indicator columns.
///
/// `indicator_col_names` must be aligned with the resolved `columns` slice (or all variables if
/// `columns` is `None`). `indicator_col_names[i]` is `Some(name)` to add an indicator column
/// named `name` immediately after column i, or `None` to skip.
///
/// Indicator columns are of type `String?` (null = not a user-declared missing; `Some(str)` =
/// the indicator label/value).
pub fn read_data_frame_with_indicators(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    compression: i32,
    bias: f64,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    indicator_col_names: &[Option<String>],
) -> Result<DataFrame> {
    let data_offset = metadata
        .data_offset
        .ok_or_else(|| Error::ParseError("missing data offset".to_string()))?;
    let record_len = metadata
        .variables
        .iter()
        .map(|v| v.width * 8)
        .sum::<usize>();

    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    reader.seek(SeekFrom::Start(data_offset))?;

    let total_rows = metadata.row_count as usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);

    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };

    let needed_label_names = if value_labels_as_strings && columns.is_some() {
        let mut names = HashSet::new();
        for &idx in &col_indices {
            if let Some(name) = metadata.variables[idx].value_label.as_ref() {
                names.insert(name.clone());
            }
        }
        Some(names)
    } else {
        None
    };

    let label_maps = if value_labels_as_strings {
        build_label_maps(metadata, needed_label_names.as_ref())
    } else {
        std::collections::HashMap::new()
    };

    let mut builders = Vec::with_capacity(col_indices.len());
    let mut plans = Vec::with_capacity(col_indices.len());
    let mut ind_builders: Vec<Option<StringChunkedBuilder>> = Vec::with_capacity(col_indices.len());

    for (local_i, &idx) in col_indices.iter().enumerate() {
        let var = &metadata.variables[idx];
        let name = var.name.as_str();
        let label_map = var
            .value_label
            .as_ref()
            .and_then(|lname| label_maps.get(lname))
            .cloned();
        let builder = match (var.var_type, label_map.is_some() && value_labels_as_strings) {
            (VarType::Numeric, true) => ColumnBuilder::Utf8 {
                builder: StringChunkedBuilder::new(name.into(), limit),
                num_cache: Some(NumericStringCache::new()),
            },
            (VarType::Numeric, false) => match var.format_class {
                Some(FormatClass::Date) => ColumnBuilder::Date(
                    PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), limit),
                ),
                Some(FormatClass::DateTime) => {
                    ColumnBuilder::DateTime(PrimitiveChunkedBuilder::<Int64Type>::new(
                        name.into(),
                        limit,
                    ))
                }
                Some(FormatClass::Time) => ColumnBuilder::Time(
                    PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), limit),
                ),
                None => ColumnBuilder::Float64(PrimitiveChunkedBuilder::<Float64Type>::new(
                    name.into(),
                    limit,
                )),
            },
            (VarType::Str, _) => ColumnBuilder::Utf8 {
                builder: StringChunkedBuilder::new(name.into(), limit),
                num_cache: None,
            },
        };
        let missing_set = if var.missing_strings.is_empty() {
            None
        } else {
            Some(var.missing_strings.iter().cloned().collect::<HashSet<_>>())
        };
        let plan = ColumnPlan::new(
            var,
            var.offset * 8,
            var.width * 8,
            label_map,
            missing_set,
            missing_string_as_null,
        );
        let ind_builder = indicator_col_names
            .get(local_i)
            .and_then(|opt| opt.as_ref())
            .map(|ind_name| StringChunkedBuilder::new(ind_name.as_str().into(), limit));
        builders.push(builder);
        plans.push(plan);
        ind_builders.push(ind_builder);
    }

    if compression == 0 {
        if start_row > 0 {
            let byte_skip = (start_row as u64) * (record_len as u64);
            reader.seek(SeekFrom::Current(byte_skip as i64))?;
        }
        let mut row_buf = vec![0u8; record_len];
        for _row_idx in start_row..end_row {
            reader.read_exact(&mut row_buf)?;
            append_row_with_indicators(
                &mut builders,
                &mut ind_builders,
                &plans,
                &row_buf,
                endian,
                metadata.encoding,
            )?;
        }
    } else if compression == 1 {
        let mut row_buf = vec![0u8; record_len];
        let mut decompressor = SavRowDecompressor::new(endian, bias);
        let mut row_idx = 0usize;
        while row_idx < end_row {
            let status = decompressor.read_row(&mut reader, &mut row_buf, record_len)?;
            if status == DecompressStatus::FinishedAll {
                break;
            }
            if row_idx >= start_row {
                append_row_with_indicators(
                    &mut builders,
                    &mut ind_builders,
                    &plans,
                    &row_buf,
                    endian,
                    metadata.encoding,
                )?;
            }
            row_idx += 1;
        }
    } else if compression == 2 {
        let mut row_buf = vec![0u8; record_len];
        read_zsav_data_with_indicators(
            &mut reader,
            endian,
            bias,
            start_row,
            end_row,
            &plans,
            &mut builders,
            &mut ind_builders,
            &mut row_buf,
            metadata.encoding,
        )?;
    } else {
        return Err(Error::Unsupported(format!(
            "SPSS compression {} not supported",
            compression
        )));
    }

    // Interleave main and indicator columns
    let mut cols: Vec<Column> = Vec::with_capacity(
        builders.len() + ind_builders.iter().filter(|b| b.is_some()).count(),
    );
    for (b, ind_b) in builders.into_iter().zip(ind_builders.into_iter()) {
        cols.push(b.finish().into());
        if let Some(ind_b) = ind_b {
            cols.push(Column::from(ind_b.finish().into_series()));
        }
    }
    DataFrame::new_infer_height(cols).map_err(|e| Error::ParseError(e.to_string()))
}

fn apply_format_class(v: f64, class: Option<FormatClass>) -> f64 {
    match class {
        Some(FormatClass::Date) => ((v as i64 - SPSS_SEC_SHIFT) / SEC_PER_DAY) as f64,
        Some(FormatClass::DateTime) => ((v as i64 - SPSS_SEC_SHIFT) * SEC_MILLISECOND) as f64,
        Some(FormatClass::Time) => ((v as i64) * SEC_NANOSECOND) as f64,
        None => v,
    }
}

fn apply_format_class_date(v: f64) -> i32 {
    ((v as i64 - SPSS_SEC_SHIFT) / SEC_PER_DAY) as i32
}

fn apply_format_class_datetime(v: f64) -> i64 {
    (v as i64 - SPSS_SEC_SHIFT) * SEC_MILLISECOND
}

fn apply_format_class_time(v: f64) -> i64 {
    (v as i64) * SEC_NANOSECOND
}

#[derive(Debug, Clone)]
struct ColumnPlan {
    var_type: VarType,
    format_class: Option<FormatClass>,
    offset: usize,
    width: usize,
    string_len_bytes: usize,
    label_map: Option<Arc<LabelMap>>,
    missing_set: Option<HashSet<String>>,
    missing_string_as_null: bool,
    fast_no_checks: bool,
    missing_range: bool,
    missing_doubles: Vec<f64>,
    missing_double_bits: Vec<u64>,
}

impl ColumnPlan {
    fn new(
        var: &Variable,
        offset: usize,
        width: usize,
        label_map: Option<Arc<LabelMap>>,
        missing_set: Option<HashSet<String>>,
        missing_string_as_null: bool,
    ) -> Self {
        let fast_no_checks = matches!(var.var_type, VarType::Str)
            && !missing_string_as_null
            && missing_set.is_none()
            && label_map.is_none();
        let string_len_bytes = if var.var_type == VarType::Str && var.string_len > 0 {
            var.string_len.min(width)
        } else {
            0
        };
        Self {
            var_type: var.var_type,
            format_class: var.format_class,
            offset,
            width,
            string_len_bytes,
            label_map,
            missing_set,
            missing_string_as_null,
            fast_no_checks,
            missing_range: var.missing_range,
            missing_doubles: var.missing_doubles.clone(),
            missing_double_bits: var.missing_double_bits.clone(),
        }
    }
}

enum ColumnBuilder {
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    Date(PrimitiveChunkedBuilder<Int32Type>),
    DateTime(PrimitiveChunkedBuilder<Int64Type>),
    Time(PrimitiveChunkedBuilder<Int64Type>),
    Utf8 {
        builder: StringChunkedBuilder,
        num_cache: Option<NumericStringCache>,
    },
}

impl ColumnBuilder {
    fn finish(self) -> Series {
        match self {
            ColumnBuilder::Float64(b) => b.finish().into_series(),
            ColumnBuilder::Date(b) => b.finish().into_date().into_series(),
            ColumnBuilder::DateTime(b) => b
                .finish()
                .into_datetime(TimeUnit::Milliseconds, None)
                .into_series(),
            ColumnBuilder::Time(b) => b.finish().into_time().into_series(),
            ColumnBuilder::Utf8 { builder, .. } => builder.finish().into_series(),
        }
    }
}

struct NumericStringCache {
    last_bits: u64,
    last_value: String,
    has_last: bool,
}

impl NumericStringCache {
    fn new() -> Self {
        Self {
            last_bits: 0,
            last_value: String::new(),
            has_last: false,
        }
    }
}

#[derive(Debug, Default)]
struct LabelMap {
    float_map: std::collections::HashMap<u64, String>,
    str_map: std::collections::HashMap<String, String>,
}

impl LabelMap {
    fn insert_float(&mut self, v: f64, label: String) {
        self.float_map.insert(v.to_bits(), label);
    }

    fn insert_str(&mut self, v: String, label: String) {
        self.str_map.insert(v, label);
    }

    fn get_float_bits(&self, bits: u64) -> Option<&String> {
        self.float_map.get(&bits)
    }

    fn get_str(&self, v: &str) -> Option<&String> {
        self.str_map.get(v)
    }
}

fn build_label_maps(
    metadata: &Metadata,
    needed_labels: Option<&HashSet<String>>,
) -> std::collections::HashMap<String, std::sync::Arc<LabelMap>> {
    let mut out = std::collections::HashMap::new();
    for vl in &metadata.value_labels {
        if let Some(needed) = needed_labels {
            if !needed.contains(&vl.name) {
                continue;
            }
        }
        let mut map = LabelMap::default();
        for (key, value) in &vl.mapping {
            match key {
                crate::spss::types::ValueLabelKey::Double(v) => {
                    map.insert_float(*v, value.clone());
                }
                crate::spss::types::ValueLabelKey::Str(s) => {
                    map.insert_str(s.clone(), value.clone());
                }
            }
        }
        out.insert(vl.name.clone(), std::sync::Arc::new(map));
    }
    out
}

#[derive(PartialEq, Eq)]
enum DecompressStatus {
    FinishedRow,
    FinishedAll,
}

struct SavRowDecompressor {
    control_chunk: [u8; 8],
    control_i: usize,
    missing_bytes: [u8; 8],
    bias: f64,
    endian: Endian,
}

impl SavRowDecompressor {
    fn new(endian: Endian, bias: f64) -> Self {
        let missing_bytes = match endian {
            Endian::Little => SAV_MISSING_DOUBLE.to_le_bytes(),
            Endian::Big => SAV_MISSING_DOUBLE.to_be_bytes(),
        };
        Self {
            control_chunk: [0u8; 8],
            control_i: 8,
            missing_bytes,
            bias,
            endian,
        }
    }

    fn read_row<R: Read>(
        &mut self,
        reader: &mut R,
        out: &mut [u8],
        record_len: usize,
    ) -> Result<DecompressStatus> {
        let mut out_pos = 0usize;
        while out_pos < record_len {
            let code = self.next_control_byte(reader)?;
            match code {
                0 => {}
                252 => return Ok(DecompressStatus::FinishedAll),
                253 => {
                    reader.read_exact(&mut out[out_pos..out_pos + 8])?;
                    out_pos += 8;
                }
                254 => {
                    out[out_pos..out_pos + 8].fill(b' ');
                    out_pos += 8;
                }
                255 => {
                    out[out_pos..out_pos + 8].copy_from_slice(&self.missing_bytes);
                    out_pos += 8;
                }
                v => {
                    let fp = (v as f64) - self.bias;
                    let bytes = match self.endian {
                        Endian::Little => fp.to_le_bytes(),
                        Endian::Big => fp.to_be_bytes(),
                    };
                    out[out_pos..out_pos + 8].copy_from_slice(&bytes);
                    out_pos += 8;
                }
            }
        }
        Ok(DecompressStatus::FinishedRow)
    }

    fn next_control_byte<R: Read>(&mut self, reader: &mut R) -> Result<u8> {
        if self.control_i == 8 {
            reader.read_exact(&mut self.control_chunk)?;
            self.control_i = 0;
        }
        let b = self.control_chunk[self.control_i];
        self.control_i += 1;
        Ok(b)
    }
}

enum StreamStatus {
    NeedData,
    FinishedRow,
    FinishedAll,
}

struct SavRowStream {
    control_chunk: [u8; 8],
    control_i: usize,
    missing_bytes: [u8; 8],
    bias: f64,
    endian: Endian,
}

impl SavRowStream {
    fn new(endian: Endian, bias: f64) -> Self {
        let missing_bytes = match endian {
            Endian::Little => SAV_MISSING_DOUBLE.to_le_bytes(),
            Endian::Big => SAV_MISSING_DOUBLE.to_be_bytes(),
        };
        Self {
            control_chunk: [0u8; 8],
            control_i: 8,
            missing_bytes,
            bias,
            endian,
        }
    }

    fn decompress_from_slice(
        &mut self,
        input: &[u8],
        input_offset: &mut usize,
        out: &mut [u8],
        out_offset: &mut usize,
    ) -> Result<StreamStatus> {
        loop {
            if *out_offset >= out.len() {
                return Ok(StreamStatus::FinishedRow);
            }

            if self.control_i == 8 {
                if *input_offset + 8 > input.len() {
                    return Ok(StreamStatus::NeedData);
                }
                self.control_chunk
                    .copy_from_slice(&input[*input_offset..*input_offset + 8]);
                *input_offset += 8;
                self.control_i = 0;
            }

            while self.control_i < 8 {
                let code = self.control_chunk[self.control_i];
                self.control_i += 1;
                match code {
                    0 => {}
                    252 => return Ok(StreamStatus::FinishedAll),
                    253 => {
                        if *input_offset + 8 > input.len() {
                            self.control_i -= 1;
                            return Ok(StreamStatus::NeedData);
                        }
                        out[*out_offset..*out_offset + 8]
                            .copy_from_slice(&input[*input_offset..*input_offset + 8]);
                        *input_offset += 8;
                        *out_offset += 8;
                    }
                    254 => {
                        out[*out_offset..*out_offset + 8].fill(b' ');
                        *out_offset += 8;
                    }
                    255 => {
                        out[*out_offset..*out_offset + 8].copy_from_slice(&self.missing_bytes);
                        *out_offset += 8;
                    }
                    v => {
                        let fp = (v as f64) - self.bias;
                        let bytes = match self.endian {
                            Endian::Little => fp.to_le_bytes(),
                            Endian::Big => fp.to_be_bytes(),
                        };
                        out[*out_offset..*out_offset + 8].copy_from_slice(&bytes);
                        *out_offset += 8;
                    }
                }

                if *out_offset >= out.len() {
                    return Ok(StreamStatus::FinishedRow);
                }
            }
        }
    }
}

fn read_zsav_data<R: Read + Seek>(
    reader: &mut R,
    endian: Endian,
    bias: f64,
    _record_len: usize,
    start_row: usize,
    end_row: usize,
    plans: &[ColumnPlan],
    builders: &mut [ColumnBuilder],
    numeric_plans: Option<&[NumericPlan]>,
    row_buf: &mut [u8],
    encoding: &'static encoding_rs::Encoding,
) -> Result<()> {
    let zheader_ofs = reader.stream_position()?;
    let zheader = read_zheader(reader, endian)?;
    if zheader.zheader_ofs != zheader_ofs {
        return Err(Error::ParseError("invalid zsav header offset".to_string()));
    }

    reader.seek(SeekFrom::Start(zheader.ztrailer_ofs))?;
    let ztrailer = read_ztrailer(reader, endian)?;
    let n_blocks = ztrailer.n_blocks as usize;

    let mut entries = Vec::with_capacity(n_blocks);
    for _ in 0..n_blocks {
        entries.push(read_ztrailer_entry(reader, endian)?);
    }

    let mut stream = SavRowStream::new(endian, bias);
    let mut row_idx = 0usize;
    let mut out_offset = 0usize;

    for entry in entries {
        reader.seek(SeekFrom::Start(entry.compressed_ofs as u64))?;
        let mut compressed = vec![0u8; entry.compressed_size as usize];
        reader.read_exact(&mut compressed)?;

        let mut decoder = ZlibDecoder::new(&compressed[..]);
        let mut uncompressed = Vec::with_capacity(entry.uncompressed_size as usize);
        decoder.read_to_end(&mut uncompressed)?;
        if uncompressed.len() != entry.uncompressed_size as usize {
            return Err(Error::ParseError("zsav block size mismatch".to_string()));
        }

        let mut input_offset = 0usize;
        while input_offset <= uncompressed.len() {
            let status = stream.decompress_from_slice(
                &uncompressed,
                &mut input_offset,
                row_buf,
                &mut out_offset,
            )?;
            match status {
                StreamStatus::NeedData => break,
                StreamStatus::FinishedAll => return Ok(()),
                StreamStatus::FinishedRow => {
                    if row_idx >= start_row && row_idx < end_row {
                        if let Some(numeric_plans) = numeric_plans {
                            append_numeric_row(builders, plans, numeric_plans, row_buf, endian)?;
                        } else {
                            append_row(builders, plans, row_buf, endian, encoding)?;
                        }
                    }
                    row_idx += 1;
                    out_offset = 0;
                    if row_idx >= end_row {
                        return Ok(());
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
struct ZHeader {
    zheader_ofs: u64,
    ztrailer_ofs: u64,
    ztrailer_len: u64,
}

#[allow(dead_code)]
struct ZTrailer {
    bias: i64,
    zero: i64,
    block_size: i32,
    n_blocks: i32,
}

#[allow(dead_code)]
struct ZTrailerEntry {
    uncompressed_ofs: i64,
    compressed_ofs: i64,
    uncompressed_size: i32,
    compressed_size: i32,
}

fn read_zheader<R: Read>(reader: &mut R, endian: Endian) -> Result<ZHeader> {
    Ok(ZHeader {
        zheader_ofs: read_u64(reader, endian)?,
        ztrailer_ofs: read_u64(reader, endian)?,
        ztrailer_len: read_u64(reader, endian)?,
    })
}

fn read_ztrailer<R: Read>(reader: &mut R, endian: Endian) -> Result<ZTrailer> {
    Ok(ZTrailer {
        bias: read_i64(reader, endian)?,
        zero: read_i64(reader, endian)?,
        block_size: read_i32_endian(reader, endian)?,
        n_blocks: read_i32_endian(reader, endian)?,
    })
}

fn read_ztrailer_entry<R: Read>(reader: &mut R, endian: Endian) -> Result<ZTrailerEntry> {
    Ok(ZTrailerEntry {
        uncompressed_ofs: read_i64(reader, endian)?,
        compressed_ofs: read_i64(reader, endian)?,
        uncompressed_size: read_i32_endian(reader, endian)?,
        compressed_size: read_i32_endian(reader, endian)?,
    })
}

fn read_u64<R: Read>(reader: &mut R, endian: Endian) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(match endian {
        Endian::Little => u64::from_le_bytes(buf),
        Endian::Big => u64::from_be_bytes(buf),
    })
}

fn read_i64<R: Read>(reader: &mut R, endian: Endian) -> Result<i64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(match endian {
        Endian::Little => i64::from_le_bytes(buf),
        Endian::Big => i64::from_be_bytes(buf),
    })
}

fn read_i32_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(match endian {
        Endian::Little => i32::from_le_bytes(buf),
        Endian::Big => i32::from_be_bytes(buf),
    })
}
