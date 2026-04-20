use crate::constants::{
    DATETIME_FORMATS, DATE_FORMATS, SAS_EPOCH_OFFSET_DAYS, SECONDS_PER_DAY, TIME_FORMATS,
};
use crate::data::DataReader;
use crate::error::{Error, Result};
use crate::page::PageReader;
use crate::reader::{data_reader_at_page_range, Sas7bdatReader};
use crate::types::{Column as SasColumn, ColumnType, Endian, Format, Header, Metadata};
use crate::value::Value;
use polars::prelude::*;
use std::cmp::min;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

const DEFAULT_SAS_BATCH_SIZE: usize = 8192;

/// Build a Polars DataFrame from rows of parsed values
pub struct DataFrameBuilder {
    columns_meta: Vec<SasColumn>,
    buffers: Vec<ColumnBuffer>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ColumnKind {
    Numeric,
    Date,
    DateTime,
    Time,
    Character,
}

/// Pre-computed plan for parsing a column directly from raw row bytes.
#[derive(Clone)]
pub(crate) struct ColumnPlan {
    pub start: usize,
    pub end: usize,
    pub kind: ColumnKind,
    pub endian: crate::types::Endian,
    pub encoding_byte: u8,
    pub encoding: &'static encoding_rs::Encoding,
    pub missing_string_as_null: bool,
    pub output_index: usize,
}

impl ColumnPlan {
    /// Build plans for the given columns.
    pub fn build_plans(
        metadata: &Metadata,
        col_indices: Option<&[usize]>,
        endian: crate::types::Endian,
        missing_string_as_null: bool,
    ) -> Vec<ColumnPlan> {
        let encoding = crate::encoding::get_encoding(metadata.encoding_byte);
        let columns: Vec<&SasColumn> = match col_indices {
            Some(indices) => indices.iter().map(|&i| &metadata.columns[i]).collect(),
            None => metadata.columns.iter().collect(),
        };
        let mut plans: Vec<ColumnPlan> = columns
            .iter()
            .enumerate()
            .map(|(output_index, col)| ColumnPlan {
                start: col.offset,
                end: col.offset + col.length,
                kind: kind_for_column(col),
                endian,
                encoding_byte: metadata.encoding_byte,
                encoding,
                missing_string_as_null,
                output_index,
            })
            .collect();
        // Improve cache locality by reading row bytes in offset order.
        plans.sort_by_key(|plan| plan.start);
        plans
    }
}

enum ColumnBuffer {
    Numeric(PrimitiveChunkedBuilder<Float64Type>),
    Date(PrimitiveChunkedBuilder<Int32Type>),
    DateTime(PrimitiveChunkedBuilder<Int64Type>),
    Time(PrimitiveChunkedBuilder<Int64Type>),
    Character(StringChunkedBuilder),
}

impl DataFrameBuilder {
    pub fn new(metadata: &Metadata, capacity: usize) -> Self {
        let columns_meta = metadata.columns.clone();
        let buffers = columns_meta
            .iter()
            .map(|col| ColumnBuffer::with_capacity(kind_for_column(col), &col.name, capacity))
            .collect();
        Self {
            columns_meta,
            buffers,
        }
    }

    pub fn new_with_columns(
        metadata: &Metadata,
        column_indices: &[usize],
        capacity: usize,
    ) -> Self {
        let columns_meta: Vec<SasColumn> = column_indices
            .iter()
            .map(|&idx| metadata.columns[idx].clone())
            .collect();
        let buffers = columns_meta
            .iter()
            .map(|col| ColumnBuffer::with_capacity(kind_for_column(col), &col.name, capacity))
            .collect();
        Self {
            columns_meta,
            buffers,
        }
    }

    pub fn add_row_ref(&mut self, row: &[Value]) -> Result<()> {
        if row.len() != self.columns_meta.len() {
            return Err(Error::ColumnCountMismatch {
                expected: self.columns_meta.len(),
                actual: row.len(),
            });
        }
        for (idx, value) in row.iter().enumerate() {
            let column = &self.columns_meta[idx];
            let buffer = &mut self.buffers[idx];
            push_value_ref(buffer, column, value);
        }
        Ok(())
    }

    /// Add a row directly from raw bytes, bypassing the Value enum entirely.
    /// `plans` must match the builder's columns in order.
    pub(crate) fn add_row_raw(&mut self, row_bytes: &[u8], plans: &[ColumnPlan]) {
        for plan in plans.iter() {
            let pos = plan.output_index;
            let start = plan.start;
            let end = plan.end;
            if end > row_bytes.len() {
                // Out of bounds → null
                match &mut self.buffers[pos] {
                    ColumnBuffer::Numeric(b) => b.append_null(),
                    ColumnBuffer::Date(b) => b.append_null(),
                    ColumnBuffer::DateTime(b) => b.append_null(),
                    ColumnBuffer::Time(b) => b.append_null(),
                    ColumnBuffer::Character(b) => b.append_null(),
                }
                continue;
            }
            match plan.kind {
                ColumnKind::Numeric
                | ColumnKind::Date
                | ColumnKind::DateTime
                | ColumnKind::Time => {
                    let (value, is_missing) = crate::value::decode_numeric_bytes_mask(
                        plan.endian,
                        &row_bytes[start..end],
                    );
                    match plan.kind {
                        ColumnKind::Numeric => {
                            if let ColumnBuffer::Numeric(b) = &mut self.buffers[pos] {
                                if is_missing {
                                    b.append_null();
                                } else {
                                    b.append_value(value);
                                }
                            }
                        }
                        ColumnKind::Date => {
                            if let ColumnBuffer::Date(b) = &mut self.buffers[pos] {
                                if is_missing {
                                    b.append_null();
                                } else {
                                    b.append_value(to_date_value(value));
                                }
                            }
                        }
                        ColumnKind::DateTime => {
                            if let ColumnBuffer::DateTime(b) = &mut self.buffers[pos] {
                                if is_missing {
                                    b.append_null();
                                } else {
                                    b.append_value(to_datetime_value(value));
                                }
                            }
                        }
                        ColumnKind::Time => {
                            if let ColumnBuffer::Time(b) = &mut self.buffers[pos] {
                                if is_missing {
                                    b.append_null();
                                } else {
                                    b.append_value(to_time_value(value));
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                ColumnKind::Character => {
                    if let ColumnBuffer::Character(b) = &mut self.buffers[pos] {
                        let bytes = &row_bytes[start..end];
                        // Trim trailing spaces and nulls
                        let mut trimmed_end = bytes.len();
                        while trimmed_end > 0
                            && (bytes[trimmed_end - 1] == b' ' || bytes[trimmed_end - 1] == 0)
                        {
                            trimmed_end -= 1;
                        }
                        // Stop at the first NUL to match ReadStat's C-string behavior
                        if let Some(pos) = bytes[..trimmed_end].iter().position(|&b| b == 0) {
                            trimmed_end = pos;
                        }
                        if trimmed_end == 0 {
                            if plan.missing_string_as_null {
                                b.append_null();
                            } else {
                                b.append_value("");
                            }
                        } else {
                            let s = crate::encoding::decode_string(
                                &bytes[..trimmed_end],
                                plan.encoding_byte,
                                plan.encoding,
                            );
                            b.append_value(&s);
                        }
                    }
                }
            }
        }
    }

    pub fn build(self) -> Result<DataFrame> {
        let mut columns = Vec::with_capacity(self.columns_meta.len());
        for (_column, buffer) in self.columns_meta.iter().zip(self.buffers.into_iter()) {
            let series = match buffer {
                ColumnBuffer::Numeric(builder) => builder.finish().into_series(),
                ColumnBuffer::Date(builder) => {
                    builder.finish().into_series().cast(&DataType::Date)?
                }
                ColumnBuffer::DateTime(builder) => builder
                    .finish()
                    .into_series()
                    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?,
                ColumnBuffer::Time(builder) => {
                    builder.finish().into_series().cast(&DataType::Time)?
                }
                ColumnBuffer::Character(builder) => builder.finish().into_series(),
            };
            columns.push(series.into());
        }
        DataFrame::new_infer_height(columns).map_err(|e| e.into())
    }
}

impl ColumnBuffer {
    fn with_capacity(kind: ColumnKind, name: &str, capacity: usize) -> Self {
        match kind {
            ColumnKind::Numeric => ColumnBuffer::Numeric(
                PrimitiveChunkedBuilder::<Float64Type>::new(name.into(), capacity),
            ),
            ColumnKind::Date => ColumnBuffer::Date(PrimitiveChunkedBuilder::<Int32Type>::new(
                name.into(),
                capacity,
            )),
            ColumnKind::DateTime => ColumnBuffer::DateTime(
                PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), capacity),
            ),
            ColumnKind::Time => ColumnBuffer::Time(PrimitiveChunkedBuilder::<Int64Type>::new(
                name.into(),
                capacity,
            )),
            ColumnKind::Character => {
                ColumnBuffer::Character(StringChunkedBuilder::new(name.into(), capacity))
            }
        }
    }
}

pub(crate) fn kind_for_column(column: &SasColumn) -> ColumnKind {
    match column.col_type {
        ColumnType::Character => ColumnKind::Character,
        ColumnType::Numeric => {
            // IMPORTANT: Check DATETIME before DATE since "DATETIME" starts with "DATE"
            if is_datetime_format(&column.format) {
                ColumnKind::DateTime
            } else if is_date_format(&column.format) {
                ColumnKind::Date
            } else if is_time_format(&column.format) {
                ColumnKind::Time
            } else {
                ColumnKind::Numeric
            }
        }
    }
}

fn push_value_ref(buffer: &mut ColumnBuffer, column: &SasColumn, value: &Value) {
    match (buffer, value, column.col_type) {
        (ColumnBuffer::Numeric(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(*v)
        }
        (ColumnBuffer::Date(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(to_date(*v))
        }
        (ColumnBuffer::DateTime(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(to_datetime(*v))
        }
        (ColumnBuffer::Time(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(to_time(*v))
        }
        (ColumnBuffer::Character(builder), Value::Character(v), ColumnType::Character) => {
            if let Some(s) = v {
                builder.append_value(s);
            } else {
                builder.append_null();
            }
        }
        (ColumnBuffer::Numeric(builder), _, _) => builder.append_null(),
        (ColumnBuffer::Date(builder), _, _) => builder.append_null(),
        (ColumnBuffer::DateTime(builder), _, _) => builder.append_null(),
        (ColumnBuffer::Time(builder), _, _) => builder.append_null(),
        (ColumnBuffer::Character(builder), _, _) => builder.append_null(),
    }
}

fn to_date(value: Option<f64>) -> Option<i32> {
    value.map(|sas_value| {
        let days_since_1970 = (sas_value as i32) - SAS_EPOCH_OFFSET_DAYS;
        if days_since_1970 >= -135080 && days_since_1970 <= 156935 {
            days_since_1970
        } else {
            (sas_value / SECONDS_PER_DAY as f64) as i32 - SAS_EPOCH_OFFSET_DAYS
        }
    })
}

fn to_date_value(sas_value: f64) -> i32 {
    let days_since_1970 = (sas_value as i32) - SAS_EPOCH_OFFSET_DAYS;
    if days_since_1970 >= -135080 && days_since_1970 <= 156935 {
        days_since_1970
    } else {
        (sas_value / SECONDS_PER_DAY as f64) as i32 - SAS_EPOCH_OFFSET_DAYS
    }
}

fn to_datetime(value: Option<f64>) -> Option<i64> {
    value.map(|sas_seconds| {
        let unix_seconds = sas_seconds - (SAS_EPOCH_OFFSET_DAYS as f64 * SECONDS_PER_DAY as f64);
        (unix_seconds * 1_000_000.0) as i64
    })
}

fn to_datetime_value(sas_seconds: f64) -> i64 {
    let unix_seconds = sas_seconds - (SAS_EPOCH_OFFSET_DAYS as f64 * SECONDS_PER_DAY as f64);
    (unix_seconds * 1_000_000.0) as i64
}

fn to_time(value: Option<f64>) -> Option<i64> {
    value.map(|sas_seconds| (sas_seconds * 1_000_000_000.0) as i64)
}

fn to_time_value(sas_seconds: f64) -> i64 {
    (sas_seconds * 1_000_000_000.0) as i64
}

/// Check if format string indicates a date column
fn is_date_format(format: &str) -> bool {
    if format.is_empty() {
        return false;
    }
    let upper = format.to_uppercase();
    DATE_FORMATS.iter().any(|&fmt| upper.starts_with(fmt))
}

/// Check if format string indicates a datetime column
fn is_datetime_format(format: &str) -> bool {
    if format.is_empty() {
        return false;
    }
    let upper = format.to_uppercase();
    DATETIME_FORMATS.iter().any(|&fmt| upper.starts_with(fmt))
}

/// Check if format string indicates a time column
fn is_time_format(format: &str) -> bool {
    if format.is_empty() {
        return false;
    }
    let upper = format.to_uppercase();
    TIME_FORMATS.iter().any(|&fmt| upper.starts_with(fmt))
}

// --- Anonymous Scan Implementation ---

pub struct SasScan {
    path: PathBuf,
    num_threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    compress_opts: crate::CompressOptionsLite,
    informative_nulls: Option<crate::InformativeNullOpts>,
}

impl SasScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        chunk_size: Option<usize>,
        preserve_order: bool,
        row_index_name: Option<String>,
        compress_opts: crate::CompressOptionsLite,
        informative_nulls: Option<crate::InformativeNullOpts>,
    ) -> Self {
        Self {
            path,
            num_threads: threads,
            missing_string_as_null,
            chunk_size,
            preserve_order,
            row_index_name,
            compress_opts,
            informative_nulls,
        }
    }
}

pub(crate) type SasBatchIter = Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>;

struct SlicedBatchIter {
    inner: SasBatchIter,
    skip: usize,
    remaining: usize,
}

impl Iterator for SlicedBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 {
            let df = self.inner.next()?;
            match df {
                Ok(df) => {
                    let height = df.height();
                    if self.skip >= height {
                        self.skip -= height;
                        continue;
                    }

                    let start = self.skip;
                    let take = (height - start).min(self.remaining);
                    self.skip = 0;
                    self.remaining -= take;

                    return Some(Ok(if start == 0 && take == height {
                        df
                    } else {
                        df.slice(start as i64, take)
                    }));
                }
                Err(err) => return Some(Err(err)),
            }
        }

        None
    }
}

// Unordered: all workers share one channel; batches arrive in completion order.
struct UnorderedParallelIter {
    rx: Option<mpsc::Receiver<PolarsResult<DataFrame>>>,
    handles: Vec<JoinHandle<()>>,
    row_index_name: Option<String>,
    row_cursor: usize,
}

impl Iterator for UnorderedParallelIter {
    type Item = PolarsResult<DataFrame>;
    fn next(&mut self) -> Option<Self::Item> {
        let df = self.rx.as_ref()?.recv().ok()?;
        Some(df.and_then(|df| {
            if let Some(ref name) = self.row_index_name {
                let n = df.height();
                let result = crate::append_row_index(df, name.as_str(), self.row_cursor);
                self.row_cursor += n;
                result
            } else {
                Ok(df)
            }
        }))
    }
}

impl Drop for UnorderedParallelIter {
    fn drop(&mut self) {
        self.rx.take();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

// Ordered: each worker has its own channel; consumer drains worker 0, then 1, etc.
struct OrderedParallelIter {
    channels: std::collections::VecDeque<mpsc::Receiver<PolarsResult<DataFrame>>>,
    handles: Vec<JoinHandle<()>>,
    row_index_name: Option<String>,
    row_cursor: usize,
}

impl Iterator for OrderedParallelIter {
    type Item = PolarsResult<DataFrame>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let front = self.channels.front()?;
            match front.recv() {
                Ok(df_result) => {
                    return Some(df_result.and_then(|df| {
                        if let Some(ref name) = self.row_index_name {
                            let n = df.height();
                            let result =
                                crate::append_row_index(df, name.as_str(), self.row_cursor);
                            self.row_cursor += n;
                            result
                        } else {
                            Ok(df)
                        }
                    }));
                }
                Err(_) => {
                    self.channels.pop_front();
                }
            }
        }
    }
}

impl Drop for OrderedParallelIter {
    fn drop(&mut self) {
        self.channels.clear();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

// For serial SAS paths (compressed files or single-thread): wraps SerialSasBatchIter
// in a background thread so IO can overlap with the consumer's processing.
struct SasBackgroundIter {
    rx: mpsc::Receiver<PolarsResult<DataFrame>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Iterator for SasBackgroundIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl Drop for SasBackgroundIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) struct SerialSasBatchIter {
    data_reader: DataReader<BufReader<File>>,
    metadata: Metadata,
    plans: Vec<ColumnPlan>,
    col_indices: Option<Vec<usize>>,
    batch_size: usize,
    remaining: usize,
    row_index_name: Option<String>,
    current_row: usize,
    // Informative-null state (empty/None when not tracking)
    null_opts: Option<crate::InformativeNullOpts>,
    indicator_plan_indices: Vec<usize>,
    indicator_names: Vec<String>,
    null_pairs: Vec<(String, String)>,
}

impl SerialSasBatchIter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        path: PathBuf,
        header: Header,
        metadata: Metadata,
        endian: Endian,
        format: Format,
        initial_data_subheaders: Vec<crate::data::DataSubheader>,
        col_indices: Option<Vec<usize>>,
        batch_size: usize,
        total: usize,
        missing_string_as_null: bool,
        null_opts: Option<crate::InformativeNullOpts>,
        skip: usize,
        row_index_name: Option<String>,
    ) -> PolarsResult<Self> {
        let mut file = BufReader::new(File::open(&path)?);
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let page_reader = PageReader::new(file, header, endian, format);
        let mut data_reader = DataReader::new(
            page_reader,
            metadata.clone(),
            endian,
            format,
            initial_data_subheaders,
        )
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        if skip > 0 {
            data_reader
                .skip_rows(skip)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        }

        let plans = ColumnPlan::build_plans(
            &metadata,
            col_indices.as_deref(),
            endian,
            missing_string_as_null,
        );

        // Compute indicator state when informative nulls are requested.
        let (indicator_plan_indices, indicator_names, null_pairs) =
            if let Some(ref opts) = null_opts {
                let active_cols: Vec<&str> = match col_indices.as_deref() {
                    Some(idx) => idx
                        .iter()
                        .map(|&i| metadata.columns[i].name.as_str())
                        .collect(),
                    None => metadata.columns.iter().map(|c| c.name.as_str()).collect(),
                };
                let eligible_cols: Vec<&str> = match col_indices.as_deref() {
                    Some(idx) => idx
                        .iter()
                        .map(|&i| &metadata.columns[i])
                        .filter(|c| c.col_type == crate::types::ColumnType::Numeric)
                        .map(|c| c.name.as_str())
                        .collect(),
                    None => metadata
                        .columns
                        .iter()
                        .filter(|c| c.col_type == crate::types::ColumnType::Numeric)
                        .map(|c| c.name.as_str())
                        .collect(),
                };
                let pairs = crate::informative_null_pairs(&active_cols, &eligible_cols, opts);
                let ind_map: std::collections::HashMap<&str, &str> = pairs
                    .iter()
                    .map(|(m, i)| (m.as_str(), i.as_str()))
                    .collect();
                let mut ipi = Vec::new();
                let mut inames = Vec::new();
                for (pi, plan) in plans.iter().enumerate() {
                    let col_name = match col_indices.as_deref() {
                        Some(idx) => &metadata.columns[idx[plan.output_index]].name,
                        None => &metadata.columns[plan.output_index].name,
                    };
                    if let Some(&ind_name) = ind_map.get(col_name.as_str()) {
                        ipi.push(pi);
                        inames.push(ind_name.to_string());
                    }
                }
                (ipi, inames, pairs)
            } else {
                (Vec::new(), Vec::new(), Vec::new())
            };

        Ok(Self {
            data_reader,
            metadata,
            plans,
            col_indices,
            batch_size,
            remaining: total,
            row_index_name,
            current_row: skip,
            null_opts,
            indicator_plan_indices,
            indicator_names,
            null_pairs,
        })
    }
}

impl Iterator for SerialSasBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let take = self.batch_size.min(self.remaining);
        let mut builder = match self.col_indices.as_deref() {
            Some(idx) => DataFrameBuilder::new_with_columns(&self.metadata, idx, take),
            None => DataFrameBuilder::new(&self.metadata, take),
        };

        // Set up indicator builders if tracking informative nulls.
        let has_inds = !self.indicator_plan_indices.is_empty();
        let mut ind_builders: Vec<StringChunkedBuilder> = if has_inds {
            self.indicator_names
                .iter()
                .map(|name| StringChunkedBuilder::new(name.as_str().into(), take))
                .collect()
        } else {
            Vec::new()
        };
        let plan_to_ind: Vec<Option<usize>> = if has_inds {
            let mut v = vec![None; self.plans.len()];
            for (i, &pi) in self.indicator_plan_indices.iter().enumerate() {
                v[pi] = Some(i);
            }
            v
        } else {
            Vec::new()
        };

        let mut read = 0usize;
        for _ in 0..take {
            match self.data_reader.read_row_borrowed() {
                Ok(Some(row_bytes)) => {
                    builder.add_row_raw(row_bytes, &self.plans);
                    if has_inds {
                        for (plan_pos, plan) in self.plans.iter().enumerate() {
                            let Some(ind_idx) = plan_to_ind[plan_pos] else {
                                continue;
                            };
                            let start = plan.start;
                            let end = plan.end;
                            let ind_builder = &mut ind_builders[ind_idx];
                            if end > row_bytes.len() {
                                ind_builder.append_null();
                                continue;
                            }
                            match plan.kind {
                                ColumnKind::Numeric
                                | ColumnKind::Date
                                | ColumnKind::DateTime
                                | ColumnKind::Time => {
                                    let (_val, offset) =
                                        crate::value::decode_numeric_bytes_mask_tagged(
                                            plan.endian,
                                            &row_bytes[start..end],
                                        );
                                    match offset {
                                        Some(off) => {
                                            let label = crate::value::sas_offset_to_label(off);
                                            ind_builder.append_value(&label);
                                        }
                                        None => ind_builder.append_null(),
                                    }
                                }
                                ColumnKind::Character => {
                                    ind_builder.append_null();
                                }
                            }
                        }
                    }
                    read += 1;
                }
                Ok(None) => break,
                Err(e) => return Some(Err(PolarsError::ComputeError(e.to_string().into()))),
            }
        }

        if read == 0 {
            self.remaining = 0;
            return None;
        }
        self.remaining = self.remaining.saturating_sub(read);
        let row_start = self.current_row;
        self.current_row = self.current_row.saturating_add(read);

        let base_df = match builder.build() {
            Ok(df) => df,
            Err(e) => return Some(Err(PolarsError::ComputeError(e.to_string().into()))),
        };

        let df = if !has_inds {
            Ok(base_df)
        } else {
            // Append indicator columns and apply informative null mode.
            let ind_series: Vec<Column> = ind_builders
                .into_iter()
                .map(|b| b.finish().into_series().into())
                .collect();
            let ind_df = match DataFrame::new_infer_height(ind_series) {
                Ok(df) => df,
                Err(e) => return Some(Err(e)),
            };
            let df_with_inds = match base_df.hstack(ind_df.columns()) {
                Ok(df) => df,
                Err(e) => return Some(Err(e)),
            };
            let null_opts = self.null_opts.as_ref().unwrap();
            crate::apply_informative_null_mode(df_with_inds, &null_opts.mode, &self.null_pairs)
        };

        let df = match df {
            Ok(df) => df,
            Err(e) => return Some(Err(e)),
        };

        if let Some(ref name) = self.row_index_name {
            Some(
                crate::append_row_index(df, name.as_str(), row_start)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into())),
            )
        } else {
            Some(Ok(df))
        }
    }
}

pub(crate) fn sas_batch_iter(
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    col_indices: Option<Vec<usize>>,
    offset: usize,
    n_rows: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    informative_nulls: Option<crate::InformativeNullOpts>,
) -> PolarsResult<SasBatchIter> {
    let reader =
        Sas7bdatReader::open(&path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    sas_batch_iter_with_reader(
        &reader,
        path,
        threads,
        missing_string_as_null,
        chunk_size,
        col_indices,
        offset,
        n_rows,
        preserve_order,
        row_index_name,
        informative_nulls,
    )
}

pub(crate) fn sas_batch_iter_with_reader(
    reader: &Sas7bdatReader,
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    col_indices: Option<Vec<usize>>,
    offset: usize,
    n_rows: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    informative_nulls: Option<crate::InformativeNullOpts>,
) -> PolarsResult<SasBatchIter> {
    let max_rows = reader.metadata().row_count.saturating_sub(offset);
    let total = n_rows.unwrap_or(max_rows).min(max_rows);
    let batch_size = chunk_size.unwrap_or(DEFAULT_SAS_BATCH_SIZE).max(1);
    let total_chunks = total.div_ceil(batch_size);
    let partial_read = offset > 0 || total < reader.metadata().row_count;

    if total_chunks == 0 {
        return Ok(Box::new(std::iter::empty()));
    }

    if let Some(ref name) = row_index_name {
        let collision = reader.metadata().columns.iter().any(|c| c.name == *name);
        if collision {
            return Err(PolarsError::ComputeError(
                format!("row_index_name '{name}' collides with existing column").into(),
            ));
        }
    }

    // When informative nulls are requested, always use the serial path (needs row-by-row decode).
    if let Some(null_opts) = informative_nulls {
        // Collision check
        let var_names: Vec<&str> = reader
            .metadata()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        let active_names: Vec<&str> = match col_indices.as_deref() {
            Some(idx) => idx.iter().map(|&i| var_names[i]).collect(),
            None => var_names.clone(),
        };
        let eligible_names: Vec<&str> = match col_indices.as_deref() {
            Some(idx) => idx
                .iter()
                .map(|&i| reader.metadata().columns[i].name.as_str())
                .filter(|_| true)
                .zip(
                    col_indices
                        .as_deref()
                        .unwrap()
                        .iter()
                        .map(|&i| &reader.metadata().columns[i]),
                )
                .filter(|(_, c)| c.col_type == crate::types::ColumnType::Numeric)
                .map(|(name, _)| name)
                .collect(),
            None => reader
                .metadata()
                .columns
                .iter()
                .filter(|c| c.col_type == crate::types::ColumnType::Numeric)
                .map(|c| c.name.as_str())
                .collect(),
        };
        let pairs = crate::informative_null_pairs(&active_names, &eligible_names, &null_opts);
        crate::check_informative_null_collisions(&var_names, &pairs)?;
        if let Some(ref name) = row_index_name {
            if pairs.iter().any(|(m, i)| m == name || i == name) {
                return Err(PolarsError::ComputeError(
                    format!("row_index_name '{name}' collides with informative-null column").into(),
                ));
            }
        }

        let header = reader.header().clone();
        let metadata = reader.metadata().clone();
        let endian = reader.endian();
        let format = reader.format();
        let initial_data_subheaders = reader.initial_data_subheaders().to_vec();
        let serial = SerialSasBatchIter::new(
            path.to_path_buf(),
            header,
            metadata,
            endian,
            format,
            initial_data_subheaders,
            col_indices,
            batch_size,
            total,
            missing_string_as_null,
            Some(null_opts),
            offset,
            row_index_name,
        )?;
        let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
        let handle = std::thread::spawn(move || {
            for batch in serial {
                if tx.send(batch).is_err() {
                    return;
                }
            }
        });
        return Ok(Box::new(SasBackgroundIter {
            rx,
            handle: Some(handle),
        }));
    }

    let n_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);
    // Uncompressed SAS is I/O-bound: hyperthreads don't help and cost extra RAM.
    // Default to min(Polars thread pool, physical cores); users can override with `threads`.
    let default_threads = if threads.is_none() {
        crate::default_thread_count().min(n_cpus)
    } else {
        n_cpus
    };
    let parallel_chunks = if partial_read {
        reader.metadata().row_count.div_ceil(batch_size)
    } else {
        total_chunks
    };
    let n_workers = min(
        threads.unwrap_or(default_threads).max(1),
        parallel_chunks.max(1),
    );

    // Compressed SAS still uses the serial decoder.
    if reader.metadata().compression != crate::Compression::None || n_workers <= 1 {
        let header = reader.header().clone();
        let metadata = reader.metadata().clone();
        let endian = reader.endian();
        let format = reader.format();
        let initial_data_subheaders = reader.initial_data_subheaders().to_vec();
        let serial = SerialSasBatchIter::new(
            path.to_path_buf(),
            header,
            metadata,
            endian,
            format,
            initial_data_subheaders,
            col_indices,
            batch_size,
            total,
            missing_string_as_null,
            None,
            offset,
            row_index_name,
        )?;
        let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
        let handle = std::thread::spawn(move || {
            for batch in serial {
                if tx.send(batch).is_err() {
                    return;
                }
            }
        });
        return Ok(Box::new(SasBackgroundIter {
            rx,
            handle: Some(handle),
        }));
    }
    // N independent readers, each owning a non-overlapping range of pages.
    // Page byte offsets are always exact (header_length + page_num * page_length),
    // so seeking is reliable regardless of page type (DATA or MIX).
    // Workers emit variable-size batches (≤ batch_size rows); the consumer reassembles.

    let header = reader.header().clone();
    let metadata = Arc::new(reader.metadata().clone());
    let row_length = metadata.row_length;
    let endian = reader.endian();
    let format = reader.format();

    let plans_arc = Arc::new(ColumnPlan::build_plans(
        &metadata,
        col_indices.as_deref(),
        endian,
        missing_string_as_null,
    ));

    // Derive total page count from file size — no I/O beyond stat(2).
    let total_pages = {
        let file_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let data_bytes = file_len.saturating_sub(header.header_length as u64);
        (data_bytes / header.page_length as u64) as usize
    };
    let first_data_page = reader.first_data_page();
    let mix_data_rows = reader.mix_data_rows();
    let data_pages = total_pages.saturating_sub(first_data_page);

    // Phase 1: serial reader for MIX-page rows (emits batches before any parallel work).
    let mix_iter: Option<SerialSasBatchIter> = if mix_data_rows > 0 {
        let initial_subs = reader.initial_data_subheaders().to_vec();
        Some(SerialSasBatchIter::new(
            path.clone(),
            header.clone(),
            metadata.as_ref().clone(),
            endian,
            format,
            initial_subs,
            col_indices.clone(),
            batch_size,
            mix_data_rows,
            missing_string_as_null,
            None,
            0,
            row_index_name.clone(),
        )?)
    } else {
        None
    };

    // If there are no DATA pages, the MIX iter (or empty) is the whole result.
    if data_pages == 0 {
        return Ok(match mix_iter {
            Some(iter) => Box::new(iter) as SasBatchIter,
            None => Box::new(std::iter::empty()) as SasBatchIter,
        });
    }

    // Phase 2: parallel workers for DATA pages only (starting at first_data_page).
    // row_start is 0 per worker — page budget is the sole stopping condition.
    let pages_per_worker = data_pages.div_ceil(n_workers);
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(n_workers);

    // Helper: spawn one worker thread that reads pages [page_start, page_start+page_count)
    // and sends each batch (≤ batch_size rows) down `tx`.
    macro_rules! spawn_worker {
        ($tx:expr, $worker_page_start:expr, $worker_page_count:expr) => {{
            let path = path.clone();
            let header = header.clone();
            let metadata = metadata.clone();
            let plans = plans_arc.clone();
            let col_indices = col_indices.clone();
            let tx = $tx;
            let worker_page_start: usize = $worker_page_start;
            let worker_page_count: usize = $worker_page_count;
            std::thread::spawn(move || {
                let mut data_reader = match data_reader_at_page_range(
                    &path,
                    &header,
                    &metadata,
                    endian,
                    format,
                    worker_page_start,
                    worker_page_count,
                    0,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                        return;
                    }
                };
                loop {
                    let mut buf = Vec::new();
                    let n_read = match data_reader.read_rows_bulk(batch_size, &mut buf) {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                            return;
                        }
                    };
                    if n_read == 0 {
                        break;
                    }
                    let mut builder = match col_indices.as_deref() {
                        Some(ci) => DataFrameBuilder::new_with_columns(&metadata, ci, n_read),
                        None => DataFrameBuilder::new(&metadata, n_read),
                    };
                    for i in 0..n_read {
                        let rs = i * row_length;
                        builder.add_row_raw(&buf[rs..rs + row_length], &plans);
                    }
                    match builder.build() {
                        Ok(df) => {
                            if tx.send(Ok(df)).is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                            return;
                        }
                    }
                }
            })
        }};
    }

    // Row index assignment happens in the iterator, not in workers, so the counter
    // is always exact based on realized row counts rather than geometry estimates.
    let row_index_start = mix_data_rows;

    // Row index and partial reads both require file-order output.
    let use_ordered = preserve_order || row_index_name.is_some() || partial_read;
    let base_iter: SasBatchIter = if use_ordered {
        let mut channels: std::collections::VecDeque<mpsc::Receiver<PolarsResult<DataFrame>>> =
            std::collections::VecDeque::with_capacity(n_workers);
        for worker_idx in 0..n_workers {
            let wp_start = first_data_page + worker_idx * pages_per_worker;
            if wp_start >= total_pages {
                break;
            }
            let wp_count = pages_per_worker.min(total_pages - wp_start);
            let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(4);
            channels.push_back(rx);
            handles.push(spawn_worker!(tx, wp_start, wp_count));
        }
        let parallel = OrderedParallelIter {
            channels,
            handles,
            row_index_name: row_index_name.clone(),
            row_cursor: row_index_start,
        };
        match mix_iter {
            Some(mix) => Box::new(mix.chain(parallel)) as SasBatchIter,
            None => Box::new(parallel) as SasBatchIter,
        }
    } else {
        let (shared_tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(n_workers * 4);
        for worker_idx in 0..n_workers {
            let wp_start = first_data_page + worker_idx * pages_per_worker;
            if wp_start >= total_pages {
                break;
            }
            let wp_count = pages_per_worker.min(total_pages - wp_start);
            handles.push(spawn_worker!(shared_tx.clone(), wp_start, wp_count));
        }
        drop(shared_tx);
        let parallel = UnorderedParallelIter {
            rx: Some(rx),
            handles,
            row_index_name: row_index_name.clone(),
            row_cursor: row_index_start,
        };
        match mix_iter {
            Some(mix) => Box::new(mix.chain(parallel)) as SasBatchIter,
            None => Box::new(parallel) as SasBatchIter,
        }
    };

    if partial_read {
        Ok(Box::new(SlicedBatchIter {
            inner: base_iter,
            skip: offset,
            remaining: total,
        }))
    } else {
        Ok(base_iter)
    }
}

impl AnonymousScan for SasScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let reader = Sas7bdatReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // Resolve Column Names -> Indices
        let col_indices = if let Some(cols) = opts.with_columns {
            let mut indices = Vec::with_capacity(cols.len());
            for name in cols.iter() {
                let idx = reader
                    .metadata()
                    .columns
                    .iter()
                    .position(|c| c.name == name.as_str())
                    .ok_or_else(|| {
                        // .to_string() creates an owned String, which satisfies the 'static requirement
                        let err_msg = name.as_str().to_string();
                        PolarsError::ColumnNotFound(err_msg.into())
                    })?;
                indices.push(idx);
            }
            Some(indices)
        } else {
            None
        };

        let iter = sas_batch_iter_with_reader(
            &reader,
            self.path.clone(),
            self.num_threads,
            self.missing_string_as_null,
            self.chunk_size,
            col_indices,
            0,
            opts.n_rows,
            self.preserve_order,
            self.row_index_name.clone(),
            self.informative_nulls.clone(),
        )?;

        let prefetch = crate::scan_prefetch::spawn_prefetcher(iter.map(|batch| batch));
        let mut out: Option<DataFrame> = None;
        while let Some(df) = prefetch.next()? {
            if let Some(acc) = out.as_mut() {
                acc.vstack_mut(&df)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            } else {
                out = Some(df);
            }
        }
        let df = out.unwrap_or_else(DataFrame::empty);
        if self.compress_opts.enabled {
            let compressed = crate::compress_df_if_enabled(&df, &self.compress_opts)
                .map_err(|e| PolarsError::ComputeError(e.into()))?;
            Ok(compressed)
        } else {
            Ok(df)
        }
    }

    // FIX: method signature updated to include Option<usize>
    fn schema(&self, _n_rows: Option<usize>) -> PolarsResult<SchemaRef> {
        let reader = Sas7bdatReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let cols = &reader.metadata().columns;
        let var_names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();

        // Build base schema
        let mut schema = Schema::with_capacity(cols.len());
        for col in cols.iter() {
            let dtype = match col.col_type {
                ColumnType::Numeric => {
                    // IMPORTANT: Check DATETIME before DATE since "DATETIME" starts with "DATE"
                    if is_datetime_format(&col.format) {
                        DataType::Datetime(TimeUnit::Microseconds, None)
                    } else if is_date_format(&col.format) {
                        DataType::Date
                    } else if is_time_format(&col.format) {
                        DataType::Time
                    } else {
                        DataType::Float64
                    }
                }
                ColumnType::Character => DataType::String,
            };
            schema.with_column(col.name.as_str().into(), dtype);
        }

        // If informative nulls requested, add/transform indicator columns
        if let Some(null_opts) = &self.informative_nulls {
            let eligible: Vec<&str> = cols
                .iter()
                .filter(|c| c.col_type == ColumnType::Numeric)
                .map(|c| c.name.as_str())
                .collect();
            let pairs = crate::informative_null_pairs(&var_names, &eligible, null_opts);
            crate::check_informative_null_collisions(&var_names, &pairs)?;

            use crate::InformativeNullMode;
            match &null_opts.mode {
                InformativeNullMode::SeparateColumn { .. } => {
                    // Interleave indicator String columns after each main col
                    let indicator_set: std::collections::HashSet<&str> =
                        pairs.iter().map(|(_, ind)| ind.as_str()).collect();
                    let main_to_ind: std::collections::HashMap<&str, &str> = pairs
                        .iter()
                        .map(|(m, i)| (m.as_str(), i.as_str()))
                        .collect();
                    let existing_names: Vec<String> =
                        schema.iter_names().map(|n| n.to_string()).collect();
                    let mut new_schema = Schema::with_capacity(existing_names.len() + pairs.len());
                    for name in &existing_names {
                        if indicator_set.contains(name.as_str()) {
                            continue;
                        }
                        let dt = schema.get(name.as_str()).unwrap().clone();
                        new_schema.with_column(name.as_str().into(), dt);
                        if let Some(&ind) = main_to_ind.get(name.as_str()) {
                            new_schema.with_column(ind.into(), DataType::String);
                        }
                    }
                    schema = new_schema;
                }
                InformativeNullMode::Struct => {
                    for (main, _ind) in &pairs {
                        let main_dt = schema
                            .get(main.as_str())
                            .cloned()
                            .unwrap_or(DataType::Float64);
                        schema.with_column(
                            main.as_str().into(),
                            DataType::Struct(vec![
                                Field::new(main.as_str().into(), main_dt),
                                Field::new("null_indicator".into(), DataType::String),
                            ]),
                        );
                    }
                }
                InformativeNullMode::MergedString => {
                    for (main, _ind) in &pairs {
                        schema.with_column(main.as_str().into(), DataType::String);
                    }
                }
            }
        }

        if let Some(ref name) = self.row_index_name {
            schema = crate::append_row_index_schema(schema, name.as_str())?;
        }

        Ok(Arc::new(schema))
    }
}

pub fn scan_sas7bdat(
    path: impl Into<std::path::PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let scan_ptr = Arc::new(SasScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        opts.chunk_size,
        opts.preserve_order.unwrap_or(false),
        opts.row_index_name,
        opts.compress_opts,
        opts.informative_nulls,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::sas_batch_iter;
    use std::path::PathBuf;

    fn small_sas_path() -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("sas")
            .join("data");
        let candidates = [
            base.join("data_pandas").join("test1.sas7bdat"),
            base.join("data_pandas").join("test2.sas7bdat"),
            base.join("test.sas7bdat"),
        ];
        for path in candidates {
            if path.exists() {
                return path;
            }
        }
        base.join("data_pandas").join("test1.sas7bdat")
    }

    #[test]
    fn test_sas_batch_streaming() {
        let path = small_sas_path();
        if !path.exists() {
            return;
        }
        let mut iter = sas_batch_iter(
            path,
            None,
            true,
            Some(10),
            None,
            0,
            Some(25),
            true,
            None,
            None,
        )
        .expect("batch iter");
        let mut batches = 0usize;
        let mut rows = 0usize;
        while let Some(batch) = iter.next() {
            let df = batch.expect("batch");
            rows += df.height();
            batches += 1;
        }
        assert!(batches >= 1);
        assert!(rows <= 25);
    }
}
