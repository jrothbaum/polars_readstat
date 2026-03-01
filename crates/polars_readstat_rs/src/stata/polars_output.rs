use crate::stata::data::{
    build_shared_decode, read_data_frame_range, read_data_frame_range_with_indicators,
    read_data_frame_streaming,
};
use crate::stata::reader::StataReader;
use crate::stata::types::{NumericType, VarType};
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::JoinHandle;

pub fn scan_dta(
    path: impl Into<std::path::PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let value_labels_as_strings = opts.value_labels_as_strings;
    let preserve_order = opts.preserve_order.unwrap_or(false);
    let scan_ptr = Arc::new(StataScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        value_labels_as_strings,
        opts.chunk_size,
        preserve_order,
        opts.compress_opts,
        opts.informative_nulls,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::stata_batch_iter;
    use std::path::PathBuf;

    fn small_stata_path() -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("stata")
            .join("data");
        let candidates = [
            base.join("sample.dta"),
            base.join("missing_test.dta"),
            base.join("sample_pyreadstat.dta"),
        ];
        for path in candidates {
            if path.exists() {
                return path;
            }
        }
        base.join("sample.dta")
    }

    #[test]
    fn test_stata_batch_streaming() {
        let path = small_stata_path();
        if !path.exists() {
            return;
        }
        let mut iter = stata_batch_iter(
            path,
            None,
            true,
            true,
            Some(10),
            false,
            None,
            0,
            Some(25),
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

pub struct StataScan {
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
    preserve_order: bool,
    compress_opts: crate::CompressOptionsLite,
    informative_nulls: Option<crate::InformativeNullOpts>,
}

impl StataScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        value_labels_as_strings: Option<bool>,
        chunk_size: Option<usize>,
        preserve_order: bool,
        compress_opts: crate::CompressOptionsLite,
        informative_nulls: Option<crate::InformativeNullOpts>,
    ) -> Self {
        Self {
            path,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            chunk_size,
            preserve_order,
            compress_opts,
            informative_nulls,
        }
    }
}

pub(crate) type StataBatchIter = Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>;

fn split_batch_ranges(total_batches: usize, n_workers: usize) -> Vec<(usize, usize)> {
    if total_batches == 0 || n_workers == 0 {
        return Vec::new();
    }
    let n = n_workers.min(total_batches);
    let base = total_batches / n;
    let rem = total_batches % n;
    let mut ranges = Vec::with_capacity(n);
    let mut start = 0usize;
    for i in 0..n {
        let len = base + if i < rem { 1 } else { 0 };
        ranges.push((start, len));
        start += len;
    }
    ranges
}

struct ParallelStataBatchIter {
    rx: Receiver<(usize, PolarsResult<DataFrame>)>,
    handle: Option<JoinHandle<()>>,
    preserve_order: bool,
    buffer: BTreeMap<usize, PolarsResult<DataFrame>>,
    next_idx: usize,
    total_chunks: usize,
}

impl Iterator for ParallelStataBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.preserve_order {
            return self.rx.recv().ok().map(|(_, df)| df);
        }
        if self.next_idx >= self.total_chunks {
            return None;
        }
        loop {
            if let Some(item) = self.buffer.remove(&self.next_idx) {
                self.next_idx += 1;
                return Some(item);
            }
            match self.rx.recv() {
                Ok((idx, df)) => {
                    self.buffer.insert(idx, df);
                }
                Err(_) => {
                    if let Some(item) = self.buffer.remove(&self.next_idx) {
                        self.next_idx += 1;
                        return Some(item);
                    }
                    return None;
                }
            }
        }
    }
}

impl Drop for ParallelStataBatchIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// For serial Stata paths: background thread builds SharedDecode once (avoids
// re-reading the StrL table per batch) and calls read_data_frame_range with
// O(1) byte seeks for each batch (fixed-width records).
struct StataBackgroundIter {
    rx: Receiver<PolarsResult<DataFrame>>,
    handle: Option<JoinHandle<()>>,
}

impl Iterator for StataBackgroundIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl Drop for StataBackgroundIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}


pub(crate) fn stata_batch_iter(
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
    preserve_order: bool,
    cols: Option<Vec<String>>,
    offset: usize,
    n_rows: Option<usize>,
    informative_nulls: Option<crate::InformativeNullOpts>,
) -> PolarsResult<StataBatchIter> {
    let reader =
        StataReader::open(&path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    stata_batch_iter_with_reader(
        &reader,
        path,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        chunk_size,
        preserve_order,
        cols,
        offset,
        n_rows,
        informative_nulls,
    )
}

pub(crate) fn stata_batch_iter_with_reader(
    reader: &StataReader,
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
    preserve_order: bool,
    cols: Option<Vec<String>>,
    offset: usize,
    n_rows: Option<usize>,
    informative_nulls: Option<crate::InformativeNullOpts>,
) -> PolarsResult<StataBatchIter> {
    let max_rows = reader
        .metadata()
        .row_count
        .saturating_sub(offset as u64) as usize;
    let total = n_rows.unwrap_or(max_rows).min(max_rows);
    let batch_size = chunk_size.unwrap_or(100_000).max(1);
    let selected = cols
        .as_ref()
        .map(|c| c.iter().cloned().collect::<HashSet<_>>());
    let col_indices = cols
        .as_ref()
        .map(|names| {
            names
                .iter()
                .map(|name| {
                    reader
                        .metadata()
                        .variables
                        .iter()
                        .position(|v| v.name == *name)
                        .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let mut time_formats = Vec::new();
    for var in &reader.metadata().variables {
        if let Some(kind) = stata_time_format_kind(var.format.as_deref(), &var.var_type) {
            if selected
                .as_ref()
                .map(|s| s.contains(&var.name))
                .unwrap_or(true)
            {
                time_formats.push((var.name.clone(), kind));
            }
        }
    }

    let n_threads = threads.unwrap_or(crate::default_thread_count()).max(1);
    if informative_nulls.is_none() && n_threads > 1 && total >= 1000 {
        let total_chunks = (total + batch_size - 1) / batch_size;
        let n_workers = n_threads.min(total_chunks.max(1));
        let (tx, rx) = mpsc::sync_channel::<(usize, PolarsResult<DataFrame>)>(n_workers);
        let path = Arc::new(path);
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.header().endian;
        let version = reader.header().version;
        let cols_idx = col_indices;
        let formats = Arc::new(time_formats);
        let missing_null = missing_string_as_null;
        let labels_as_strings = value_labels_as_strings;
        let shared = build_shared_decode(&path, &metadata, endian, version, labels_as_strings)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let shared = Arc::new(shared);

        let ranges = split_batch_ranges(total_chunks, n_workers);

        let handle = std::thread::spawn(move || {
            let pool = match ThreadPoolBuilder::new().num_threads(n_workers).build() {
                Ok(pool) => pool,
                Err(e) => {
                    let _ = tx.send((
                        0,
                        Err(PolarsError::ComputeError(e.to_string().into())),
                    ));
                    return;
                }
            };
            pool.install(|| {
                ranges.into_par_iter().for_each_with(tx, |sender, (batch_start, batch_count)| {
                    if batch_count == 0 {
                        return;
                    }
                    let start_row = offset + batch_start * batch_size;
                    if start_row >= offset + total {
                        return;
                    }
                    let range_rows =
                        (batch_count * batch_size).min(total - batch_start * batch_size);
                    let mut local_idx = 0usize;
                    let mut on_batch = |mut df: DataFrame| -> bool {
                        if let Err(e) = apply_stata_time_formats(&mut df, &formats) {
                            let _ = sender.send((
                                batch_start + local_idx,
                                Err(PolarsError::ComputeError(e.to_string().into())),
                            ));
                            return false;
                        }
                        let idx = batch_start + local_idx;
                        local_idx += 1;
                        sender.send((idx, Ok(df))).is_ok()
                    };

                    let result = read_data_frame_streaming(
                        &path,
                        &metadata,
                        endian,
                        version,
                        cols_idx.as_deref(),
                        start_row,
                        range_rows,
                        missing_null,
                        labels_as_strings,
                        &shared,
                        batch_size,
                        &mut on_batch,
                    );
                    if let Err(e) = result {
                        let _ = sender.send((
                            batch_start + local_idx,
                            Err(PolarsError::ComputeError(e.to_string().into())),
                        ));
                    }
                });
            });
        });

        return Ok(Box::new(ParallelStataBatchIter {
            rx,
            handle: Some(handle),
            preserve_order,
            buffer: BTreeMap::new(),
            next_idx: 0,
            total_chunks,
        }));
    }

    let _ = preserve_order;

    // Serial path: background thread builds SharedDecode once (avoids re-loading
    // StrL tables per batch) and reads with O(1) byte seeks (fixed-width records).
    let path = Arc::new(path);
    let metadata = Arc::new(reader.metadata().clone());
    let endian = reader.header().endian;
    let version = reader.header().version;
    let formats = Arc::new(time_formats);
    let missing_null = missing_string_as_null;
    let labels = value_labels_as_strings;
    let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);

    if let Some(null_opts) = informative_nulls {
        // Compute pairs/indicator_set before spawning so errors surface immediately.
        let var_names: Vec<String> = metadata.variables.iter().map(|v| v.name.clone()).collect();
        let var_name_refs: Vec<&str> = var_names.iter().map(|s| s.as_str()).collect();
        let eligible: Vec<&str> = metadata.variables.iter()
            .filter(|v| matches!(v.var_type, VarType::Numeric(_)))
            .map(|v| v.name.as_str())
            .collect();
        let pairs = crate::informative_null_pairs(&var_name_refs, &eligible, &null_opts);
        crate::check_informative_null_collisions(&var_name_refs, &pairs)?;
        let indicator_set: std::collections::HashSet<String> =
            pairs.iter().map(|(m, _)| m.clone()).collect();
        let suffix = match &null_opts.mode {
            crate::InformativeNullMode::SeparateColumn { suffix } => suffix.clone(),
            _ => "_null".to_string(),
        };
        let col_indices_clone = col_indices;
        let path_clone = path.clone();
        let metadata_clone = metadata.clone();
        let formats_clone = formats.clone();
        let handle = std::thread::spawn(move || {
            let shared = match build_shared_decode(&path_clone, &metadata_clone, endian, version, labels) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                    return;
                }
            };
            let mut cur_offset = offset;
            let mut remaining = total;
            while remaining > 0 {
                let take = batch_size.min(remaining);
                let result = read_data_frame_range_with_indicators(
                    &path_clone, &metadata_clone, endian, version,
                    col_indices_clone.as_deref(), cur_offset, take, missing_null, labels,
                    &shared, &indicator_set, null_opts.use_value_labels, &suffix,
                )
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                .and_then(|mut df| {
                    apply_stata_time_formats(&mut df, &formats_clone)?;
                    Ok(df)
                })
                .and_then(|df| crate::apply_informative_null_mode(df, &null_opts.mode, &pairs));
                if tx.send(result).is_err() {
                    return;
                }
                cur_offset += take;
                remaining -= take;
            }
        });
        return Ok(Box::new(StataBackgroundIter { rx, handle: Some(handle) }));
    }

    // Normal serial: build SharedDecode once, then read one batch at a time.
    let handle = std::thread::spawn(move || {
        let shared = match build_shared_decode(&path, &metadata, endian, version, labels) {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                return;
            }
        };
        let mut cur_offset = offset;
        let mut remaining = total;
        while remaining > 0 {
            let take = batch_size.min(remaining);
            let result = read_data_frame_range(
                &path, &metadata, endian, version, col_indices.as_deref(),
                cur_offset, take, missing_null, labels, &shared,
            )
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            .and_then(|mut df| {
                apply_stata_time_formats(&mut df, &formats)?;
                Ok(df)
            });
            if tx.send(result).is_err() {
                return;
            }
            cur_offset += take;
            remaining -= take;
        }
    });
    Ok(Box::new(StataBackgroundIter { rx, handle: Some(handle) }))
}

impl AnonymousScan for StataScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let cols = opts
            .with_columns
            .map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let iter = stata_batch_iter(
            self.path.clone(),
            self.threads,
            self.missing_string_as_null,
            self.value_labels_as_strings.unwrap_or(true),
            self.chunk_size,
            self.preserve_order,
            cols,
            0,
            opts.n_rows,
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

    fn schema(&self, _n_rows: Option<usize>) -> PolarsResult<SchemaRef> {
        let reader = StataReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut schema = Schema::with_capacity(reader.metadata().variables.len());
        for var in &reader.metadata().variables {
            let use_labels = self.value_labels_as_strings.unwrap_or(true);
            let dtype = if use_labels && var.value_label_name.is_some() {
                DataType::String
            } else {
                if let Some(kind) = stata_time_format_kind(var.format.as_deref(), &var.var_type) {
                    match kind {
                        StataTimeFormatKind::Date => DataType::Date,
                        StataTimeFormatKind::DateTime => {
                            DataType::Datetime(TimeUnit::Milliseconds, None)
                        }
                        StataTimeFormatKind::Time { .. } => DataType::Time,
                    }
                } else {
                    match var.var_type {
                        VarType::Numeric(NumericType::Byte) => DataType::Int8,
                        VarType::Numeric(NumericType::Int) => DataType::Int16,
                        VarType::Numeric(NumericType::Long) => DataType::Int32,
                        VarType::Numeric(NumericType::Float) => DataType::Float32,
                        VarType::Numeric(NumericType::Double) => DataType::Float64,
                        VarType::Str(_) | VarType::StrL => DataType::String,
                    }
                }
            };
            schema.with_column(var.name.as_str().into(), dtype);
        }

        if let Some(ref null_opts) = self.informative_nulls {
            let var_names: Vec<&str> = reader
                .metadata()
                .variables
                .iter()
                .map(|v| v.name.as_str())
                .collect();
            let eligible: Vec<&str> = reader
                .metadata()
                .variables
                .iter()
                .filter(|v| matches!(v.var_type, VarType::Numeric(_)))
                .map(|v| v.name.as_str())
                .collect();
            let pairs = crate::informative_null_pairs(&var_names, &eligible, null_opts);
            crate::check_informative_null_collisions(&var_names, &pairs)?;
            return Ok(Arc::new(crate::build_indicator_schema(schema, &pairs, &null_opts.mode)));
        }

        Ok(Arc::new(schema))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StataTimeFormatKind {
    Date,
    DateTime,
    Time { null_on_datetime: bool },
}

fn allow_date(var_type: &VarType) -> bool {
    matches!(
        var_type,
        VarType::Numeric(NumericType::Byte)
            | VarType::Numeric(NumericType::Int)
            | VarType::Numeric(NumericType::Long)
            | VarType::Numeric(NumericType::Float)
            | VarType::Numeric(NumericType::Double)
    )
}

fn allow_datetime(var_type: &VarType) -> bool {
    matches!(
        var_type,
        VarType::Numeric(NumericType::Long)
            | VarType::Numeric(NumericType::Float)
            | VarType::Numeric(NumericType::Double)
    )
}

pub(crate) fn stata_time_format_kind(
    format: Option<&str>,
    var_type: &VarType,
) -> Option<StataTimeFormatKind> {
    let fmt = format?.trim();
    if fmt.starts_with("%t") {
        let mut chars = fmt.chars();
        chars.next();
        chars.next();
        let unit = chars.next()?;
        return match unit {
            'c' | 'C' => {
                let rest: String = chars.collect();
                if rest.is_empty() {
                    if allow_datetime(var_type) {
                        Some(StataTimeFormatKind::DateTime)
                    } else {
                        None
                    }
                } else {
                    if allow_datetime(var_type) {
                        let has_date_tokens = rest
                            .chars()
                            .any(|c| matches!(c, 'C' | 'c' | 'Y' | 'y' | 'N' | 'n' | 'D' | 'd'));
                        Some(StataTimeFormatKind::Time {
                            null_on_datetime: has_date_tokens,
                        })
                    } else {
                        None
                    }
                }
            }
            'd' | 'w' | 'm' | 'q' | 'h' | 'y' => {
                if allow_date(var_type) {
                    Some(StataTimeFormatKind::Date)
                } else {
                    None
                }
            }
            _ => None,
        };
    }

    if fmt.starts_with('%') {
        let mut chars = fmt.chars();
        chars.next();
        let unit = chars.next()?;
        return match unit {
            'c' | 'C' => {
                if allow_datetime(var_type) {
                    Some(StataTimeFormatKind::DateTime)
                } else {
                    None
                }
            }
            'd' | 'w' | 'm' | 'q' | 'h' | 'y' => {
                if allow_date(var_type) {
                    Some(StataTimeFormatKind::Date)
                } else {
                    None
                }
            }
            _ => None,
        };
    }

    None
}

pub(crate) fn apply_stata_time_formats(
    df: &mut DataFrame,
    formats: &[(String, StataTimeFormatKind)],
) -> PolarsResult<()> {
    if formats.is_empty() {
        return Ok(());
    }
    let offset_days: i64 = 3653;
    let offset_ms: i64 = offset_days * 86_400_000;
    let day_ms: i64 = 86_400_000;
    let mut exprs = Vec::with_capacity(formats.len());
    for (name, kind) in formats {
        let dtype_ok = df
            .column(name)
            .map(|s| s.dtype().is_numeric())
            .unwrap_or(false);
        if !dtype_ok {
            continue;
        }
        let expr = match kind {
            StataTimeFormatKind::Date => (col(name).cast(DataType::Int64) - lit(offset_days))
                .cast(DataType::Date)
                .alias(name),
            StataTimeFormatKind::DateTime => (col(name).cast(DataType::Int64) - lit(offset_ms))
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias(name),
            StataTimeFormatKind::Time { null_on_datetime } => {
                if *null_on_datetime {
                    lit(NULL).cast(DataType::Time).alias(name)
                } else {
                    ((col(name).cast(DataType::Int64) % lit(day_ms) + lit(day_ms)) % lit(day_ms)
                        * lit(1_000_000i64))
                    .cast(DataType::Time)
                    .alias(name)
                }
            }
        };
        exprs.push(expr);
    }
    let df_owned = std::mem::take(df);
    *df = df_owned
        .lazy()
        .with_columns(exprs)
        .collect()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    Ok(())
}
