use crate::spss::data::read_data_frame_streaming;
use crate::spss::reader::SpssReader;
use crate::spss::types::FormatClass;
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::JoinHandle;

pub fn scan_sav(path: impl Into<PathBuf>, opts: crate::ScanOptions) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let preserve_order = opts.preserve_order.unwrap_or(false);
    let scan_ptr = Arc::new(SpssScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        opts.value_labels_as_strings,
        opts.chunk_size,
        preserve_order,
        opts.informative_nulls,
        opts.compress_opts,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::spss_batch_iter;
    use std::path::PathBuf;

    fn small_spss_path() -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("spss")
            .join("data");
        let candidates = [
            base.join("sample.sav"),
            base.join("labelled-num.sav"),
            base.join("missing_test.sav"),
        ];
        for path in candidates {
            if path.exists() {
                return path;
            }
        }
        base.join("sample.sav")
    }

    #[test]
    fn test_spss_batch_streaming() {
        let path = small_spss_path();
        if !path.exists() {
            return;
        }
        let mut iter = spss_batch_iter(
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

pub(crate) type SpssBatchIter = Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>;

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

struct ParallelSpssBatchIter {
    rx: Receiver<(usize, PolarsResult<DataFrame>)>,
    handle: Option<JoinHandle<()>>,
    preserve_order: bool,
    buffer: BTreeMap<usize, PolarsResult<DataFrame>>,
    next_idx: usize,
    total_chunks: usize,
}

impl Iterator for ParallelSpssBatchIter {
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

impl Drop for ParallelSpssBatchIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// For compressed SPSS files: reads the full range sequentially in a background
// thread (one pass, no re-seeking) and sends batches via a bounded channel.
// This avoids the O(N²) re-decompression cost of calling with_offset() per batch.
struct SpssBackgroundIter {
    rx: mpsc::Receiver<PolarsResult<DataFrame>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Iterator for SpssBackgroundIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl Drop for SpssBackgroundIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}



pub(crate) fn spss_batch_iter(
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
) -> PolarsResult<SpssBatchIter> {
    let reader =
        SpssReader::open(&path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    spss_batch_iter_with_reader(
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

pub(crate) fn spss_batch_iter_with_reader(
    reader: &SpssReader,
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
) -> PolarsResult<SpssBatchIter> {
    let max_rows = reader.metadata().row_count.saturating_sub(offset as u64) as usize;
    let total = n_rows.unwrap_or(max_rows).min(max_rows);
    let batch_size = chunk_size.unwrap_or(100_000).max(1);

    // Informative nulls: read once with indicators, then slice into batches in a background thread.
    if let Some(null_opts) = informative_nulls {
        use crate::spss::types::VarType;
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let compression = reader.compression();
        let bias = reader.header().bias;
        let cols_idx = cols
            .as_ref()
            .map(|names| {
                names
                    .iter()
                    .map(|name| {
                        metadata
                            .variables
                            .iter()
                            .position(|v| v.name == *name)
                            .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let var_names: Vec<&str> = metadata.variables.iter().map(|v| v.name.as_str()).collect();
        let eligible: Vec<&str> = metadata
            .variables
            .iter()
            .filter(|v| {
                (v.var_type == VarType::Numeric
                    && (!v.missing_doubles.is_empty() || v.missing_range))
                    || (v.var_type == VarType::Str && !v.missing_strings.is_empty())
            })
            .map(|v| v.name.as_str())
            .collect();
        let pairs = crate::informative_null_pairs(&var_names, &eligible, &null_opts);
        crate::check_informative_null_collisions(&var_names, &pairs)?;
        let pairs_map: std::collections::HashMap<&str, &str> = pairs
            .iter()
            .map(|(m, i)| (m.as_str(), i.as_str()))
            .collect();
        let effective_indices: Vec<usize> = cols_idx
            .clone()
            .unwrap_or_else(|| (0..metadata.variables.len()).collect());
        let indicator_col_names: Vec<Option<String>> = effective_indices
            .iter()
            .map(|&idx| {
                let vname = metadata.variables[idx].name.as_str();
                pairs_map.get(vname).map(|&ind| ind.to_string())
            })
            .collect();

        let path = Arc::new(path);
        let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
        let handle = std::thread::spawn(move || {
            let df = crate::spss::data::read_data_frame_with_indicators(
                &path,
                &metadata,
                endian,
                compression,
                bias,
                cols_idx.as_deref(),
                offset,
                total,
                missing_string_as_null,
                value_labels_as_strings,
                &indicator_col_names,
            )
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            .and_then(|df| crate::apply_informative_null_mode(df, &null_opts.mode, &pairs));

            let df = match df {
                Ok(df) => df,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };

            let mut start = 0usize;
            while start < df.height() {
                let take = batch_size.min(df.height() - start);
                let slice = df.slice(start as i64, take);
                if tx.send(Ok(slice)).is_err() {
                    return;
                }
                start += take;
            }
        });
        return Ok(Box::new(SpssBackgroundIter {
            rx,
            handle: Some(handle),
        }));
    }

    let n_threads = threads.unwrap_or(crate::default_thread_count()).max(1);
    if reader.compression() == 0 && n_threads > 1 && total >= 1000 {
        let total_chunks = (total + batch_size - 1) / batch_size;
        let n_workers = n_threads.min(total_chunks.max(1));
        let (tx, rx) = mpsc::sync_channel::<(usize, PolarsResult<DataFrame>)>(n_workers);
        let path = Arc::new(path);
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let bias = reader.header().bias;
        let cols_idx = cols
            .as_ref()
            .map(|names| {
                names
                    .iter()
                    .map(|name| {
                        metadata
                            .variables
                            .iter()
                            .position(|v| v.name == *name)
                            .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let missing_null = missing_string_as_null;
        let labels_as_strings = value_labels_as_strings;

        let ranges = split_batch_ranges(total_chunks, n_workers);

        let handle = std::thread::spawn(move || {
            let pool = match ThreadPoolBuilder::new().num_threads(n_workers).build() {
                Ok(pool) => pool,
                Err(e) => {
                    let _ = tx.send((0, Err(PolarsError::ComputeError(e.to_string().into()))));
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
                    let mut on_batch = |df: DataFrame| -> bool {
                        let idx = batch_start + local_idx;
                        local_idx += 1;
                        sender.send((idx, Ok(df))).is_ok()
                    };

                    let result = read_data_frame_streaming(
                        &path,
                        &metadata,
                        endian,
                        0,
                        bias,
                        cols_idx.as_deref(),
                        start_row,
                        range_rows,
                        missing_null,
                        labels_as_strings,
                        batch_size,
                        &mut on_batch,
                    )
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()));
                    if let Err(e) = result {
                        let _ = sender.send((batch_start + local_idx, Err(e)));
                    }
                });
            });
        });

        return Ok(Box::new(ParallelSpssBatchIter {
            rx,
            handle: Some(handle),
            preserve_order,
            buffer: BTreeMap::new(),
            next_idx: 0,
            total_chunks,
        }));
    }

    // Compressed SPSS: a background thread reads the full range in one sequential
    // pass and slices it into batches. Re-seeking through compressed data for each
    // batch would require re-decompressing from row 0, making it O(N²).
    if reader.compression() != 0 {
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let compression = reader.compression();
        let bias = reader.header().bias;
        let cols_idx = cols
            .as_ref()
            .map(|names| {
                names
                    .iter()
                    .map(|name| {
                        metadata
                            .variables
                            .iter()
                            .position(|v| v.name == *name)
                            .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let missing_null = missing_string_as_null;
        let labels = value_labels_as_strings;
        let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
        let handle = std::thread::spawn(move || {
            if let Err(e) = crate::spss::data::read_data_frame_streaming(
                &path,
                &metadata,
                endian,
                compression,
                bias,
                cols_idx.as_deref(),
                offset,
                total,
                missing_null,
                labels,
                batch_size,
                &mut |df| tx.send(Ok(df)).is_ok(),
            ) {
                let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
            }
        });
        return Ok(Box::new(SpssBackgroundIter {
            rx,
            handle: Some(handle),
        }));
    }

    // Uncompressed single-thread: stream on a background thread.
    let _ = preserve_order;
    let metadata = Arc::new(reader.metadata().clone());
    let endian = reader.endian();
    let compression = reader.compression();
    let bias = reader.header().bias;
    let cols_idx = cols
        .as_ref()
        .map(|names| {
            names
                .iter()
                .map(|name| {
                    metadata
                        .variables
                        .iter()
                        .position(|v| v.name == *name)
                        .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let missing_null = missing_string_as_null;
    let labels = value_labels_as_strings;
    let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
    let handle = std::thread::spawn(move || {
        if let Err(e) = crate::spss::data::read_data_frame_streaming(
            &path,
            &metadata,
            endian,
            compression,
            bias,
            cols_idx.as_deref(),
            offset,
            total,
            missing_null,
            labels,
            batch_size,
            &mut |df| tx.send(Ok(df)).is_ok(),
        ) {
            let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
        }
    });
    Ok(Box::new(SpssBackgroundIter {
        rx,
        handle: Some(handle),
    }))
}

pub struct SpssScan {
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
    preserve_order: bool,
    informative_nulls: Option<crate::InformativeNullOpts>,
    compress_opts: crate::CompressOptionsLite,
}

impl SpssScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        value_labels_as_strings: Option<bool>,
        chunk_size: Option<usize>,
        preserve_order: bool,
        informative_nulls: Option<crate::InformativeNullOpts>,
        compress_opts: crate::CompressOptionsLite,
    ) -> Self {
        Self {
            path,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            chunk_size,
            preserve_order,
            informative_nulls,
            compress_opts,
        }
    }
}

impl AnonymousScan for SpssScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let cols = opts
            .with_columns
            .map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let iter = spss_batch_iter(
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
        let reader = SpssReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let metadata = reader.metadata();
        let value_labels_as_strings = self.value_labels_as_strings.unwrap_or(true);
        let base_schema = build_schema(metadata, value_labels_as_strings);

        if let Some(null_opts) = &self.informative_nulls {
            let var_names: Vec<&str> =
                metadata.variables.iter().map(|v| v.name.as_str()).collect();
            let eligible: Vec<&str> = metadata
                .variables
                .iter()
                .filter(|v| {
                    use crate::spss::types::VarType;
                    (v.var_type == VarType::Numeric
                        && (!v.missing_doubles.is_empty() || v.missing_range))
                        || (v.var_type == VarType::Str && !v.missing_strings.is_empty())
                })
                .map(|v| v.name.as_str())
                .collect();
            let pairs = crate::informative_null_pairs(&var_names, &eligible, null_opts);
            crate::check_informative_null_collisions(&var_names, &pairs)?;
            Ok(Arc::new(crate::build_indicator_schema(base_schema, &pairs, &null_opts.mode)))
        } else {
            Ok(Arc::new(base_schema))
        }
    }
}

fn build_schema(metadata: &crate::spss::types::Metadata, value_labels_as_strings: bool) -> Schema {
    let mut schema = Schema::with_capacity(metadata.variables.len());
    for var in &metadata.variables {
        let dtype = if value_labels_as_strings && var.value_label.is_some() {
            DataType::String
        } else {
            match var.var_type {
                crate::spss::types::VarType::Numeric => match var.format_class {
                    Some(FormatClass::Date) => DataType::Date,
                    Some(FormatClass::DateTime) => DataType::Datetime(TimeUnit::Milliseconds, None),
                    Some(FormatClass::Time) => DataType::Time,
                    None => DataType::Float64,
                },
                crate::spss::types::VarType::Str => DataType::String,
            }
        };
        schema.with_column(var.name.as_str().into(), dtype);
    }
    schema
}
