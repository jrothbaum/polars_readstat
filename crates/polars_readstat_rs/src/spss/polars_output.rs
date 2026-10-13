use crate::source::ReadSource;
use crate::spss::data::read_data_frame_streaming;
use crate::spss::reader::SpssReader;
use crate::spss::types::FormatClass;
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;

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
        opts.row_index_name,
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
        crate::disconnect_receiver(&mut self.rx);
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
    row_index_name: Option<String>,
    cols: Option<Vec<String>>,
    offset: usize,
    n_rows: Option<usize>,
    informative_nulls: Option<crate::InformativeNullOpts>,
) -> PolarsResult<SpssBatchIter> {
    let reader =
        SpssReader::open(&path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let source = reader.source();
    spss_batch_iter_with_reader(
        &reader,
        source,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        chunk_size,
        preserve_order,
        row_index_name,
        cols,
        offset,
        n_rows,
        informative_nulls,
    )
}

pub(crate) fn spss_batch_iter_with_reader(
    reader: &SpssReader,
    source: Arc<dyn ReadSource>,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    cols: Option<Vec<String>>,
    offset: usize,
    n_rows: Option<usize>,
    informative_nulls: Option<crate::InformativeNullOpts>,
) -> PolarsResult<SpssBatchIter> {
    let max_rows = reader.metadata().row_count.saturating_sub(offset as u64) as usize;
    let total = n_rows.unwrap_or(max_rows).min(max_rows);
    let batch_size = chunk_size.map(|n| n.max(1)).unwrap_or_else(|| {
        let row_bytes = reader.metadata().variables.iter().map(|v| v.width * 8).sum();
        crate::default_batch_rows(row_bytes)
    });
    let n_threads = threads.unwrap_or(crate::default_thread_count()).max(1);

    if total == 0 {
        return Ok(Box::new(std::iter::empty()));
    }

    crate::check_row_index_collision(
        row_index_name.as_deref(),
        reader.metadata().variables.iter().map(|v| v.name.as_str()),
    )?;

    // Informative nulls: read once with indicators, then slice into batches in a background thread.
    if let Some(null_opts) = informative_nulls {
        use crate::spss::types::VarType;
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let compression = reader.compression();
        let bias = reader.header().bias;
        let cols_idx = crate::resolve_column_indices(
            metadata.variables.iter().map(|v| v.name.as_str()),
            cols.as_deref(),
        )?;
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
        if let Some(ref name) = row_index_name {
            if pairs.iter().any(|(m, i)| m == name || i == name) {
                return Err(PolarsError::ComputeError(
                    format!("row_index_name '{name}' collides with informative-null column").into(),
                ));
            }
        }
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

        let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
        let handle = std::thread::spawn(move || {
            let df = crate::spss::data::read_data_frame_with_indicators(
                source.as_ref(),
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
                let mut slice = df.slice(start as i64, take);
                if let Some(ref name) = row_index_name {
                    let row_start = offset + start;
                    match crate::append_row_index(slice, name.as_str(), row_start) {
                        Ok(with_idx) => slice = with_idx,
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return;
                        }
                    }
                }
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

    if reader.compression() == 0 && n_threads > 1 && total >= 1000 {
        let total_chunks = (total + batch_size - 1) / batch_size;
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let bias = reader.header().bias;
        let cols_idx = crate::resolve_column_indices(
            metadata.variables.iter().map(|v| v.name.as_str()),
            cols.as_deref(),
        )?;

        // One job per batch: rows [start_row, start_row + batch_size).
        let job = move |chunk: usize| -> PolarsResult<Vec<DataFrame>> {
            let start_row = offset + chunk * batch_size;
            let rows = batch_size.min(offset + total - start_row);
            let mut batches = Vec::with_capacity(1);
            read_data_frame_streaming(
                source.as_ref(),
                &metadata,
                endian,
                0,
                bias,
                cols_idx.as_deref(),
                start_row,
                rows,
                missing_string_as_null,
                value_labels_as_strings,
                batch_size,
                &mut |df| {
                    batches.push(df);
                    true
                },
            )
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            let Some(name) = row_index_name.as_deref() else {
                return Ok(batches);
            };
            let mut row_start = start_row;
            batches
                .into_iter()
                .map(|df| {
                    let height = df.height();
                    let df = crate::append_row_index(df, name, row_start)?;
                    row_start += height;
                    Ok(df)
                })
                .collect()
        };

        return Ok(Box::new(crate::parallel::ParallelBatches::new(
            total_chunks,
            n_threads,
            preserve_order,
            Arc::new(job),
        )));
    }

    // Compressed SPSS: a background thread reads the full range in one sequential
    // pass and slices it into batches. Re-seeking through compressed data for each
    // batch would require re-decompressing from row 0, making it O(N²).
    if reader.compression() != 0 {
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let compression = reader.compression();
        let bias = reader.header().bias;
        let cols_idx = crate::resolve_column_indices(
            metadata.variables.iter().map(|v| v.name.as_str()),
            cols.as_deref(),
        )?;
        let missing_null = missing_string_as_null;
        let labels = value_labels_as_strings;
        let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
        let handle = std::thread::spawn(move || {
            let mut next_row = offset;
            if let Err(e) = crate::spss::data::read_data_frame_streaming(
                source.as_ref(),
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
                &mut |mut df| {
                    if let Some(ref name) = row_index_name {
                        let row_start = next_row;
                        next_row = next_row.saturating_add(df.height());
                        match crate::append_row_index(df, name.as_str(), row_start) {
                            Ok(with_idx) => df = with_idx,
                            Err(e) => {
                                let _ =
                                    tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                                return false;
                            }
                        }
                    } else {
                        next_row = next_row.saturating_add(df.height());
                    }
                    tx.send(Ok(df)).is_ok()
                },
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
    let cols_idx = crate::resolve_column_indices(
        metadata.variables.iter().map(|v| v.name.as_str()),
        cols.as_deref(),
    )?;
    let missing_null = missing_string_as_null;
    let labels = value_labels_as_strings;
    let (tx, rx) = mpsc::sync_channel::<PolarsResult<DataFrame>>(2);
    let handle = std::thread::spawn(move || {
        let mut next_row = offset;
        if let Err(e) = crate::spss::data::read_data_frame_streaming(
            source.as_ref(),
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
            &mut |mut df| {
                if let Some(ref name) = row_index_name {
                    let row_start = next_row;
                    next_row = next_row.saturating_add(df.height());
                    match crate::append_row_index(df, name.as_str(), row_start) {
                        Ok(with_idx) => df = with_idx,
                        Err(e) => {
                            let _ = tx.send(Err(PolarsError::ComputeError(e.to_string().into())));
                            return false;
                        }
                    }
                } else {
                    next_row = next_row.saturating_add(df.height());
                }
                tx.send(Ok(df)).is_ok()
            },
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
    row_index_name: Option<String>,
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
        row_index_name: Option<String>,
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
            row_index_name,
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
            self.row_index_name.clone(),
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
        let schema = schema_from_spss_metadata(
            reader.metadata(),
            self.value_labels_as_strings.unwrap_or(true),
            self.informative_nulls.as_ref(),
            self.row_index_name.as_deref(),
        )?;
        Ok(Arc::new(schema))
    }
}

/// Build the output `Schema` for an SPSS .sav/.zsav file directly from its
/// metadata — no data is read, so this is correct even for a zero-row file.
/// Shared by [`SpssScan::schema`] (lazy, path-based) and callers that only
/// have an already-open [`SpssReader`] (e.g. a bytes-backed one, which has
/// no lazy-scan equivalent).
pub fn schema_from_spss_metadata(
    metadata: &crate::spss::types::Metadata,
    value_labels_as_strings: bool,
    informative_nulls: Option<&crate::InformativeNullOpts>,
    row_index_name: Option<&str>,
) -> PolarsResult<Schema> {
    let base_schema = build_schema(metadata, value_labels_as_strings);

    if let Some(null_opts) = informative_nulls {
        let var_names: Vec<&str> = metadata.variables.iter().map(|v| v.name.as_str()).collect();
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
        let mut schema = crate::build_indicator_schema(base_schema, &pairs, &null_opts.mode);
        if let Some(name) = row_index_name {
            schema = crate::append_row_index_schema(schema, name)?;
        }
        Ok(schema)
    } else {
        let mut schema = base_schema;
        if let Some(name) = row_index_name {
            schema = crate::append_row_index_schema(schema, name)?;
        }
        Ok(schema)
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
                    Some(FormatClass::Duration) => DataType::Duration(TimeUnit::Microseconds),
                    None => DataType::Float64,
                },
                crate::spss::types::VarType::Str => DataType::String,
            }
        };
        schema.with_column(var.name.as_str().into(), dtype);
    }
    schema
}
