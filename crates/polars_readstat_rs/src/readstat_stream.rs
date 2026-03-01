use crate::compress_df_if_enabled;
use crate::sas::polars_output::sas_batch_iter;
use crate::spss::polars_output::spss_batch_iter;
use crate::stata::polars_output::stata_batch_iter;
use crate::{ReadStatFormat, ScanOptions};
use polars::prelude::{DataFrame, PolarsError, PolarsResult};
use std::path::Path;

pub struct ReadstatBatchIter {
    inner: Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>,
}

impl ReadstatBatchIter {
    fn new(inner: Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>) -> Self {
        Self { inner }
    }
}

impl Iterator for ReadstatBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct ReadstatBatchStream {
    iter: ReadstatBatchIter,
}

impl ReadstatBatchStream {
    pub fn new(
        path: impl AsRef<Path>,
        opts: Option<ScanOptions>,
        format: Option<ReadStatFormat>,
        columns: Option<Vec<String>>,
        n_rows: Option<usize>,
        batch_size: Option<usize>,
    ) -> PolarsResult<Self> {
        let iter = readstat_batch_iter(path, opts, format, columns, n_rows, batch_size)?;
        Ok(Self { iter })
    }

    pub fn next_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        match self.iter.next() {
            Some(result) => result.map(Some),
            None => Ok(None),
        }
    }
}

/// Format-agnostic batch iterator for streaming readstat files.
pub fn readstat_batch_iter(
    path: impl AsRef<Path>,
    opts: Option<ScanOptions>,
    format: Option<ReadStatFormat>,
    columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
) -> PolarsResult<ReadstatBatchIter> {
    let path = path.as_ref();
    let opts = opts.unwrap_or_default();
    let format = format.or_else(|| super::detect_format(path)).ok_or_else(|| {
        PolarsError::ComputeError("unknown file extension".into())
    })?;

    let columns = columns.and_then(|cols| if cols.is_empty() { None } else { Some(cols) });

    let chunk_size = batch_size.or(opts.chunk_size);
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let value_labels_as_strings = opts.value_labels_as_strings.unwrap_or(true);
    let preserve_order = opts.preserve_order.unwrap_or(false);

    let iter: Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send> = match format {
        ReadStatFormat::Sas => {
            let col_indices = if let Some(cols) = columns {
                let reader = crate::sas::reader::Sas7bdatReader::open(path)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                let indices = resolve_sas_column_indices(&reader, &cols)?;
                Some(indices)
            } else {
                None
            };
            let iter = sas_batch_iter(
                path.to_path_buf(),
                opts.threads,
                missing_string_as_null,
                chunk_size,
                col_indices,
                0,
                n_rows,
                preserve_order,
                opts.informative_nulls.clone(),
            )?;
            Box::new(iter)
        }
        ReadStatFormat::Stata => {
            let iter = stata_batch_iter(
                path.to_path_buf(),
                opts.threads,
                missing_string_as_null,
                value_labels_as_strings,
                chunk_size,
                preserve_order,
                columns,
                0,
                n_rows,
                opts.informative_nulls.clone(),
            )?;
            Box::new(iter)
        }
        ReadStatFormat::Spss => {
            let iter = spss_batch_iter(
                path.to_path_buf(),
                opts.threads,
                missing_string_as_null,
                value_labels_as_strings,
                chunk_size,
                preserve_order,
                columns,
                0,
                n_rows,
                opts.informative_nulls.clone(),
            )?;
            Box::new(iter)
        }
    };

    if opts.compress_opts.enabled {
        let compress_opts = opts.compress_opts;
        let mapped = iter.map(move |batch| {
            let df = batch?;
            compress_df_if_enabled(&df, &compress_opts)
                .map_err(|e| PolarsError::ComputeError(e.into()))
        });
        Ok(ReadstatBatchIter::new(Box::new(mapped)))
    } else {
        Ok(ReadstatBatchIter::new(iter))
    }
}

fn resolve_sas_column_indices(
    reader: &crate::sas::reader::Sas7bdatReader,
    columns: &[String],
) -> PolarsResult<Vec<usize>> {
    let meta = reader.metadata();
    let mut indices = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = meta
            .columns
            .iter()
            .position(|c| c.name == *name)
            .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))?;
        indices.push(idx);
    }
    Ok(indices)
}
