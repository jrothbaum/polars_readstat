use crate::stata::error::Result;
use crate::stata::header::read_header;
use crate::stata::metadata::read_metadata;
use crate::stata::types::{Header, Metadata};
use polars::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Main reader for Stata .dta files
pub struct StataReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct OpenProfile {
    pub header_ms: f64,
    pub metadata_ms: f64,
}

#[derive(Debug, Clone)]
pub struct ReadProfile {
    pub parse_rows_ms: f64,
    pub parse_values_ms: f64,
    pub add_row_ms: f64,
    pub build_df_ms: f64,
    pub total_ms: f64,
}

impl StataReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let header = read_header(&mut file)?;
        let metadata = read_metadata(&mut file, &header)?;

        Ok(Self {
            path,
            header,
            metadata,
        })
    }

    pub fn open_with_profile(path: impl AsRef<Path>) -> Result<(Self, OpenProfile)> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let header_start = Instant::now();
        let header = read_header(&mut file)?;
        let header_ms = header_start.elapsed().as_secs_f64() * 1000.0;

        let metadata_start = Instant::now();
        let metadata = read_metadata(&mut file, &header)?;
        let metadata_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;

        Ok((
            Self {
                path,
                header,
                metadata,
            },
            OpenProfile {
                header_ms,
                metadata_ms,
            },
        ))
    }

    /// Entry point for reading. Returns a builder to configure the operation.
    pub fn read(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub fn header(&self) -> &Header {
        &self.header
    }

    fn execute_read(&self, _opts: ReadBuilder) -> Result<DataFrame> {
        let limit = _opts
            .limit
            .unwrap_or(self.metadata.row_count.saturating_sub(_opts.offset as u64) as usize);
        let threads = if _opts.parallel { _opts.num_threads } else { Some(1) };
        let mut iter = crate::stata::polars_output::stata_batch_iter_with_reader(
            self,
            self.path.clone(),
            threads,
            _opts.missing_string_as_null,
            _opts.value_labels_as_strings,
            _opts.chunk_size,
            true,
            _opts.columns,
            _opts.offset,
            Some(limit),
            _opts.informative_nulls.clone(),
        )
        .map_err(|e| crate::stata::error::Error::ParseError(e.to_string()))?;

        let mut df: Option<DataFrame> = None;
        while let Some(batch) = iter.next() {
            let batch = batch.map_err(|e| crate::stata::error::Error::ParseError(e.to_string()))?;
            if let Some(acc) = df.as_mut() {
                acc.vstack_mut(&batch)?;
            } else {
                df = Some(batch);
            }
        }
        let mut df = df.unwrap_or_else(DataFrame::empty);

        if let Some(schema) = _opts.schema {
            df = cast_dataframe(df, &schema)?;
        }

        Ok(df)
    }

    fn execute_read_profiled(&self, _opts: ReadBuilder) -> Result<(DataFrame, ReadProfile)> {
        let start = Instant::now();
        let df = self.execute_read(_opts)?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        Ok((
            df,
            ReadProfile {
                parse_rows_ms: 0.0,
                parse_values_ms: 0.0,
                add_row_ms: 0.0,
                build_df_ms: total_ms,
                total_ms,
            },
        ))
    }
}

/// Fluent Builder for configuring read operations
pub struct ReadBuilder<'a> {
    reader: &'a StataReader,
    columns: Option<Vec<String>>,
    schema: Option<Arc<Schema>>,
    offset: usize,
    limit: Option<usize>,
    parallel: bool,
    num_threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    informative_nulls: Option<crate::InformativeNullOpts>,
}

impl<'a> ReadBuilder<'a> {
    fn new(reader: &'a StataReader) -> Self {
        Self {
            reader,
            columns: None,
            schema: None,
            offset: 0,
            limit: None,
            parallel: true,
            num_threads: None,
            chunk_size: None,
            missing_string_as_null: true,
            value_labels_as_strings: true,
            informative_nulls: None,
        }
    }

    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.columns = Some(cols);
        self
    }
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }
    pub fn with_n_threads(mut self, n: usize) -> Self {
        self.num_threads = Some(n);
        self
    }
    pub fn with_chunk_size(mut self, n: usize) -> Self {
        self.chunk_size = Some(n);
        self
    }
    pub fn sequential(mut self) -> Self {
        self.parallel = false;
        self
    }
    pub fn missing_string_as_null(mut self, v: bool) -> Self {
        self.missing_string_as_null = v;
        self
    }
    pub fn value_labels_as_strings(mut self, v: bool) -> Self {
        self.value_labels_as_strings = v;
        self
    }
    pub fn informative_nulls(mut self, v: Option<crate::InformativeNullOpts>) -> Self {
        self.informative_nulls = v;
        self
    }

    pub fn finish(self) -> Result<DataFrame> {
        self.reader.execute_read(self)
    }

    pub fn finish_profiled(self) -> Result<(DataFrame, ReadProfile)> {
        self.reader.execute_read_profiled(self)
    }
}

fn cast_dataframe(mut df: DataFrame, schema: &Schema) -> Result<DataFrame> {
    for (name, dtype) in schema.iter() {
        if let Ok(col) = df.column(name) {
            let casted = col.as_materialized_series().cast(dtype).map_err(|e| {
                crate::stata::error::Error::ParseError(format!("Cast failed for {}: {}", name, e))
            })?;
            df.replace(name, casted.into())?;
        }
    }
    Ok(df)
}
