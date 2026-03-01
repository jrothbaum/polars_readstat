use crate::spss::error::{Error, Result};
use crate::spss::header::read_header;
use crate::spss::metadata::read_metadata;
use crate::spss::types::{Header, Metadata};
use polars::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct SpssReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
}

impl SpssReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        let header = read_header(&mut reader)?;
        let metadata = read_metadata(&mut reader, &header)?;
        Ok(Self {
            path,
            header,
            metadata,
        })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn compression(&self) -> i32 {
        self.header.compression
    }

    pub fn endian(&self) -> crate::spss::types::Endian {
        self.header.endian
    }

    pub fn read(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }
}

pub struct ReadBuilder<'a> {
    reader: &'a SpssReader,
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
    fn new(reader: &'a SpssReader) -> Self {
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
        let limit = self
            .limit
            .unwrap_or(self.reader.metadata.row_count as usize);
        let threads = if self.parallel { self.num_threads } else { Some(1) };
        let mut iter = crate::spss::polars_output::spss_batch_iter_with_reader(
            &self.reader,
            self.reader.path.clone(),
            threads,
            self.missing_string_as_null,
            self.value_labels_as_strings,
            self.chunk_size,
            true,
            self.columns,
            self.offset,
            Some(limit),
            self.informative_nulls.clone(),
        )
        .map_err(|e| Error::ParseError(e.to_string()))?;

        let mut df: Option<DataFrame> = None;
        while let Some(batch) = iter.next() {
            let batch = batch.map_err(|e| Error::ParseError(e.to_string()))?;
            if let Some(acc) = df.as_mut() {
                acc.vstack_mut(&batch)
                    .map_err(|e| Error::ParseError(e.to_string()))?;
            } else {
                df = Some(batch);
            }
        }
        let mut df = df.unwrap_or_else(DataFrame::empty);

        if let Some(schema) = self.schema {
            df = cast_dataframe(df, &schema)?;
        }

        Ok(df)
    }
}

fn cast_dataframe(mut df: DataFrame, schema: &Schema) -> Result<DataFrame> {
    for (name, dtype) in schema.iter() {
        if let Ok(col) = df.column(name) {
            let casted = col
                .as_materialized_series()
                .cast(dtype)
                .map_err(|e| Error::ParseError(format!("Cast failed for {}: {}", name, e)))?;
            df.replace(name, casted.into())
                .map_err(|e| Error::ParseError(format!("Replace failed for {}: {}", name, e)))?;
        }
    }
    Ok(df)
}
