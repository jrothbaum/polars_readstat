use crate::data::{DataReader, DataSubheader};
use crate::error::Result;
use crate::header::{check_header, read_header};
use crate::metadata::read_metadata;
use crate::page::PageReader;
use crate::types::{Endian, Format, Header, Metadata};
use polars::prelude::*;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Entry in the data page index for fast parallel seeking
#[derive(Debug, Clone)]
pub(crate) struct DataPageEntry {
    /// Page number (0-indexed from start of file pages, after header)
    page_number: usize,
    /// Cumulative row count at the START of this page
    row_start: usize,
    /// Number of data rows on this page
    row_count: usize,
}

/// Main reader for SAS7BDAT files
pub struct Sas7bdatReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
    endian: Endian,
    format: Format,
    initial_data_subheaders: Vec<DataSubheader>,
    /// 0-based index of the first DATA page (page right after all metadata pages).
    /// Used for analytical parallel seek without scanning the file.
    first_data_page: usize,
    /// Number of data rows that live on MIX pages before the first DATA page.
    /// These are returned by the DataReader before any DATA-page rows.
    mix_data_rows: usize,
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

impl Sas7bdatReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let (endian, format) = check_header(&mut file)?;
        let header = read_header(&mut file, endian, format)?;

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let (metadata, initial_data_subheaders, first_data_page, mix_data_rows) =
            read_metadata(file, &header, endian, format)?;

        Ok(Self {
            path,
            header,
            metadata,
            endian,
            format,
            initial_data_subheaders,
            first_data_page,
            mix_data_rows,
        })
    }

    pub fn open_with_profile(path: impl AsRef<Path>) -> Result<(Self, OpenProfile)> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let header_start = Instant::now();
        let (endian, format) = check_header(&mut file)?;
        let header = read_header(&mut file, endian, format)?;
        let header_ms = header_start.elapsed().as_secs_f64() * 1000.0;

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let metadata_start = Instant::now();
        let (metadata, initial_data_subheaders, first_data_page, mix_data_rows) =
            read_metadata(file, &header, endian, format)?;
        let metadata_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;

        Ok((
            Self {
                path,
                header,
                metadata,
                endian,
                format,
                initial_data_subheaders,
                first_data_page,
                mix_data_rows,
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
    pub fn endian(&self) -> Endian {
        self.endian
    }
    pub fn format(&self) -> Format {
        self.format
    }
    pub fn initial_data_subheaders(&self) -> &[DataSubheader] {
        &self.initial_data_subheaders
    }
    pub(crate) fn first_data_page(&self) -> usize {
        self.first_data_page
    }
    pub(crate) fn mix_data_rows(&self) -> usize {
        self.mix_data_rows
    }

    /// The single internal execution path for all read operations
    fn execute_read(&self, opts: ReadBuilder) -> Result<DataFrame> {
        let col_indices = resolve_column_indices(&self.metadata, opts.columns.as_deref())?;
        let limit = opts
            .limit
            .unwrap_or(self.metadata.row_count.saturating_sub(opts.offset));

        let missing_null = opts.missing_string_as_null;
        let threads = if opts.parallel { opts.num_threads } else { Some(1) };
        let mut iter = crate::sas::polars_output::sas_batch_iter_with_reader(
            self,
            self.path.clone(),
            threads,
            missing_null,
            opts.chunk_size,
            col_indices,
            opts.offset,
            Some(limit),
            true,
            opts.informative_nulls.clone(),
        )
        .map_err(|e| crate::error::Error::ParseError(e.to_string()))?;

        let mut df: Option<DataFrame> = None;
        while let Some(batch) = iter.next() {
            let batch =
                batch.map_err(|e| crate::error::Error::ParseError(e.to_string()))?;
            if let Some(acc) = df.as_mut() {
                acc.vstack_mut(&batch)?;
            } else {
                df = Some(batch);
            }
        }
        let mut df = df.unwrap_or_else(DataFrame::empty);

        if let Some(schema) = opts.schema {
            df = cast_dataframe(df, &schema)?;
        }

        Ok(df)
    }

    /// Profiled read path (streaming execution)
    fn execute_read_profiled(&self, opts: ReadBuilder) -> Result<(DataFrame, ReadProfile)> {
        let total_start = Instant::now();
        let df = self.execute_read(opts)?;
        let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
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
    reader: &'a Sas7bdatReader,
    columns: Option<Vec<String>>,
    schema: Option<Arc<Schema>>,
    offset: usize,
    limit: Option<usize>,
    parallel: bool,
    num_threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    informative_nulls: Option<crate::InformativeNullOpts>,
}

impl<'a> ReadBuilder<'a> {
    fn new(reader: &'a Sas7bdatReader) -> Self {
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

/// Build a page index analytically — zero I/O, pure arithmetic from header/metadata.
/// For uncompressed SAS files all pages after the metadata section are DATA pages
/// with a constant rows-per-page value derived from page_length and row_length.
/// We simply enumerate them without touching the file.
pub(crate) fn compute_page_index(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    first_data_page: usize,
    mix_data_rows: usize,
) -> Vec<DataPageEntry> {
    let row_length = metadata.row_length;
    if row_length == 0 {
        return Vec::new();
    }

    let page_bit_offset: usize = match format {
        Format::Bit64 => 32,
        Format::Bit32 => 16,
    };
    let data_start = page_bit_offset + 8;
    let rows_per_page = header.page_length.saturating_sub(data_start) / row_length;
    if rows_per_page == 0 {
        return Vec::new();
    }

    // Validate: read the actual block_count from the first DATA page header.
    // If it doesn't match our analytical rows_per_page the assumption is broken
    // and we return an empty index so all workers fall back to skip_rows.
    if let Ok(actual_block_count) = read_first_data_page_block_count(
        path, header, endian, page_bit_offset, first_data_page,
    ) {
        if actual_block_count != rows_per_page {
            return Vec::new();
        }
    } else {
        return Vec::new();
    }

    // DATA-page rows start after any MIX-page data rows.
    let total_rows = metadata.row_count;
    let data_page_rows = total_rows.saturating_sub(mix_data_rows);
    let n_data_pages = (data_page_rows + rows_per_page - 1) / rows_per_page;

    let mut entries = Vec::with_capacity(n_data_pages);
    for i in 0..n_data_pages {
        let row_start = mix_data_rows + i * rows_per_page;
        let row_count = rows_per_page.min(total_rows - row_start);
        entries.push(DataPageEntry {
            page_number: first_data_page + i,
            row_start,
            row_count,
        });
    }
    entries
}

/// Read the block_count field from the first DATA page header.
/// This is a single seek + 6-byte read — essentially free.
fn read_first_data_page_block_count(
    path: &Path,
    header: &Header,
    endian: Endian,
    page_bit_offset: usize,
    first_data_page: usize,
) -> crate::error::Result<usize> {
    use std::io::Read;
    let byte_offset = header.header_length as u64
        + first_data_page as u64 * header.page_length as u64
        + page_bit_offset as u64;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(byte_offset))?;
    let mut buf = [0u8; 6];
    file.read_exact(&mut buf)?;
    // page_type: buf[0..2], block_count: buf[2..4]
    let block_count = match endian {
        Endian::Little => u16::from_le_bytes([buf[2], buf[3]]),
        Endian::Big => u16::from_be_bytes([buf[2], buf[3]]),
    };
    Ok(block_count as usize)
}

/// Create a DataReader positioned at `target_row` using the page index when possible.
/// Falls back to the sequential skip_rows path when the target row is in the MIX prefix.
pub(crate) fn data_reader_at_row(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    target_row: usize,
    initial_data_subheaders: &[DataSubheader],
    page_index: &[DataPageEntry],
) -> Result<DataReader<BufReader<File>>> {
    if target_row >= metadata.row_count {
        // Construct a reader at EOF (caller should handle empty range).
        let mut file = BufReader::new(File::open(path)?);
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let page_reader = PageReader::new(file, header.clone(), endian, format);
        let mut data_reader = DataReader::new(
            page_reader,
            metadata.clone(),
            endian,
            format,
            initial_data_subheaders.to_vec(),
        )?;
        data_reader.set_current_row(metadata.row_count);
        return Ok(data_reader);
    }

    let page_idx = page_index
        .partition_point(|p| p.row_start + p.row_count <= target_row);

    let use_index = page_idx < page_index.len()
        && target_row >= page_index.first().map_or(0, |e| e.row_start);

    if !use_index {
        let mut file = BufReader::new(File::open(path)?);
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let page_reader = PageReader::new(file, header.clone(), endian, format);
        let mut data_reader = DataReader::new(
            page_reader,
            metadata.clone(),
            endian,
            format,
            initial_data_subheaders.to_vec(),
        )?;
        if target_row > 0 {
            data_reader.skip_rows(target_row)?;
        }
        return Ok(data_reader);
    }

    let entry = &page_index[page_idx];
    let within_page_skip = target_row - entry.row_start;

    let page_byte_offset =
        header.header_length as u64 + (entry.page_number as u64 * header.page_length as u64);
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(page_byte_offset))?;

    let page_reader = PageReader::new(file, header.clone(), endian, format);
    let mut data_reader = DataReader::new(
        page_reader,
        metadata.clone(),
        endian,
        format,
        Vec::new(),
    )?;

    data_reader.set_current_row(entry.row_start);
    if within_page_skip > 0 {
        data_reader.skip_rows(within_page_skip)?;
    }
    Ok(data_reader)
}

fn resolve_column_indices(
    metadata: &Metadata,
    columns: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    columns
        .map(|names| {
            names
                .iter()
                .map(|name| {
                    metadata
                        .columns
                        .iter()
                        .position(|c| c.name == *name)
                        .ok_or_else(|| {
                            crate::error::Error::ParseError(format!("Column '{}' not found", name))
                        })
                })
                .collect()
        })
        .transpose()
}

fn cast_dataframe(mut df: DataFrame, schema: &Schema) -> Result<DataFrame> {
    for (name, dtype) in schema.iter() {
        if let Ok(col) = df.column(name) {
            let casted = col.as_materialized_series().cast(dtype).map_err(|e| {
                crate::error::Error::ParseError(format!("Cast failed for {}: {}", name, e))
            })?;
            df.replace(name, casted.into())?;
        }
    }
    Ok(df)
}
