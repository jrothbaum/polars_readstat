use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::data::{DataReader, DataSubheader};
use crate::error::Result;
use crate::page::PageReader;
use crate::reader::{data_reader_at_page_range, Sas7bdatReader};
use crate::types::{Endian, Format, Header, Metadata};
use crate::value::decode_numeric_bytes_mask;
use super::polars_output::{
    ColumnKind, ColumnPlan, kind_for_column,
    to_date_value, to_datetime_value, to_time_value,
};

const DEFAULT_BATCH_SIZE: usize = 8192;

// ── public types ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum SasColumnKind {
    F64,
    DateI32,
    DateTimeI64,
    TimeI64,
    Str,
}

impl From<ColumnKind> for SasColumnKind {
    fn from(k: ColumnKind) -> Self {
        match k {
            ColumnKind::Numeric   => SasColumnKind::F64,
            ColumnKind::Date      => SasColumnKind::DateI32,
            ColumnKind::DateTime  => SasColumnKind::DateTimeI64,
            ColumnKind::Time      => SasColumnKind::TimeI64,
            ColumnKind::Character => SasColumnKind::Str,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SasColumnInfo {
    pub name: String,
    pub kind: SasColumnKind,
}

/// A handle to one segment (or the whole file) of a SAS file read by background threads.
///
/// Call [`next_chunk`] to load up to N rows, then call the typed `fill_*`
/// methods for each column, then [`commit`] to advance past those rows.
///
/// Multiple `SasRowReader`s returned by [`sas_row_readers`] cover disjoint
/// segments of the file and can be drained in any order.  For compressed files
/// a single reader is returned that chains all worker threads internally
/// (matching the `OrderedParallelIter + SlicedBatchIter` approach used by the
/// polars output path).
pub struct SasRowReader {
    /// One receiver per background worker, drained left-to-right.
    rxs: VecDeque<mpsc::Receiver<Result<Vec<u8>>>>,
    _handles: Vec<JoinHandle<()>>,
    /// Plans indexed by column position (column 0 → plans[0], etc.).
    plans: Vec<ColumnPlan>,
    pub schema: Vec<SasColumnInfo>,
    row_length: usize,
    current_buf: Vec<u8>,
    buf_rows: usize,
    buf_offset: usize,
    done: bool,
    /// Row cap: for compressed files this is metadata.row_count and trims phantom
    /// rows from the last page; for uncompressed files it is None.
    remaining: Option<usize>,
}

unsafe impl Send for SasRowReader {}

impl SasRowReader {
    /// Load up to `max_rows` rows into the internal buffer.
    /// Returns the number of rows available (0 = all segments exhausted).
    /// Call [`commit`] once you have consumed them.
    pub fn next_chunk(&mut self, max_rows: usize) -> Result<usize> {
        if self.done {
            return Ok(0);
        }
        let cap = self.remaining.unwrap_or(usize::MAX);
        if cap == 0 {
            self.done = true;
            return Ok(0);
        }
        let want = max_rows.min(cap);

        // Still have unconsumed rows in the current batch.
        let buffered = self.buf_rows.saturating_sub(self.buf_offset);
        if buffered > 0 {
            return Ok(buffered.min(want));
        }

        // Fetch from workers in order, advancing to the next when one is done.
        loop {
            let rx = match self.rxs.front() {
                Some(r) => r,
                None => { self.done = true; return Ok(0); }
            };
            match rx.recv() {
                Ok(Ok(buf)) => {
                    let n = buf.len() / self.row_length;
                    if n == 0 {
                        self.rxs.pop_front();
                        continue;
                    }
                    self.current_buf = buf;
                    self.buf_offset = 0;
                    self.buf_rows = n;
                    return Ok(n.min(want));
                }
                Ok(Err(e)) => {
                    self.done = true;
                    return Err(e);
                }
                Err(_) => {
                    // Channel closed after worker completion: advance to next worker.
                    self.rxs.pop_front();
                }
            }
        }
    }

    /// Advance the internal offset past the `n` rows you just consumed.
    pub fn commit(&mut self, n: usize) {
        self.buf_offset += n;
        if let Some(rem) = self.remaining.as_mut() {
            *rem = rem.saturating_sub(n);
        }
    }

    /// Fill `vals[..n]` and `valid[..n]` from column `col` (a numeric column).
    pub fn fill_f64(&self, col: usize, n: usize, vals: &mut [f64], valid: &mut [bool]) {
        let plan = &self.plans[col];
        let rl = self.row_length;
        for i in 0..n {
            let base = (self.buf_offset + i) * rl;
            let (v, missing) =
                decode_numeric_bytes_mask(plan.endian, &self.current_buf[base + plan.start..base + plan.end]);
            vals[i] = v;
            valid[i] = !missing;
        }
    }

    /// Fill `vals[..n]` and `valid[..n]` from column `col` (a Date column,
    /// i32 days since 1970-01-01).
    pub fn fill_date_i32(&self, col: usize, n: usize, vals: &mut [i32], valid: &mut [bool]) {
        let plan = &self.plans[col];
        let rl = self.row_length;
        for i in 0..n {
            let base = (self.buf_offset + i) * rl;
            let (v, missing) =
                decode_numeric_bytes_mask(plan.endian, &self.current_buf[base + plan.start..base + plan.end]);
            valid[i] = !missing;
            vals[i] = if missing { 0 } else { to_date_value(v) };
        }
    }

    /// Fill `vals[..n]` and `valid[..n]` from column `col` (a Datetime column,
    /// i64 microseconds since Unix epoch).
    pub fn fill_datetime_i64(&self, col: usize, n: usize, vals: &mut [i64], valid: &mut [bool]) {
        let plan = &self.plans[col];
        let rl = self.row_length;
        for i in 0..n {
            let base = (self.buf_offset + i) * rl;
            let (v, missing) =
                decode_numeric_bytes_mask(plan.endian, &self.current_buf[base + plan.start..base + plan.end]);
            valid[i] = !missing;
            vals[i] = if missing { 0 } else { to_datetime_value(v) };
        }
    }

    /// Fill `vals[..n]` and `valid[..n]` from column `col` (a Time column,
    /// i64 microseconds since midnight — matches DuckDB `Time` / `duckdb_time.micros`).
    pub fn fill_time_i64(&self, col: usize, n: usize, vals: &mut [i64], valid: &mut [bool]) {
        let plan = &self.plans[col];
        let rl = self.row_length;
        for i in 0..n {
            let base = (self.buf_offset + i) * rl;
            let (v, missing) =
                decode_numeric_bytes_mask(plan.endian, &self.current_buf[base + plan.start..base + plan.end]);
            valid[i] = !missing;
            vals[i] = if missing { 0 } else { to_time_value(v) };
        }
    }

    /// Fill string column `col` into a flat byte buffer with Arrow-style offsets.
    ///
    /// After return, row `i` is the UTF-8 string
    /// `bytes[offsets[i] as usize .. offsets[i+1] as usize]`.
    /// `nulls[i]` is true when the value is missing/empty.
    /// `offsets` has length `n + 1`; `bytes` and `nulls` have length `n` (bytes)
    /// or `n` entries (nulls).
    ///
    /// All three buffers are cleared before filling.  Reuse them across chunks to
    /// amortise the allocation — for UTF-8 and Latin-1 encoded files this method
    /// performs zero per-string heap allocations.
    pub fn fill_str_buf(
        &self,
        col: usize,
        n: usize,
        bytes: &mut Vec<u8>,
        offsets: &mut Vec<u32>,
        nulls: &mut Vec<bool>,
    ) {
        let plan = &self.plans[col];
        let rl = self.row_length;
        bytes.clear();
        offsets.clear();
        nulls.clear();
        offsets.push(0u32);
        for i in 0..n {
            let base = (self.buf_offset + i) * rl;
            let raw = &self.current_buf[base + plan.start..base + plan.end];
            let mut end = raw.len();
            while end > 0 && (raw[end - 1] == b' ' || raw[end - 1] == 0) {
                end -= 1;
            }
            if let Some(nul) = raw[..end].iter().position(|&b| b == 0) {
                end = nul;
            }
            let trimmed = &raw[..end];
            let is_null = trimmed.is_empty() && plan.missing_string_as_null;
            nulls.push(is_null);
            if !trimmed.is_empty() {
                crate::encoding::decode_string_into(trimmed, plan.encoding_byte, plan.encoding, bytes);
            }
            offsets.push(bytes.len() as u32);
        }
    }

    // ── row-at-a-time decode helpers ─────────────────────────────────────────

    /// Returns the raw bytes for row `i` in the current chunk (0-indexed relative to buf_offset).
    #[inline]
    pub fn row_bytes(&self, i: usize) -> &[u8] {
        let base = (self.buf_offset + i) * self.row_length;
        &self.current_buf[base..base + self.row_length]
    }

    /// Decode a numeric (f64) column from raw row bytes. Returns (value, is_valid).
    #[inline]
    pub fn decode_f64_row(&self, col: usize, row_bytes: &[u8]) -> (f64, bool) {
        let plan = &self.plans[col];
        let (v, missing) = decode_numeric_bytes_mask(plan.endian, &row_bytes[plan.start..plan.end]);
        (v, !missing)
    }

    /// Decode a Date column from raw row bytes. Returns (days-since-epoch, is_valid).
    #[inline]
    pub fn decode_date_row(&self, col: usize, row_bytes: &[u8]) -> (i32, bool) {
        let plan = &self.plans[col];
        let (v, missing) = decode_numeric_bytes_mask(plan.endian, &row_bytes[plan.start..plan.end]);
        (if missing { 0 } else { to_date_value(v) }, !missing)
    }

    /// Decode a Datetime column from raw row bytes. Returns (microseconds-since-epoch, is_valid).
    #[inline]
    pub fn decode_datetime_row(&self, col: usize, row_bytes: &[u8]) -> (i64, bool) {
        let plan = &self.plans[col];
        let (v, missing) = decode_numeric_bytes_mask(plan.endian, &row_bytes[plan.start..plan.end]);
        (if missing { 0 } else { to_datetime_value(v) }, !missing)
    }

    /// Decode a Time column from raw row bytes. Returns (microseconds-since-midnight, is_valid).
    #[inline]
    pub fn decode_time_row(&self, col: usize, row_bytes: &[u8]) -> (i64, bool) {
        let plan = &self.plans[col];
        let (v, missing) = decode_numeric_bytes_mask(plan.endian, &row_bytes[plan.start..plan.end]);
        (if missing { 0 } else { to_time_value(v) }, !missing)
    }

    /// Decode a string column from raw row bytes into `out`. Returns true if the value is null.
    /// `out` is cleared before writing.
    pub fn decode_str_row_into(&self, col: usize, row_bytes: &[u8], out: &mut Vec<u8>) -> bool {
        let plan = &self.plans[col];
        let raw = &row_bytes[plan.start..plan.end];
        let mut end = raw.len();
        while end > 0 && (raw[end - 1] == b' ' || raw[end - 1] == 0) {
            end -= 1;
        }
        if let Some(nul) = raw[..end].iter().position(|&b| b == 0) {
            end = nul;
        }
        let trimmed = &raw[..end];
        let is_null = trimmed.is_empty() && plan.missing_string_as_null;
        out.clear();
        if !is_null && !trimmed.is_empty() {
            crate::encoding::decode_string_into(trimmed, plan.encoding_byte, plan.encoding, out);
        }
        is_null
    }

    /// Fill `vals` with `n` `Option<String>` values from character column `col`.
    /// `vals` is cleared and re-populated.
    pub fn fill_str(&self, col: usize, n: usize, vals: &mut Vec<Option<String>>) {
        let plan = &self.plans[col];
        let rl = self.row_length;
        vals.clear();
        for i in 0..n {
            let base = (self.buf_offset + i) * rl;
            let bytes = &self.current_buf[base + plan.start..base + plan.end];
            let mut end = bytes.len();
            while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == 0) {
                end -= 1;
            }
            if let Some(nul) = bytes[..end].iter().position(|&b| b == 0) {
                end = nul;
            }
            vals.push(if end == 0 {
                if plan.missing_string_as_null { None } else { Some(String::new()) }
            } else {
                Some(crate::encoding::decode_string(&bytes[..end], plan.encoding_byte, plan.encoding))
            });
        }
    }
}

// ── background workers ────────────────────────────────────────────────────────

fn spawn_serial_raw_worker(
    tx: mpsc::SyncSender<Result<Vec<u8>>>,
    path: std::path::PathBuf,
    header: Header,
    metadata: Arc<Metadata>,
    row_length: usize,
    endian: Endian,
    format: Format,
    initial_data_subheaders: Vec<DataSubheader>,
    batch_size: usize,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let file = match File::open(&path) {
            Ok(f) => BufReader::new(f),
            Err(e) => { let _ = tx.send(Err(e.into())); return; }
        };
        let mut file = file;
        if let Err(e) = file.seek(SeekFrom::Start(header.header_length as u64)) {
            let _ = tx.send(Err(e.into())); return;
        }
        let page_reader = PageReader::new(file, header, endian, format);
        let mut data_reader = match DataReader::new(
            page_reader, metadata.as_ref().clone(), endian, format, initial_data_subheaders,
        ) {
            Ok(r) => r,
            Err(e) => { let _ = tx.send(Err(e)); return; }
        };
        loop {
            let mut buf = Vec::new();
            let n_read = match data_reader.read_rows_bulk(batch_size, &mut buf) {
                Ok(n) => n,
                Err(e) => { let _ = tx.send(Err(e)); return; }
            };
            if n_read == 0 { break; }
            buf.truncate(n_read * row_length);
            if tx.send(Ok(buf)).is_err() { return; }
        }
    })
}

fn spawn_parallel_raw_worker(
    tx: mpsc::SyncSender<Result<Vec<u8>>>,
    path: std::path::PathBuf,
    header: Header,
    metadata: Arc<Metadata>,
    row_length: usize,
    endian: Endian,
    format: Format,
    batch_size: usize,
    worker_page_start: usize,
    worker_page_count: usize,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut data_reader = match data_reader_at_page_range(
            &path, &header, &metadata, endian, format,
            worker_page_start, worker_page_count, 0,
        ) {
            Ok(r) => r,
            Err(e) => { let _ = tx.send(Err(e)); return; }
        };
        loop {
            let mut buf = Vec::new();
            let n_read = match data_reader.read_rows_bulk(batch_size, &mut buf) {
                Ok(n) => n,
                Err(e) => { let _ = tx.send(Err(e)); return; }
            };
            if n_read == 0 { break; }
            buf.truncate(n_read * row_length);
            if tx.send(Ok(buf)).is_err() { return; }
        }
    })
}

// ── public entry point ────────────────────────────────────────────────────────

/// Spawn background reader threads and return one [`SasRowReader`] per segment.
///
/// For uncompressed files with multiple threads, returns one reader per worker
/// covering a disjoint page range — drain them in any order.
///
/// For compressed files or when a single thread is requested, returns a single
/// reader that chains all worker threads internally in file order with a
/// `metadata.row_count` cap to trim phantom rows from the last page — identical
/// to the `OrderedParallelIter + SlicedBatchIter` approach used by the polars
/// output path.
///
/// Files with mix-page rows always use a single serial reader.
pub fn sas_row_readers(path: &Path, opts: &crate::ScanOptions) -> Result<Vec<SasRowReader>> {
    let reader = Sas7bdatReader::open(path)?;
    let header = reader.header().clone();
    let metadata = Arc::new(reader.metadata().clone());
    let row_length = metadata.row_length;
    let endian = reader.endian();
    let format = reader.format();
    let batch_size = opts.chunk_size.unwrap_or(DEFAULT_BATCH_SIZE);
    let n_threads = opts.threads.unwrap_or_else(crate::default_thread_count).max(1);
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);

    let schema: Vec<SasColumnInfo> = metadata.columns.iter()
        .map(|col| SasColumnInfo { name: col.name.clone(), kind: kind_for_column(col).into() })
        .collect();

    let plans = build_column_plans(&metadata, endian, missing_string_as_null);

    let mix_data_rows = reader.mix_data_rows();
    let is_compressed = metadata.compression != crate::Compression::None;
    let row_count = metadata.row_count;

    // Serial path: mix-page rows present or single thread.
    if mix_data_rows > 0 || n_threads == 1 {
        let initial_subs = reader.initial_data_subheaders().to_vec();
        let (tx, rx) = mpsc::sync_channel(4);
        let handle = spawn_serial_raw_worker(
            tx, path.to_path_buf(), header, metadata, row_length,
            endian, format, initial_subs, batch_size,
        );
        return Ok(vec![make_reader(
            VecDeque::from([rx]), vec![handle],
            plans, schema, row_length, None,
        )]);
    }

    // Parallel path: divide data pages across n_threads workers.
    let total_pages = {
        let file_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let data_bytes = file_len.saturating_sub(header.header_length as u64);
        (data_bytes / header.page_length as u64) as usize
    };
    let first_data_page = reader.first_data_page();
    let data_pages = total_pages.saturating_sub(first_data_page);
    let n_workers = n_threads.min(data_pages.max(1));
    let pages_per_worker = data_pages.div_ceil(n_workers);

    if is_compressed {
        // Compressed files must be drained in order so the row_count cap always
        // trims the same trailing phantom rows.  Chain all workers into one reader
        // (mirrors OrderedParallelIter + SlicedBatchIter).
        let mut rxs = VecDeque::with_capacity(n_workers);
        let mut handles = Vec::with_capacity(n_workers);
        for worker_idx in 0..n_workers {
            let wp_start = first_data_page + worker_idx * pages_per_worker;
            if wp_start >= total_pages { break; }
            let wp_count = pages_per_worker.min(total_pages - wp_start);
            let (tx, rx) = mpsc::sync_channel(4);
            handles.push(spawn_parallel_raw_worker(
                tx, path.to_path_buf(), header.clone(), metadata.clone(),
                row_length, endian, format, batch_size, wp_start, wp_count,
            ));
            rxs.push_back(rx);
        }
        return Ok(vec![make_reader(rxs, handles, plans, schema, row_length, Some(row_count))]);
    }

    // Uncompressed parallel: one independent reader per worker.
    let mut readers = Vec::with_capacity(n_workers);
    for worker_idx in 0..n_workers {
        let wp_start = first_data_page + worker_idx * pages_per_worker;
        if wp_start >= total_pages { break; }
        let wp_count = pages_per_worker.min(total_pages - wp_start);
        let (tx, rx) = mpsc::sync_channel(4);
        let handle = spawn_parallel_raw_worker(
            tx, path.to_path_buf(), header.clone(), metadata.clone(),
            row_length, endian, format, batch_size, wp_start, wp_count,
        );
        readers.push(make_reader(
            VecDeque::from([rx]), vec![handle],
            plans.clone(), schema.clone(), row_length, None,
        ));
    }
    Ok(readers)
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_reader(
    rxs: VecDeque<mpsc::Receiver<Result<Vec<u8>>>>,
    handles: Vec<JoinHandle<()>>,
    plans: Vec<ColumnPlan>,
    schema: Vec<SasColumnInfo>,
    row_length: usize,
    remaining: Option<usize>,
) -> SasRowReader {
    SasRowReader {
        rxs, _handles: handles, plans, schema, row_length,
        current_buf: Vec::new(), buf_rows: 0, buf_offset: 0, done: false, remaining,
    }
}

fn build_column_plans(
    metadata: &Metadata,
    endian: Endian,
    missing_string_as_null: bool,
) -> Vec<ColumnPlan> {
    let encoding = crate::encoding::get_encoding(metadata.encoding_byte);
    metadata.columns.iter().enumerate().map(|(i, col)| ColumnPlan {
        start: col.offset,
        end: col.offset + col.length,
        kind: kind_for_column(col),
        endian,
        encoding_byte: metadata.encoding_byte,
        encoding,
        missing_string_as_null,
        output_index: i,
    }).collect()
}
