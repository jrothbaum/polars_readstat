use crate::sas::constants::{DATE_FORMATS, DATETIME_FORMATS, TIME_FORMATS};
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::JoinHandle;

const LINE_LEN: usize = 80;
const NAMESTR_SIZE: usize = 140;

const XPT_COL_TYPE_CHR: u16 = 2;

// ────────────────────────────────────────────────────────────────
// Public types
// ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum XptColumnType {
    Numeric,
    Character,
}

#[derive(Debug, Clone)]
pub struct XptColumn {
    pub name: String,
    pub label: String,
    pub format: String,
    pub col_type: XptColumnType,
    pub storage_width: usize,
}

#[derive(Debug)]
pub struct XptMetadata {
    pub version: u8,
    pub table_name: String,
    pub file_label: String,
    pub columns: Vec<XptColumn>,
    pub row_length: usize,
    pub data_offset: u64,
    /// Upper-bound row count derived from (file_size - data_offset) / row_length.
    /// May be off by ≤1 due to trailing space-padding; workers discard blank rows naturally.
    pub row_count: usize,
}

// ────────────────────────────────────────────────────────────────
// Low-level helpers
// ────────────────────────────────────────────────────────────────

fn trim_bytes(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == 0) {
        end -= 1;
    }
    &bytes[..end]
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(trim_bytes(bytes)).into_owned()
}

fn read_line(reader: &mut impl Read) -> PolarsResult<[u8; LINE_LEN]> {
    let mut buf = [0u8; LINE_LEN];
    reader
        .read_exact(&mut buf)
        .map_err(|e| PolarsError::ComputeError(format!("XPT read error: {e}").into()))?;
    Ok(buf)
}

fn parse_5digit(bytes: &[u8]) -> PolarsResult<usize> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| PolarsError::ComputeError("XPT: invalid UTF-8 in header".into()))?;
    s.trim()
        .parse::<usize>()
        .map_err(|_| PolarsError::ComputeError(format!("XPT: bad number {:?}", s).into()))
}

// Header record format: "HEADER RECORD*******<8s> HEADER RECORD!!!!!!!"
// followed by six %05d fields. The prefix is exactly 48 bytes:
// "HEADER RECORD*******" (20) + name (7-8 chars padded to 8) + " HEADER RECORD!!!!!!!" (20).
// num1=[48..53), num2=[53..58), num3=[58..63), ...
fn header_num1(record: &[u8; LINE_LEN]) -> PolarsResult<usize> {
    parse_5digit(&record[48..53])
}

fn header_num2(record: &[u8; LINE_LEN]) -> PolarsResult<usize> {
    parse_5digit(&record[53..58])
}

fn detect_version(record: &[u8; LINE_LEN]) -> PolarsResult<u8> {
    // bytes 20-27 hold the 8-char record name (e.g. "LIBRARY ", "LIBV8   ")
    if record[20..].starts_with(b"LIBRARY") {
        Ok(5)
    } else if record[20..].starts_with(b"LIBV8") {
        Ok(8)
    } else {
        Err(PolarsError::ComputeError(
            format!(
                "Not a SAS XPORT file (got {:?})",
                bytes_to_string(&record[20..28])
            )
            .into(),
        ))
    }
}

// ────────────────────────────────────────────────────────────────
// namestr parsing
// ────────────────────────────────────────────────────────────────

// namestr layout (packed, big-endian integers):
//   0: ntype  u16    – 1=num, 2=char
//   2: nhfun  u16
//   4: nlng   u16    – storage width (bytes)
//   6: nvar0  u16
//   8: nname  [u8;8] – short name (always present)
//  16: nlabel [u8;40]– variable label
//  56: nform  [u8;8] – format name
//  64: nfl    u16    – format width
//  66: nfd    u16    – format decimals
//  68: nfj    u16    – justification (0=left, 1=right)
//  70: nfill  [u8;2]
//  72: niform [u8;8] – informat name
//  80: nifl   u16
//  82: nifd   u16
//  84: npos   u32
//  88: longname [u8;32] – long name (v8)
// 120: labeln u16
// 122: rest   [u8;18]
// = 140 bytes total
fn parse_namestr(buf: &[u8], version: u8) -> XptColumn {
    debug_assert!(buf.len() >= NAMESTR_SIZE);

    let ntype = u16::from_be_bytes([buf[0], buf[1]]);
    let nlng = u16::from_be_bytes([buf[4], buf[5]]) as usize;

    let name = if version >= 8 {
        bytes_to_string(&buf[88..120])
    } else {
        bytes_to_string(&buf[8..16])
    };
    let label = bytes_to_string(&buf[16..56]);
    let format_name = bytes_to_string(&buf[56..64]);
    let nfl = u16::from_be_bytes([buf[64], buf[65]]) as usize;
    let nfd = u16::from_be_bytes([buf[66], buf[67]]) as usize;

    let format = if format_name.is_empty() {
        String::new()
    } else if nfl > 0 && nfd > 0 {
        format!("{}{}.{}", format_name, nfl, nfd)
    } else if nfl > 0 {
        format!("{}{}", format_name, nfl)
    } else {
        format_name
    };

    let col_type = if ntype == XPT_COL_TYPE_CHR {
        XptColumnType::Character
    } else {
        XptColumnType::Numeric
    };

    XptColumn {
        name,
        label,
        format,
        col_type,
        storage_width: nlng,
    }
}

// ────────────────────────────────────────────────────────────────
// Label-section handlers (v8/v9)
// ────────────────────────────────────────────────────────────────

// LABELV8: each entry is [index u16, name_len u16, label_len u16] + bytes
fn read_labels_v8(
    reader: &mut (impl Read + Seek),
    columns: &mut Vec<XptColumn>,
    label_count: usize,
) -> PolarsResult<()> {
    for _ in 0..label_count {
        let mut hdr = [0u8; 6];
        reader
            .read_exact(&mut hdr)
            .map_err(|e| PolarsError::ComputeError(format!("XPT LABELV8 read: {e}").into()))?;
        let index = u16::from_be_bytes([hdr[0], hdr[1]]) as usize;
        let name_len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
        let label_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;

        let mut payload = vec![0u8; name_len + label_len];
        reader
            .read_exact(&mut payload)
            .map_err(|e| PolarsError::ComputeError(format!("XPT LABELV8 payload: {e}").into()))?;

        if index > 0 && index <= columns.len() {
            let col = &mut columns[index - 1];
            if name_len > 0 {
                col.name = bytes_to_string(&payload[..name_len]);
            }
            if label_len > 0 {
                col.label = bytes_to_string(&payload[name_len..]);
            }
        }
    }
    align_to_record(reader)?;
    Ok(())
}

// LABELV9: each entry is [index, name_len, label_len, format_len, informat_len] u16 + bytes
fn read_labels_v9(
    reader: &mut (impl Read + Seek),
    columns: &mut Vec<XptColumn>,
    label_count: usize,
) -> PolarsResult<()> {
    for _ in 0..label_count {
        let mut hdr = [0u8; 10];
        reader
            .read_exact(&mut hdr)
            .map_err(|e| PolarsError::ComputeError(format!("XPT LABELV9 read: {e}").into()))?;
        let index = u16::from_be_bytes([hdr[0], hdr[1]]) as usize;
        let name_len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
        let label_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
        let format_len = u16::from_be_bytes([hdr[6], hdr[7]]) as usize;
        let informat_len = u16::from_be_bytes([hdr[8], hdr[9]]) as usize;

        let total = name_len + label_len + format_len + informat_len;
        let mut payload = vec![0u8; total];
        reader
            .read_exact(&mut payload)
            .map_err(|e| PolarsError::ComputeError(format!("XPT LABELV9 payload: {e}").into()))?;

        if index > 0 && index <= columns.len() {
            let col = &mut columns[index - 1];
            if name_len > 0 {
                col.name = bytes_to_string(&payload[..name_len]);
            }
            if label_len > 0 {
                col.label = bytes_to_string(&payload[name_len..name_len + label_len]);
            }
            if format_len > 0 {
                col.format = bytes_to_string(
                    &payload[name_len + label_len..name_len + label_len + format_len],
                );
            }
        }
    }
    align_to_record(reader)?;
    Ok(())
}

fn align_to_record(reader: &mut (impl Read + Seek)) -> PolarsResult<()> {
    let pos = reader
        .stream_position()
        .map_err(|e| PolarsError::ComputeError(format!("XPT seek: {e}").into()))?;
    let rem = (pos % LINE_LEN as u64) as usize;
    if rem != 0 {
        reader
            .seek(SeekFrom::Current((LINE_LEN - rem) as i64))
            .map_err(|e| PolarsError::ComputeError(format!("XPT align seek: {e}").into()))?;
    }
    Ok(())
}

// ────────────────────────────────────────────────────────────────
// Metadata parsing (public)
// ────────────────────────────────────────────────────────────────

pub fn read_xpt_metadata(path: &Path) -> PolarsResult<XptMetadata> {
    let file = File::open(path)
        .map_err(|e| PolarsError::ComputeError(format!("XPT open: {e}").into()))?;
    let mut r = BufReader::new(file);

    // 1. LIBRARY/LIBV8 header
    let rec = read_line(&mut r)?;
    let version = detect_version(&rec)?;

    // 2. SAS library name record (skip)
    read_line(&mut r)?;

    // 3. Timestamp record (skip)
    read_line(&mut r)?;

    // 4. MEMBER/MEMBV8 header
    read_line(&mut r)?;

    // 5. DSCRPTR/DSCPTV8 header
    read_line(&mut r)?;

    // 6. Table name record — name at bytes 8..16 (v5) or 8..40 (v8)
    let rec = read_line(&mut r)?;
    let table_name = if version >= 8 {
        bytes_to_string(&rec[8..40])
    } else {
        bytes_to_string(&rec[8..16])
    };

    // 7. File label record — label at bytes 32..72
    let rec = read_line(&mut r)?;
    let file_label = bytes_to_string(&rec[32..72]);

    // 8. NAMESTR/NAMSTV8 header — var_count in num2 field
    let rec = read_line(&mut r)?;
    let var_count = header_num2(&rec)?;

    // 9. Variable namestr records (padded to 80-byte boundary)
    let namestr_bytes = var_count * NAMESTR_SIZE;
    let namestr_padded = namestr_bytes.div_ceil(LINE_LEN) * LINE_LEN;
    let mut namestr_buf = vec![0u8; namestr_padded];
    r.read_exact(&mut namestr_buf)
        .map_err(|e| PolarsError::ComputeError(format!("XPT namestr read: {e}").into()))?;

    let mut columns: Vec<XptColumn> = (0..var_count)
        .map(|i| parse_namestr(&namestr_buf[i * NAMESTR_SIZE..], version))
        .collect();

    // 10. OBS / LABEL header (v8 may have label records before OBS)
    if version >= 8 {
        let rec = read_line(&mut r)?;
        let name = bytes_to_string(&rec[20..28]);
        if name.starts_with("LABELV8") {
            let count = header_num1(&rec)?;
            read_labels_v8(&mut r, &mut columns, count)?;
            read_line(&mut r)?; // OBSV8
        } else if name.starts_with("LABELV9") {
            let count = header_num1(&rec)?;
            read_labels_v9(&mut r, &mut columns, count)?;
            read_line(&mut r)?; // OBSV8
        }
        // else it's already OBSV8 — nothing more to read
    } else {
        // v5: OBS header
        read_line(&mut r)?;
    }

    let data_offset = r
        .stream_position()
        .map_err(|e| PolarsError::ComputeError(format!("XPT seek: {e}").into()))?;

    let file_size = r
        .get_ref()
        .metadata()
        .map_err(|e| PolarsError::ComputeError(format!("XPT stat: {e}").into()))?
        .len();

    let row_length: usize = columns.iter().map(|c| c.storage_width).sum();
    let row_count = if row_length == 0 {
        0
    } else {
        ((file_size.saturating_sub(data_offset)) as usize) / row_length
    };

    Ok(XptMetadata {
        version,
        table_name,
        file_label,
        columns,
        row_length,
        data_offset,
        row_count,
    })
}

// ────────────────────────────────────────────────────────────────
// IBM XPORT → IEEE 754 double conversion
//
// IBM format (big-endian, 8 bytes):
//   byte 0: sign(1) | exponent(7, excess-64)
//   bytes 1-7: 56-bit hex mantissa (big-endian)
//   value = (-1)^sign × mantissa × 2^(4×exponent − 312)
//   where mantissa is the 56-bit integer from bytes 1-7.
//
// Missing values: bytes 1..width are all zero AND byte 0 is
//   '.' (system missing) or 'A'–'Z' / '_' (tagged missing).
// ────────────────────────────────────────────────────────────────

fn is_xpt_missing(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    if !bytes[1..].iter().all(|&b| b == 0) {
        return false;
    }
    let b = bytes[0];
    b == b'.' || (b >= b'A' && b <= b'Z') || b == b'_'
}

fn xpt_to_f64(xport: &[u8; 8]) -> Option<f64> {
    if is_xpt_missing(xport) {
        return None;
    }

    let sign = (xport[0] >> 7) as u64;
    let ibm_exp = (xport[0] & 0x7F) as i32;

    // 56-bit mantissa from bytes 1–7 (big-endian)
    let mantissa: u64 = ((xport[1] as u64) << 48)
        | ((xport[2] as u64) << 40)
        | ((xport[3] as u64) << 32)
        | ((xport[4] as u64) << 24)
        | ((xport[5] as u64) << 16)
        | ((xport[6] as u64) << 8)
        | (xport[7] as u64);

    if mantissa == 0 {
        return Some(if sign != 0 { -0.0_f64 } else { 0.0_f64 });
    }

    // Position of leading 1 bit (0 = LSB), within a 64-bit word where
    // the mantissa occupies bits 55-0.
    let leading_zeros = mantissa.leading_zeros() as i32;
    let k = 63 - leading_zeros; // 0-indexed bit position from LSB

    // value = mantissa × 2^(4×ibm_exp − 312)
    // = 2^k × (1 + frac/2^k) × 2^(4×ibm_exp − 312)
    // → ieee_exp = k + 4×ibm_exp − 312 + 1023 = k + 4×ibm_exp + 711
    let ieee_exp = k + 4 * ibm_exp + 711;

    if ieee_exp <= 0 {
        return Some(if sign != 0 { -0.0_f64 } else { 0.0_f64 });
    }
    if ieee_exp >= 2047 {
        return Some(if sign != 0 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }

    // Extract 52-bit IEEE fraction from the bits below the leading 1.
    let frac_bits = mantissa ^ (1u64 << k); // clear leading 1
    let fraction: u64 = if k >= 52 {
        frac_bits >> (k - 52)
    } else {
        frac_bits << (52 - k)
    };
    let fraction = fraction & 0x000F_FFFF_FFFF_FFFF;

    let bits: u64 = (sign << 63) | ((ieee_exp as u64) << 52) | fraction;
    Some(f64::from_bits(bits))
}

// Decode a numeric XPORT field of `width` bytes into an f64.
// The field is zero-padded on the right to 8 bytes before conversion.
fn decode_xpt_numeric(field: &[u8]) -> Option<f64> {
    let mut padded = [0u8; 8];
    let n = field.len().min(8);
    padded[..n].copy_from_slice(&field[..n]);
    xpt_to_f64(&padded)
}

// ────────────────────────────────────────────────────────────────
// Column kind (for Polars type mapping)
// ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
enum ColKind {
    Numeric,
    Date,
    DateTime,
    Time,
    Character,
}

fn col_kind(col: &XptColumn) -> ColKind {
    match col.col_type {
        XptColumnType::Character => ColKind::Character,
        XptColumnType::Numeric => {
            let upper = col.format.to_uppercase();
            // Check DATETIME before DATE (both start with "DATE")
            if DATETIME_FORMATS.iter().any(|&f| upper.starts_with(f)) {
                ColKind::DateTime
            } else if DATE_FORMATS.iter().any(|&f| upper.starts_with(f)) {
                ColKind::Date
            } else if TIME_FORMATS.iter().any(|&f| upper.starts_with(f)) {
                ColKind::Time
            } else {
                ColKind::Numeric
            }
        }
    }
}

fn col_dtype(kind: ColKind) -> DataType {
    match kind {
        ColKind::Numeric => DataType::Float64,
        ColKind::Date => DataType::Date,
        ColKind::DateTime => DataType::Datetime(TimeUnit::Microseconds, None),
        ColKind::Time => DataType::Time,
        ColKind::Character => DataType::String,
    }
}

// ────────────────────────────────────────────────────────────────
// DataFrame builder for one batch of XPT rows
// ────────────────────────────────────────────────────────────────

enum ColBuf {
    Numeric(PrimitiveChunkedBuilder<Float64Type>),
    Date(PrimitiveChunkedBuilder<Int32Type>),
    DateTime(PrimitiveChunkedBuilder<Int64Type>),
    Time(PrimitiveChunkedBuilder<Int64Type>),
    Character(StringChunkedBuilder),
}

impl ColBuf {
    fn new(name: &str, kind: ColKind, cap: usize) -> Self {
        match kind {
            ColKind::Numeric => {
                ColBuf::Numeric(PrimitiveChunkedBuilder::<Float64Type>::new(name.into(), cap))
            }
            ColKind::Date => {
                ColBuf::Date(PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), cap))
            }
            ColKind::DateTime => {
                ColBuf::DateTime(PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), cap))
            }
            ColKind::Time => {
                ColBuf::Time(PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), cap))
            }
            ColKind::Character => ColBuf::Character(StringChunkedBuilder::new(name.into(), cap)),
        }
    }

    fn finish(self) -> PolarsResult<Series> {
        match self {
            ColBuf::Numeric(b) => Ok(b.finish().into_series()),
            ColBuf::Date(b) => b.finish().into_series().cast(&DataType::Date),
            ColBuf::DateTime(b) => b
                .finish()
                .into_series()
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None)),
            ColBuf::Time(b) => b.finish().into_series().cast(&DataType::Time),
            ColBuf::Character(b) => Ok(b.finish().into_series()),
        }
    }

    #[allow(dead_code)]
    fn push_null(&mut self) {
        match self {
            ColBuf::Numeric(b) => b.append_null(),
            ColBuf::Date(b) => b.append_null(),
            ColBuf::DateTime(b) => b.append_null(),
            ColBuf::Time(b) => b.append_null(),
            ColBuf::Character(b) => b.append_null(),
        }
    }
}

// SAS epoch: Jan 1, 1960 = days before Unix epoch (Jan 1, 1970) = 3653 days
const SAS_EPOCH_DAYS: i32 = 3653;
const SECS_PER_DAY: f64 = 86400.0;

fn sas_date_to_polars(v: f64) -> i32 {
    v as i32 - SAS_EPOCH_DAYS
}

fn sas_datetime_to_polars_us(v: f64) -> i64 {
    let unix_secs = v - SAS_EPOCH_DAYS as f64 * SECS_PER_DAY;
    (unix_secs * 1_000_000.0) as i64
}

fn sas_time_to_polars_ns(v: f64) -> i64 {
    (v * 1_000_000_000.0) as i64
}

struct XptBatchBuilder {
    columns: Vec<(XptColumn, ColKind)>,
    buffers: Vec<ColBuf>,
    missing_string_as_null: bool,
}

impl XptBatchBuilder {
    fn new(
        cols: &[(XptColumn, ColKind)],
        cap: usize,
        missing_string_as_null: bool,
    ) -> Self {
        let columns: Vec<(XptColumn, ColKind)> = cols.to_vec();
        let buffers = columns
            .iter()
            .map(|(col, kind)| ColBuf::new(&col.name, *kind, cap))
            .collect();
        Self {
            columns,
            buffers,
            missing_string_as_null,
        }
    }

    fn push_row(&mut self, row: &[u8]) {
        let mut offset = 0usize;
        for (i, (col, kind)) in self.columns.iter().enumerate() {
            let w = col.storage_width;
            let field = if offset + w <= row.len() {
                &row[offset..offset + w]
            } else {
                &[][..]
            };
            offset += w;

            let buf = &mut self.buffers[i];
            match kind {
                ColKind::Character => {
                    if let ColBuf::Character(b) = buf {
                        let trimmed = trim_bytes(field);
                        if trimmed.is_empty() {
                            if self.missing_string_as_null {
                                b.append_null();
                            } else {
                                b.append_value("");
                            }
                        } else {
                            b.append_value(String::from_utf8_lossy(trimmed).as_ref());
                        }
                    }
                }
                ColKind::Numeric | ColKind::Date | ColKind::DateTime | ColKind::Time => {
                    let opt = decode_xpt_numeric(field);
                    match kind {
                        ColKind::Numeric => {
                            if let ColBuf::Numeric(b) = buf {
                                b.append_option(opt);
                            }
                        }
                        ColKind::Date => {
                            if let ColBuf::Date(b) = buf {
                                b.append_option(opt.map(sas_date_to_polars));
                            }
                        }
                        ColKind::DateTime => {
                            if let ColBuf::DateTime(b) = buf {
                                b.append_option(opt.map(sas_datetime_to_polars_us));
                            }
                        }
                        ColKind::Time => {
                            if let ColBuf::Time(b) = buf {
                                b.append_option(opt.map(sas_time_to_polars_ns));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn build(self) -> PolarsResult<DataFrame> {
        let series: Vec<Series> = self
            .buffers
            .into_iter()
            .map(|buf| buf.finish())
            .collect::<PolarsResult<_>>()?;
        let cols: Vec<Column> = series.into_iter().map(Into::into).collect();
        DataFrame::new_infer_height(cols)
    }
}

// ────────────────────────────────────────────────────────────────
// Batch iterator
// ────────────────────────────────────────────────────────────────

struct XptBatchIter {
    reader: BufReader<File>,
    col_plan: Vec<(XptColumn, ColKind)>,
    row_length: usize,
    batch_size: usize,
    missing_string_as_null: bool,
    row_index_name: Option<String>,
    row_cursor: usize,
    #[allow(dead_code)]
    total: usize,
    remaining: usize,
    // blank row buffering (XPT padding uses space-filled rows)
    pending_blanks: usize,
    blank_row: Vec<u8>,
    done: bool,
}

impl XptBatchIter {
    fn new(
        path: &Path,
        meta: &XptMetadata,
        col_plan: Vec<(XptColumn, ColKind)>,
        batch_size: usize,
        skip: usize,
        n_rows: Option<usize>,
        missing_string_as_null: bool,
        row_index_name: Option<String>,
    ) -> PolarsResult<Self> {
        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("XPT open: {e}").into()))?;
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(meta.data_offset))
            .map_err(|e| PolarsError::ComputeError(format!("XPT seek: {e}").into()))?;

        let row_length = meta.row_length;
        let blank_row = vec![b' '; row_length];

        // Skip rows if needed (read and discard, respecting blank-row logic)
        let mut pending_blanks = 0usize;
        let mut skipped = 0usize;
        let mut row_buf = vec![0u8; row_length];
        while skipped < skip {
            match reader.read_exact(&mut row_buf) {
                Ok(()) => {}
                Err(_) => break,
            }
            if row_buf.iter().all(|&b| b == b' ') {
                pending_blanks += 1;
            } else {
                // flush buffered blanks
                let flush = pending_blanks.min(skip - skipped);
                skipped += flush;
                pending_blanks = 0;
                if skipped < skip {
                    skipped += 1; // count this non-blank row
                }
            }
        }
        // Any remaining pending_blanks are discarded (they would have been padding).

        let remaining = if let Some(n) = n_rows { n } else { usize::MAX };

        Ok(Self {
            reader,
            col_plan,
            row_length,
            batch_size,
            missing_string_as_null,
            row_index_name,
            row_cursor: skip,
            total: remaining,
            remaining,
            pending_blanks: 0,
            blank_row,
            done: false,
        })
    }

    fn next_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        if self.done || self.remaining == 0 {
            return Ok(None);
        }

        let take = self.batch_size.min(self.remaining);
        let mut builder = XptBatchBuilder::new(&self.col_plan, take, self.missing_string_as_null);
        let mut row_buf = vec![0u8; self.row_length];
        let mut count = 0usize;

        while count < take {
            match self.reader.read_exact(&mut row_buf) {
                Ok(()) => {}
                Err(_) => {
                    self.done = true;
                    // Trailing blank rows are XPT padding — discard them.
                    self.pending_blanks = 0;
                    break;
                }
            }

            if row_buf.iter().all(|&b| b == b' ') {
                // Accumulate; only emit if a non-blank row follows.
                self.pending_blanks += 1;
                continue;
            }

            // Non-blank row: flush any accumulated blank rows, then push this row.
            while self.pending_blanks > 0 && count < take {
                builder.push_row(&self.blank_row.clone());
                self.pending_blanks -= 1;
                count += 1;
            }
            if count < take {
                builder.push_row(&row_buf);
                count += 1;
            }
        }

        if count == 0 {
            self.done = true;
            return Ok(None);
        }

        self.remaining = self.remaining.saturating_sub(count);
        let row_start = self.row_cursor;
        self.row_cursor += count;

        let df = builder.build()?;

        if let Some(ref name) = self.row_index_name {
            let df = crate::append_row_index(df, name, row_start)?;
            Ok(Some(df))
        } else {
            Ok(Some(df))
        }
    }
}

impl Iterator for XptBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch() {
            Ok(Some(df)) => Some(Ok(df)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

// ────────────────────────────────────────────────────────────────
// Column plan helpers
// ────────────────────────────────────────────────────────────────

fn build_col_plan(
    meta: &XptMetadata,
    col_names: Option<&[String]>,
) -> Vec<(XptColumn, ColKind)> {
    let cols = match col_names {
        Some(names) => {
            let name_set: std::collections::HashSet<&str> =
                names.iter().map(|s| s.as_str()).collect();
            meta.columns
                .iter()
                .filter(|c| name_set.contains(c.name.as_str()))
                .cloned()
                .collect::<Vec<_>>()
        }
        None => meta.columns.clone(),
    };
    cols.into_iter().map(|c| {
        let kind = col_kind(&c);
        (c, kind)
    }).collect()
}

fn build_schema(col_plan: &[(XptColumn, ColKind)]) -> Schema {
    let mut schema = Schema::with_capacity(col_plan.len());
    for (col, kind) in col_plan {
        schema.with_column(col.name.as_str().into(), col_dtype(*kind));
    }
    schema
}

// ────────────────────────────────────────────────────────────────
// AnonymousScan implementation
// ────────────────────────────────────────────────────────────────

struct XptScan {
    path: PathBuf,
    threads: Option<usize>,
    preserve_order: bool,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    row_index_name: Option<String>,
    compress_opts: crate::CompressOptionsLite,
    col_plan: Vec<(XptColumn, ColKind)>,
}

impl XptScan {
    fn new(path: PathBuf, opts: &crate::ScanOptions) -> PolarsResult<Self> {
        let meta = read_xpt_metadata(&path)?;
        let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
        let col_plan = build_col_plan(&meta, None);
        Ok(Self {
            path,
            threads: opts.threads,
            preserve_order: opts.preserve_order.unwrap_or(true),
            missing_string_as_null,
            chunk_size: opts.chunk_size,
            row_index_name: opts.row_index_name.clone(),
            compress_opts: opts.compress_opts.clone(),
            col_plan,
        })
    }
}

impl AnonymousScan for XptScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _n_rows: Option<usize>) -> PolarsResult<SchemaRef> {
        let mut schema = build_schema(&self.col_plan);
        if let Some(ref name) = self.row_index_name {
            schema = crate::append_row_index_schema(schema, name)?;
        }
        Ok(Arc::new(schema))
    }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let columns = opts.with_columns.map(|cols| {
            let name_set: std::collections::HashSet<&str> =
                cols.iter().map(|s| s.as_str()).collect();
            self.col_plan
                .iter()
                .filter(|(c, _)| name_set.contains(c.name.as_str()))
                .map(|(c, _)| c.name.clone())
                .collect::<Vec<_>>()
        });

        let iter = xpt_batch_iter(
            self.path.clone(),
            self.threads,
            self.missing_string_as_null,
            self.chunk_size,
            self.preserve_order,
            self.row_index_name.clone(),
            columns,
            0,
            opts.n_rows,
        )?;

        let mut out: Option<DataFrame> = None;
        for batch in iter {
            let df = batch?;
            if let Some(acc) = out.as_mut() {
                acc.vstack_mut(&df)?;
            } else {
                out = Some(df);
            }
        }
        let df = out.unwrap_or_else(DataFrame::empty);

        if self.compress_opts.enabled {
            crate::compress_df_if_enabled(&df, &self.compress_opts)
                .map_err(|e| PolarsError::ComputeError(e.into()))
        } else {
            Ok(df)
        }
    }
}

// ────────────────────────────────────────────────────────────────
// Public entry points
// ────────────────────────────────────────────────────────────────

pub fn scan_xpt(
    path: impl Into<PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let scan = Arc::new(XptScan::new(path, &opts)?);
    LazyFrame::anonymous_scan(scan, Default::default())
}

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

struct ParallelXptBatchIter {
    rx: Receiver<(usize, PolarsResult<DataFrame>)>,
    handle: Option<JoinHandle<()>>,
    preserve_order: bool,
    buffer: BTreeMap<usize, PolarsResult<DataFrame>>,
    next_idx: usize,
    total_chunks: usize,
}

impl Iterator for ParallelXptBatchIter {
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
                    return self.buffer.remove(&self.next_idx).map(|item| {
                        self.next_idx += 1;
                        item
                    });
                }
            }
        }
    }
}

impl Drop for ParallelXptBatchIter {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Batch iterator for XPT files (used by readstat_batch_iter).
pub fn xpt_batch_iter(
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    columns: Option<Vec<String>>,
    skip: usize,
    n_rows: Option<usize>,
) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>> {
    let meta = read_xpt_metadata(&path)?;
    let col_plan = build_col_plan(&meta, columns.as_deref());

    let max_rows = meta.row_count.saturating_sub(skip);
    let total = n_rows.unwrap_or(max_rows).min(max_rows);
    let batch_size = chunk_size.unwrap_or(8192).max(1);

    if total == 0 {
        return Ok(Box::new(std::iter::empty()));
    }

    let n_threads = threads.unwrap_or(crate::default_thread_count()).max(1);
    if n_threads > 1 && total >= 1000 {
        let total_chunks = total.div_ceil(batch_size);
        let n_workers = n_threads.min(total_chunks);
        let (tx, rx) = mpsc::sync_channel::<(usize, PolarsResult<DataFrame>)>(n_workers);

        let path = Arc::new(path);
        let meta = Arc::new(meta);
        let col_plan = Arc::new(col_plan);
        let ranges = split_batch_ranges(total_chunks, n_workers);

        let handle = std::thread::spawn(move || {
            let pool = match ThreadPoolBuilder::new().num_threads(n_workers).build() {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.send((0, Err(PolarsError::ComputeError(e.to_string().into()))));
                    return;
                }
            };
            pool.install(|| {
                ranges.into_par_iter().for_each_with(
                    tx,
                    |sender, (batch_start, batch_count)| {
                        if batch_count == 0 {
                            return;
                        }
                        let worker_skip = skip + batch_start * batch_size;
                        let worker_rows = (batch_count * batch_size).min(total - batch_start * batch_size);
                        let row_index_name = row_index_name.clone();

                        let iter = match XptBatchIter::new(
                            &path,
                            &meta,
                            (*col_plan).clone(),
                            batch_size,
                            worker_skip,
                            Some(worker_rows),
                            missing_string_as_null,
                            row_index_name,
                        ) {
                            Ok(it) => it,
                            Err(e) => {
                                let _ = sender.send((batch_start, Err(e)));
                                return;
                            }
                        };

                        let mut local_idx = 0usize;
                        for batch in iter {
                            if sender.send((batch_start + local_idx, batch)).is_err() {
                                return;
                            }
                            local_idx += 1;
                        }
                    },
                );
            });
        });

        return Ok(Box::new(ParallelXptBatchIter {
            rx,
            handle: Some(handle),
            preserve_order,
            buffer: BTreeMap::new(),
            next_idx: 0,
            total_chunks,
        }));
    }

    // Serial path
    let iter = XptBatchIter::new(
        &path,
        &meta,
        col_plan,
        batch_size,
        skip,
        Some(total),
        missing_string_as_null,
        row_index_name,
    )?;
    Ok(Box::new(iter))
}

/// Export XPT metadata as JSON (mirrors the SAS7BDAT metadata_json format).
pub fn xpt_metadata_json(path: &Path) -> PolarsResult<String> {
    let meta = read_xpt_metadata(path)?;
    let columns: Vec<serde_json::Value> = meta
        .columns
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c.name,
                "label": if c.label.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(c.label.clone()) },
                "format": if c.format.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(c.format.clone()) },
                "type": match c.col_type { XptColumnType::Numeric => "Numeric", XptColumnType::Character => "Character" },
                "storage_width": c.storage_width,
            })
        })
        .collect();

    let v = serde_json::json!({
        "xpt_version": meta.version,
        "table_name": if meta.table_name.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(meta.table_name.clone()) },
        "file_label": if meta.file_label.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(meta.file_label.clone()) },
        "row_length": meta.row_length,
        "column_count": meta.columns.len(),
        "columns": columns,
    });
    Ok(v.to_string())
}
