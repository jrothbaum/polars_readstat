use chrono::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

const LINE_LEN: usize = 80;
const NAMESTR_SIZE: usize = 140;
const SAS_EPOCH_DAYS: i64 = 3653; // days between 1960-01-01 and 1970-01-01
const SECS_PER_DAY: f64 = 86400.0;

pub type XptVariableLabels = HashMap<String, String>;
pub type XptVariableFormats = HashMap<String, String>;
pub type XptStorageWidths = HashMap<String, usize>;

// ────────────────────────────────────────────────────────────────
// IBM XPORT ← IEEE 754 double conversion
//
// Inverse of xpt_to_f64 in xpt.rs.
//
// IBM format: value = mantissa × 2^(4×ibm_exp − 312)
// mantissa is a 56-bit integer; ibm_exp is the 7-bit biased exponent.
//
// Given IEEE: value = (-1)^s × (2^52 + F) × 2^(E − 1075)
// We want:    value = (-1)^s × M × 2^(4e − 312)
//
// Setting M = (2^52 + F) << adj and 4e = E − 763 − adj
// where adj = (E − 763) mod 4 ∈ {0,1,2,3} ensures e is an integer
// and M stays in [2^52, 2^56) (hex-normalised).
// ────────────────────────────────────────────────────────────────

fn f64_to_xpt(value: f64) -> [u8; 8] {
    if value.is_nan() || value.is_infinite() {
        return [b'.', 0, 0, 0, 0, 0, 0, 0];
    }
    if value == 0.0 {
        return [0u8; 8];
    }
    let bits = value.to_bits();
    let sign = (bits >> 63) as u8;
    let ieee_exp = ((bits >> 52) & 0x7FF) as i32;
    let ieee_frac = bits & 0x000F_FFFF_FFFF_FFFF;

    if ieee_exp == 0 {
        // Subnormal — too small to represent; emit zero.
        return [0u8; 8];
    }

    let s = (1u64 << 52) | ieee_frac;
    let adj = (ieee_exp - 763).rem_euclid(4) as u32;
    let ibm_exp = (ieee_exp - 763 - adj as i32) / 4;

    if !(0..=127).contains(&ibm_exp) {
        // Out of XPT range — emit system missing.
        return [b'.', 0, 0, 0, 0, 0, 0, 0];
    }

    let mantissa = s << adj;
    [
        (sign << 7) | ibm_exp as u8,
        (mantissa >> 48) as u8,
        (mantissa >> 40) as u8,
        (mantissa >> 32) as u8,
        (mantissa >> 24) as u8,
        (mantissa >> 16) as u8,
        (mantissa >> 8) as u8,
        mantissa as u8,
    ]
}

// ────────────────────────────────────────────────────────────────
// Format string parsing
// ────────────────────────────────────────────────────────────────

fn parse_format(s: &str) -> (String, u16, u16) {
    let s = s.trim();
    let i = s.find(|c: char| c.is_ascii_digit()).unwrap_or(s.len());
    let name = s[..i].trim_end_matches('.').to_ascii_uppercase();
    let rest = &s[i..];
    if let Some(dot) = rest.find('.') {
        let width = rest[..dot].parse::<u16>().unwrap_or(0);
        let dec = rest[dot + 1..].trim_end_matches('.').parse::<u16>().unwrap_or(0);
        (name, width, dec)
    } else {
        let width = rest.trim_end_matches('.').parse::<u16>().unwrap_or(0);
        (name, width, 0)
    }
}

// ────────────────────────────────────────────────────────────────
// Timestamp
// ────────────────────────────────────────────────────────────────

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];

fn format_timestamp() -> String {
    let now = Local::now();
    format!(
        "{:02}{}{:02}:{:02}:{:02}:{:02}",
        now.day(),
        MONTHS[now.month0() as usize],
        now.year() % 100,
        now.hour(),
        now.minute(),
        now.second()
    )
}

// ────────────────────────────────────────────────────────────────
// Column kinds
// ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
enum WriteKind {
    Numeric,
    Date,
    Datetime,
    Time,
    Character,
}

fn write_kind_for_dtype(dtype: &DataType) -> WriteKind {
    match dtype {
        DataType::Date => WriteKind::Date,
        DataType::Datetime(_, _) => WriteKind::Datetime,
        DataType::Time => WriteKind::Time,
        DataType::String | DataType::Categorical(_, _) => WriteKind::Character,
        _ => WriteKind::Numeric,
    }
}

fn default_format_for_kind(kind: WriteKind) -> Option<&'static str> {
    match kind {
        WriteKind::Date => Some("DATE9"),
        WriteKind::Datetime => Some("DATETIME20"),
        WriteKind::Time => Some("TIME8"),
        _ => None,
    }
}

// ────────────────────────────────────────────────────────────────
// Column plan
// ────────────────────────────────────────────────────────────────

struct WriteColumn {
    name: String,
    short_name: String,
    label_trunc: String, // first 40 bytes for NAMESTR
    full_label: String,
    format_name: String,
    format_width: u16,
    format_decimals: u16,
    kind: WriteKind,
    storage_width: usize,
    row_offset: usize,
    is_numeric: bool,
}

// ────────────────────────────────────────────────────────────────
// Write context
// ────────────────────────────────────────────────────────────────

struct Ctx<W: Write> {
    w: W,
    pos: usize,
}

impl<W: Write> Ctx<W> {
    fn new(w: W) -> Self {
        Self { w, pos: 0 }
    }

    fn write_all(&mut self, bytes: &[u8]) -> PolarsResult<()> {
        self.w
            .write_all(bytes)
            .map_err(|e| PolarsError::ComputeError(format!("XPT write: {e}").into()))?;
        self.pos += bytes.len();
        Ok(())
    }

    // Write `data` padded with spaces to exactly LINE_LEN bytes.
    fn write_record(&mut self, data: &[u8]) -> PolarsResult<()> {
        let n = data.len().min(LINE_LEN);
        self.write_all(&data[..n])?;
        if n < LINE_LEN {
            let pad = [b' '; LINE_LEN];
            self.write_all(&pad[..LINE_LEN - n])?;
        }
        Ok(())
    }

    // Pad the current partial record to the next LINE_LEN boundary.
    fn pad_to_record(&mut self) -> PolarsResult<()> {
        let rem = self.pos % LINE_LEN;
        if rem != 0 {
            let pad = [b' '; LINE_LEN];
            self.write_all(&pad[..LINE_LEN - rem])?;
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

// ────────────────────────────────────────────────────────────────
// Header record helpers
// ────────────────────────────────────────────────────────────────

// "HEADER RECORD*******{name:<8}HEADER RECORD!!!!!!!" + six %05d fields (78 bytes, padded to 80).
fn write_header_record<W: Write>(
    ctx: &mut Ctx<W>,
    name: &str,
    num1: usize,
    num2: usize,
    num3: usize,
    num4: usize,
    num5: usize,
    num6: usize,
) -> PolarsResult<()> {
    let s = format!(
        "HEADER RECORD*******{:<8}HEADER RECORD!!!!!!!{:05}{:05}{:05}{:05}{:05}{:05}",
        name, num1, num2, num3, num4, num5, num6
    );
    ctx.write_record(s.as_bytes())
}

// LABELV8/V9 header: num1 is left-justified in a 30-char field.
fn write_header_record_v8<W: Write>(
    ctx: &mut Ctx<W>,
    name: &str,
    num1: usize,
) -> PolarsResult<()> {
    let s = format!(
        "HEADER RECORD*******{:<8}HEADER RECORD!!!!!!!{:<30}",
        name, num1
    );
    ctx.write_record(s.as_bytes())
}

// ────────────────────────────────────────────────────────────────
// NAMESTR writer
// ────────────────────────────────────────────────────────────────

fn write_namestr<W: Write>(
    ctx: &mut Ctx<W>,
    col: &WriteColumn,
    index: usize,
    version: u8,
) -> PolarsResult<()> {
    let mut buf = [0u8; NAMESTR_SIZE];

    // Numeric fields (u16/u32, big-endian) — zero is the default.
    let ntype: u16 = if col.is_numeric { 1 } else { 2 };
    buf[0..2].copy_from_slice(&ntype.to_be_bytes());
    // nhfun [2..4] = 0
    buf[4..6].copy_from_slice(&(col.storage_width as u16).to_be_bytes());
    buf[6..8].copy_from_slice(&(index as u16).to_be_bytes());

    // Text fields — space-padded.
    space_pad(&mut buf[8..16], col.short_name.as_bytes()); // nname
    space_pad(&mut buf[16..56], col.label_trunc.as_bytes()); // nlabel
    space_pad(&mut buf[56..64], col.format_name.as_bytes()); // nform

    buf[64..66].copy_from_slice(&col.format_width.to_be_bytes()); // nfl
    buf[66..68].copy_from_slice(&col.format_decimals.to_be_bytes()); // nfd
    // nfj [68..70] = 0 (left-justify)
    // nfill [70..72] = 0

    space_pad(&mut buf[72..80], col.format_name.as_bytes()); // niform (copy of nform)
    buf[80..82].copy_from_slice(&col.format_width.to_be_bytes()); // nifl
    buf[82..84].copy_from_slice(&col.format_decimals.to_be_bytes()); // nifd

    buf[84..88].copy_from_slice(&(col.row_offset as u32).to_be_bytes()); // npos

    if version >= 8 {
        space_pad(&mut buf[88..120], col.name.as_bytes()); // longname
        let labeln = col.full_label.len() as u16;
        buf[120..122].copy_from_slice(&labeln.to_be_bytes()); // labeln
    }
    // rest [122..140] = 0

    ctx.write_all(&buf)
}

fn space_pad(dst: &mut [u8], src: &[u8]) {
    dst.fill(b' ');
    let n = src.len().min(dst.len());
    dst[..n].copy_from_slice(&src[..n]);
}

// ────────────────────────────────────────────────────────────────
// Column data extraction
// ────────────────────────────────────────────────────────────────

enum ColData {
    Numeric(Vec<Option<f64>>),
    Character(Vec<Option<Vec<u8>>>),
}

fn extract_col_data(df: &DataFrame, cols: &[WriteColumn]) -> PolarsResult<Vec<ColData>> {
    let mut out = Vec::with_capacity(cols.len());
    for (col_info, column) in cols.iter().zip(df.columns().iter()) {
        let series = column.as_materialized_series();
        let cd = match col_info.kind {
            WriteKind::Character => {
                let s2 = if matches!(series.dtype(), DataType::Categorical(_, _)) {
                    series.cast(&DataType::String)?
                } else {
                    series.clone()
                };
                let ca = s2.str().map_err(|e| {
                    PolarsError::ComputeError(
                        format!("XPT: expected string column '{}': {e}", col_info.name).into(),
                    )
                })?;
                let vals: Vec<Option<Vec<u8>>> = ca
                    .iter()
                    .map(|o: Option<&str>| o.map(|s| s.as_bytes().to_vec()))
                    .collect();
                ColData::Character(vals)
            }
            WriteKind::Numeric => {
                let ca = series.cast(&DataType::Float64)?;
                let ca = ca.f64()?;
                let vals: Vec<Option<f64>> = ca.iter().collect();
                ColData::Numeric(vals)
            }
            WriteKind::Date => {
                let ca = series.cast(&DataType::Date)?;
                let ca = ca.date()?;
                let vals: Vec<Option<f64>> = ca
                    .phys
                    .iter()
                    .map(|o: Option<i32>| o.map(|d| (d as i64 + SAS_EPOCH_DAYS) as f64))
                    .collect();
                ColData::Numeric(vals)
            }
            WriteKind::Datetime => {
                let ca = series.cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
                let ca = ca.datetime()?;
                let epoch_us = SAS_EPOCH_DAYS as f64 * SECS_PER_DAY * 1_000_000.0;
                let vals: Vec<Option<f64>> = ca
                    .phys
                    .iter()
                    .map(|o: Option<i64>| o.map(|us| (us as f64 + epoch_us) / 1_000_000.0))
                    .collect();
                ColData::Numeric(vals)
            }
            WriteKind::Time => {
                let ca = series.cast(&DataType::Time)?;
                let ca = ca.time()?;
                let vals: Vec<Option<f64>> = ca
                    .phys
                    .iter()
                    .map(|o: Option<i64>| o.map(|ns| ns as f64 / 1_000_000_000.0))
                    .collect();
                ColData::Numeric(vals)
            }
        };
        out.push(cd);
    }
    Ok(out)
}

// ────────────────────────────────────────────────────────────────
// Public writer
// ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct XptWriter {
    path: PathBuf,
    version: u8,
    table_name: String,
    file_label: String,
    variable_labels: XptVariableLabels,
    variable_formats: XptVariableFormats,
    storage_widths: XptStorageWidths,
}

impl XptWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            version: 8,
            table_name: String::new(),
            file_label: String::new(),
            variable_labels: HashMap::new(),
            variable_formats: HashMap::new(),
            storage_widths: HashMap::new(),
        }
    }

    pub fn with_version(mut self, v: u8) -> Self {
        self.version = v;
        self
    }
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = name.into();
        self
    }
    pub fn with_file_label(mut self, label: impl Into<String>) -> Self {
        self.file_label = label.into();
        self
    }
    pub fn with_variable_labels(mut self, labels: XptVariableLabels) -> Self {
        self.variable_labels = labels;
        self
    }
    pub fn with_variable_formats(mut self, formats: XptVariableFormats) -> Self {
        self.variable_formats = formats;
        self
    }
    pub fn with_storage_widths(mut self, widths: XptStorageWidths) -> Self {
        self.storage_widths = widths;
        self
    }

    pub fn write_df(self, df: &DataFrame) -> PolarsResult<()> {
        if self.version != 5 && self.version != 8 {
            return Err(PolarsError::ComputeError(
                "XPT version must be 5 or 8".into(),
            ));
        }
        let file = File::create(&self.path)
            .map_err(|e| PolarsError::ComputeError(format!("XPT create: {e}").into()))?;
        let mut ctx = Ctx::new(BufWriter::new(file));
        let (cols, row_length) = self.build_col_plan(df)?;
        let timestamp = format_timestamp();
        write_headers(&mut ctx, &self, &cols, row_length, &timestamp)?;
        write_data(&mut ctx, df, &cols, row_length)?;
        ctx.flush()
            .map_err(|e| PolarsError::ComputeError(format!("XPT flush: {e}").into()))
    }

    fn build_col_plan(&self, df: &DataFrame) -> PolarsResult<(Vec<WriteColumn>, usize)> {
        let mut cols = Vec::with_capacity(df.width());
        let mut offset = 0usize;

        // Validate names up front: length limit and no case collisions.
        let max_name_len = if self.version >= 8 { 32 } else { 8 };
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for column in df.columns().iter() {
            let name = column.name().as_str();
            if name.len() > max_name_len {
                return Err(PolarsError::ComputeError(
                    format!("XPT v{} variable name '{}' exceeds {} characters",
                        self.version, name, max_name_len).into(),
                ));
            }
            let upper = name.to_ascii_uppercase();
            if !seen.insert(upper) {
                return Err(PolarsError::ComputeError(
                    format!("XPT variable name '{}' collides with another column after uppercase mapping", name).into(),
                ));
            }
        }

        for column in df.columns().iter() {
            let series = column.as_materialized_series();
            let name = series.name().to_string();
            let dtype = series.dtype();
            let kind = write_kind_for_dtype(dtype);
            let is_numeric = !matches!(kind, WriteKind::Character);

            let storage_width = if is_numeric {
                if let Some(&w) = self.storage_widths.get(&name) {
                    w.clamp(3, 8)
                } else {
                    8usize
                }
            } else {
                // Character: scan actual data for max byte length; declared width is only used for the warning.
                let scan_width = series
                    .str()
                    .map(|ca| {
                        ca.iter()
                            .filter_map(|v: Option<&str>| v)
                            .map(|s| s.len())
                            .max()
                            .unwrap_or(1)
                    })
                    .unwrap_or(1)
                    .max(1);
                if let Some(&w) = self.storage_widths.get(&name) {
                    if scan_width > w {
                        eprintln!(
                            "warning: column '{}' declared storage_width={} but data contains strings up to {} bytes; using {}",
                            name, w, scan_width, scan_width
                        );
                    }
                }
                scan_width
            };

            let full_label = self
                .variable_labels
                .get(&name)
                .cloned()
                .unwrap_or_default();
            let label_trunc = truncate_utf8(&full_label, 40);

            let user_fmt = self.variable_formats.get(&name).map(|s| s.as_str());
            let auto_fmt = default_format_for_kind(kind);
            let fmt_str = user_fmt.or(auto_fmt).unwrap_or("");
            let (format_name, format_width, format_decimals) = if fmt_str.is_empty() {
                (String::new(), 0u16, 0u16)
            } else {
                parse_format(fmt_str)
            };

            let short_name = truncate_utf8(&name, 8);

            cols.push(WriteColumn {
                name,
                short_name,
                label_trunc,
                full_label,
                format_name,
                format_width,
                format_decimals,
                kind,
                storage_width,
                row_offset: offset,
                is_numeric,
            });

            offset += storage_width;
        }

        Ok((cols, offset))
    }
}

fn truncate_utf8(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

// ────────────────────────────────────────────────────────────────
// Header writing
// ────────────────────────────────────────────────────────────────

fn write_headers<W: Write>(
    ctx: &mut Ctx<W>,
    writer: &XptWriter,
    cols: &[WriteColumn],
    _row_length: usize,
    timestamp: &str,
) -> PolarsResult<()> {
    let is_v8 = writer.version >= 8;

    let ds_name = if writer.table_name.is_empty() {
        "DATASET".to_string()
    } else {
        let max = if is_v8 { 32 } else { 8 };
        truncate_utf8(&writer.table_name, max)
    };

    let file_label = truncate_utf8(&writer.file_label, 40);

    // 1. LIBRARY / LIBV8
    write_header_record(ctx, if is_v8 { "LIBV8" } else { "LIBRARY" }, 0, 0, 0, 0, 0, 0)?;

    // 2. SAS real header record
    // "%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-24.24s%16.16s"
    let real = format!(
        "{:<8.8}{:<8.8}{:<8.8}{:<8.8}{:<8.8}{:<24.24}{:>16.16}",
        "SAS", "SAS", "SASLIB", "6.06", "bsd4.2", "", timestamp
    );
    ctx.write_record(real.as_bytes())?;

    // 3. Timestamp record
    ctx.write_record(timestamp.as_bytes())?;

    // 4. MEMBER / MEMBV8  (num4=160, num6=140 per C reference)
    write_header_record(
        ctx,
        if is_v8 { "MEMBV8" } else { "MEMBER" },
        0,
        0,
        0,
        160,
        0,
        140,
    )?;

    // 5. DSCRPTR / DSCPTV8
    write_header_record(
        ctx,
        if is_v8 { "DSCPTV8" } else { "DSCRPTR" },
        0,
        0,
        0,
        0,
        0,
        0,
    )?;

    // 6. Member record
    if is_v8 {
        // "%-8.8s%-32.32s%-8.8s%-8.8s%-8.8s%16.16s"
        let rec = format!(
            "{:<8.8}{:<32.32}{:<8.8}{:<8.8}{:<8.8}{:>16.16}",
            "SAS", ds_name, "SASDATA", "6.06", "bsd4.2", timestamp
        );
        ctx.write_record(rec.as_bytes())?;
    } else {
        // "%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-24.24s%16.16s"
        let rec = format!(
            "{:<8.8}{:<8.8}{:<8.8}{:<8.8}{:<8.8}{:<24.24}{:>16.16}",
            "SAS", ds_name, "SASDATA", "6.06", "bsd4.2", "", timestamp
        );
        ctx.write_record(rec.as_bytes())?;
    }

    // 7. File label record
    // "%16.16s%16.16s%-40.40s%-8.8s"
    let label_rec = format!(
        "{:>16.16}{:>16.16}{:<40.40}{:<8.8}",
        timestamp, "", file_label, ""
    );
    ctx.write_record(label_rec.as_bytes())?;

    // 8. NAMESTR / NAMSTV8 header
    write_header_record(
        ctx,
        if is_v8 { "NAMSTV8" } else { "NAMESTR" },
        0,
        cols.len(),
        0,
        0,
        0,
        0,
    )?;

    // 9. NAMESTR records (continuous binary stream, padded to 80-byte boundary)
    let mut long_label_indices: Vec<usize> = Vec::new();
    for (i, col) in cols.iter().enumerate() {
        write_namestr(ctx, col, i + 1, writer.version)?;
        if is_v8 && col.full_label.len() > 40 {
            long_label_indices.push(i);
        }
    }
    ctx.pad_to_record()?;

    // 10. Optional LABELV8 (v8 only, for labels > 40 bytes)
    if is_v8 && !long_label_indices.is_empty() {
        write_header_record_v8(ctx, "LABELV8", long_label_indices.len())?;
        for &idx in &long_label_indices {
            let col = &cols[idx];
            let name_bytes = col.name.as_bytes();
            let label_bytes = col.full_label.as_bytes();
            let header: [u16; 3] = [
                (idx + 1) as u16,
                name_bytes.len() as u16,
                label_bytes.len() as u16,
            ];
            let mut hdr_be = [0u8; 6];
            hdr_be[0..2].copy_from_slice(&header[0].to_be_bytes());
            hdr_be[2..4].copy_from_slice(&header[1].to_be_bytes());
            hdr_be[4..6].copy_from_slice(&header[2].to_be_bytes());
            ctx.write_all(&hdr_be)?;
            ctx.write_all(name_bytes)?;
            ctx.write_all(label_bytes)?;
        }
        ctx.pad_to_record()?;
    }

    // 11. OBS / OBSV8
    write_header_record(ctx, if is_v8 { "OBSV8" } else { "OBS" }, 0, 0, 0, 0, 0, 0)?;

    Ok(())
}

// ────────────────────────────────────────────────────────────────
// Data writing
// ────────────────────────────────────────────────────────────────

fn write_data<W: Write>(
    ctx: &mut Ctx<W>,
    df: &DataFrame,
    cols: &[WriteColumn],
    row_length: usize,
) -> PolarsResult<()> {
    let nrows = df.height();
    if nrows == 0 || row_length == 0 {
        ctx.pad_to_record()?;
        return Ok(());
    }

    let col_data = extract_col_data(df, cols)?;
    let mut row_buf = vec![b' '; row_length];

    // Pre-fill numeric slots with the system-missing marker.
    for col in cols.iter() {
        if col.is_numeric {
            let field = &mut row_buf[col.row_offset..col.row_offset + col.storage_width];
            field[0] = b'.';
            for b in field.iter_mut().skip(1) {
                *b = 0;
            }
        }
    }

    for row_idx in 0..nrows {
        // Reset row buffer.
        for col in cols.iter() {
            if col.is_numeric {
                row_buf[col.row_offset] = b'.';
                for b in row_buf[col.row_offset + 1..col.row_offset + col.storage_width].iter_mut()
                {
                    *b = 0;
                }
            } else {
                row_buf[col.row_offset..col.row_offset + col.storage_width].fill(b' ');
            }
        }

        for (i, col) in cols.iter().enumerate() {
            match &col_data[i] {
                ColData::Numeric(vals) => {
                    if let Some(f) = vals[row_idx] {
                        let xpt = f64_to_xpt(f);
                        row_buf[col.row_offset..col.row_offset + col.storage_width]
                            .copy_from_slice(&xpt[..col.storage_width]);
                    }
                    // None: missing marker already set
                }
                ColData::Character(vals) => {
                    if let Some(ref bytes) = vals[row_idx] {
                        let n = bytes.len().min(col.storage_width);
                        row_buf[col.row_offset..col.row_offset + n].copy_from_slice(&bytes[..n]);
                        // rest already space-filled
                    }
                }
            }
        }

        ctx.write_all(&row_buf)?;
    }

    ctx.pad_to_record()?;
    Ok(())
}
