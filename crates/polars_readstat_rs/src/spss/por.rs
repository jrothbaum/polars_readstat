use crate::spss::error::{Error, Result};
use crate::spss::types::FormatClass;
use polars::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ─── Constants ────────────────────────────────────────────────────────────────

const POR_LINE_LEN: usize = 80;
const BASE30_PRECISION: usize = 50;
const SPSS_SEC_SHIFT: i64 = 12_219_379_200;
const SEC_PER_DAY: i64 = 86_400;

// ─── SPSS POR character table ─────────────────────────────────────────────────
//
// Index i → the ASCII byte that SPSS POR "position i" represents.
// Positions 64-73: '0'-'9', 74-99: 'A'-'Z', 100-125: 'a'-'z'.
// All other positions relevant to POR syntax are listed; 0 means no mapping.

static POR_ASCII_LOOKUP: [u8; 256] = {
    let mut t = [0u8; 256];
    // digits
    t[64] = b'0'; t[65] = b'1'; t[66] = b'2'; t[67] = b'3'; t[68] = b'4';
    t[69] = b'5'; t[70] = b'6'; t[71] = b'7'; t[72] = b'8'; t[73] = b'9';
    // uppercase
    t[74]  = b'A'; t[75]  = b'B'; t[76]  = b'C'; t[77]  = b'D'; t[78]  = b'E';
    t[79]  = b'F'; t[80]  = b'G'; t[81]  = b'H'; t[82]  = b'I'; t[83]  = b'J';
    t[84]  = b'K'; t[85]  = b'L'; t[86]  = b'M'; t[87]  = b'N'; t[88]  = b'O';
    t[89]  = b'P'; t[90]  = b'Q'; t[91]  = b'R'; t[92]  = b'S'; t[93]  = b'T';
    t[94]  = b'U'; t[95]  = b'V'; t[96]  = b'W'; t[97]  = b'X'; t[98]  = b'Y';
    t[99]  = b'Z';
    // lowercase
    t[100] = b'a'; t[101] = b'b'; t[102] = b'c'; t[103] = b'd'; t[104] = b'e';
    t[105] = b'f'; t[106] = b'g'; t[107] = b'h'; t[108] = b'i'; t[109] = b'j';
    t[110] = b'k'; t[111] = b'l'; t[112] = b'm'; t[113] = b'n'; t[114] = b'o';
    t[115] = b'p'; t[116] = b'q'; t[117] = b'r'; t[118] = b's'; t[119] = b't';
    t[120] = b'u'; t[121] = b'v'; t[122] = b'w'; t[123] = b'x'; t[124] = b'y';
    t[125] = b'z';
    // punctuation / special
    t[126] = b' '; t[127] = b'.'; t[128] = b'<'; t[129] = b'(';
    t[130] = b'+'; t[131] = b'|'; t[132] = b'&'; t[133] = b'['; t[134] = b']';
    t[135] = b'!'; t[136] = b'$'; t[137] = b'*'; t[138] = b')'; t[139] = b';';
    t[140] = b'^'; t[141] = b'-'; t[142] = b'/'; t[143] = b'|'; t[144] = b',';
    t[145] = b'%'; t[146] = b'_'; t[147] = b'>'; t[148] = b'?'; t[149] = b'`';
    t[150] = b':'; t[151] = b'#'; t[152] = b'@'; t[153] = b'\'';t[154] = b'=';
    t[155] = b'"'; t[162] = b'~';
    t[184] = b'{'; t[185] = b'}'; t[186] = b'\\';
    t
};

// ─── Variable / Metadata ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PorVariable {
    pub name: String,
    pub width: u32,
    pub print_format_type: u32,
    pub print_format_width: u32,
    pub print_format_decimals: u32,
    pub write_format_type: u32,
    pub write_format_width: u32,
    pub write_format_decimals: u32,
    pub label: Option<String>,
    pub format_class: Option<FormatClass>,
}

impl PorVariable {
    fn is_string(&self) -> bool { self.width > 0 }
}

#[derive(Debug, Clone)]
pub struct PorMetadata {
    pub file_label: String,
    pub variables: Vec<PorVariable>,
    pub precision: u32,
    pub row_count: Option<u64>,
}

// ─── Line-aware byte stream ───────────────────────────────────────────────────

struct PorStream<R: Read> {
    inner: R,
    pos: usize,
    pending_spaces: usize,
    space: u8,
    byte2char: [u8; 256],
}

impl<R: Read> PorStream<R> {
    fn new(inner: R) -> Self {
        let mut byte2char = [0u8; 256];
        for i in 0..=255u8 { byte2char[i as usize] = i; }
        Self { inner, pos: 0, pending_spaces: 0, space: b' ', byte2char }
    }

    fn read_raw_byte(&mut self) -> std::io::Result<Option<u8>> {
        if self.pending_spaces > 0 {
            self.pending_spaces -= 1;
            return Ok(Some(self.space));
        }
        let mut b = [0u8; 1];
        match self.inner.read(&mut b) {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(b[0])),
            Err(e) => Err(e),
        }
    }

    // Read one byte from the logical stream, handling CRLF-to-space padding.
    fn read_byte(&mut self) -> Result<u8> {
        loop {
            match self.read_raw_byte()? {
                None => return Err(Error::ParseError("unexpected EOF in POR stream".into())),
                Some(b'\r') => {
                    match self.read_raw_byte()? {
                        Some(b'\n') | None => {}
                        Some(b) => {
                            // treat lone CR as LF, put back isn't available so just handle LF
                            let _ = b;
                        }
                    }
                    self.pending_spaces = POR_LINE_LEN.saturating_sub(self.pos);
                    self.pos = 0;
                }
                Some(b'\n') => {
                    self.pending_spaces = POR_LINE_LEN.saturating_sub(self.pos);
                    self.pos = 0;
                }
                Some(b) => {
                    self.pos += 1;
                    return Ok(b);
                }
            }
        }
    }

    fn read_n_raw(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n { out.push(self.read_byte()?); }
        Ok(out)
    }

    // Read one byte and map it through byte2char.
    fn read_char(&mut self) -> Result<u8> {
        let b = self.read_byte()?;
        Ok(self.byte2char[b as usize])
    }

    // Read n mapped chars.
    fn read_chars(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n { out.push(self.read_char()?); }
        Ok(out)
    }

    // Initialise byte2char from the 256-byte reverse-lookup in the file.
    // reverse_lookup[i] = file byte that represents POR table position i.
    // byte2char[file_byte] = ASCII char for that byte.
    fn set_char_table(&mut self, reverse_lookup: &[u8; 256]) {
        self.byte2char = [0u8; 256];
        for i in 0..256usize {
            let ch = POR_ASCII_LOOKUP[i];
            if ch != 0 {
                self.byte2char[reverse_lookup[i] as usize] = ch;
            }
        }
        // Space byte: table position 126 → ' '
        self.space = reverse_lookup[126];
        // Make sure '0' fills in for any unmapped bytes so we don't crash
        // on '0' padding bytes (which have no por_ascii_lookup entry).
        self.byte2char[reverse_lookup[64] as usize] = b'0';
    }

    // Read a base-30 double (terminated by '/').  Returns NaN for "*.".
    fn read_double(&mut self) -> Result<f64> {
        let c = self.read_char()?;
        self.read_double_peek(c)
    }

    fn read_double_peek(&mut self, first: u8) -> Result<f64> {
        if first == b'*' {
            let c2 = self.read_char()?;
            if c2 == b'.' { return Ok(f64::NAN); }
            return Err(Error::ParseError(format!("POR: expected '.' after '*', got '{}'", c2 as char)));
        }
        let mut chars = vec![first];
        loop {
            let c = self.read_char()?;
            if c == b'/' { break; }
            chars.push(c);
        }
        parse_base30_double(&chars)
    }

    fn read_integer(&mut self) -> Result<u64> {
        let v = self.read_double()?;
        if v.is_nan() || v < 0.0 || v > u64::MAX as f64 {
            return Err(Error::ParseError(format!("POR: invalid integer: {}", v)));
        }
        Ok(v as u64)
    }

    // Read a string field: <base30_len>/<n_bytes> (mapped through byte2char).
    fn read_string_field(&mut self) -> Result<String> {
        let len = self.read_integer()? as usize;
        let raw = self.read_chars(len)?;
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }

    // Like read_double but returns None if first char is 'Z' (end-of-data).
    fn maybe_read_double(&mut self) -> Result<Option<f64>> {
        let c = self.read_char()?;
        if c == b'Z' { return Ok(None); }
        Ok(Some(self.read_double_peek(c)?))
    }

    // Like read_string_field but returns None if first char is 'Z'.
    fn maybe_read_string_field(&mut self) -> Result<Option<String>> {
        let c = self.read_char()?;
        if c == b'Z' { return Ok(None); }
        let len = self.read_double_peek(c)? as usize;
        let raw = self.read_chars(len)?;
        Ok(Some(String::from_utf8_lossy(&raw).into_owned()))
    }
}

// ─── Base-30 parser ───────────────────────────────────────────────────────────

fn base30_digit(c: u8) -> Option<f64> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as f64),
        b'A'..=b'T' => Some((10 + c - b'A') as f64),
        _ => None,
    }
}

// Parse a base-30 encoded number from chars (the '/' terminator is already stripped).
// Format: [-] integer_digits [. frac_digits] [+|- exp_digits]
// Missing = "*." (handled by caller).
fn parse_base30_double(chars: &[u8]) -> Result<f64> {
    let mut i = 0;
    let negative = i < chars.len() && chars[i] == b'-';
    if negative || (i < chars.len() && chars[i] == b'+') { i += 1; }

    let mut num = 0.0f64;
    while i < chars.len() {
        let c = chars[i];
        if c == b'.' || c == b'+' || c == b'-' { break; }
        match base30_digit(c) {
            Some(d) => { num = num * 30.0 + d; i += 1; }
            None => return Err(Error::ParseError(format!("POR: invalid base-30 digit '{}'", c as char))),
        }
    }

    let mut frac = 0.0f64;
    if i < chars.len() && chars[i] == b'.' {
        i += 1;
        let mut denom = 30.0f64;
        while i < chars.len() && chars[i] != b'+' && chars[i] != b'-' {
            match base30_digit(chars[i]) {
                Some(d) => { frac += d / denom; denom *= 30.0; i += 1; }
                None => return Err(Error::ParseError(format!("POR: invalid base-30 frac digit '{}'", chars[i] as char))),
            }
        }
    }

    let mut exp_neg = false;
    let mut exp = 0.0f64;
    if i < chars.len() {
        exp_neg = chars[i] == b'-';
        i += 1;
        while i < chars.len() {
            match base30_digit(chars[i]) {
                Some(d) => { exp = exp * 30.0 + d; i += 1; }
                None => return Err(Error::ParseError(format!("POR: invalid base-30 exp digit '{}'", chars[i] as char))),
            }
        }
    }

    let mut val = num + frac;
    if exp != 0.0 {
        let e = if exp_neg { -exp } else { exp };
        val *= 30.0f64.powf(e);
    }
    if negative { val = -val; }
    Ok(val)
}

// ─── Format class ─────────────────────────────────────────────────────────────

fn format_class_from_por_type(code: u32) -> Option<FormatClass> {
    // Some SPSS versions shift date/time format codes by 82
    let c = if code > 82 { code - 82 } else { code };
    match c {
        20 | 23 | 24 | 38 | 39 => Some(FormatClass::Date),
        21 | 25 => Some(FormatClass::Time),
        22 | 41 => Some(FormatClass::DateTime),
        _ => None,
    }
}

// ─── Reader: parse metadata + data ───────────────────────────────────────────

pub fn read_por<P: AsRef<Path>>(path: P) -> Result<(PorMetadata, DataFrame)> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(1 << 20, file);
    let mut stream = PorStream::new(reader);

    // 1. Vanity (5 × 40 = 200 logical bytes).
    let vanity = stream.read_n_raw(200)?;
    // File label is in vanity row 1 (bytes 40-79), positions 20-39 of that row.
    let label_bytes = &vanity[60..80]; // vanity[1][20..40]
    let file_label = String::from_utf8_lossy(label_bytes)
        .trim_end_matches(' ')
        .to_string();

    // 2. 256-byte reverse lookup table.
    let raw_lookup = stream.read_n_raw(256)?;
    let mut lookup = [0u8; 256];
    lookup.copy_from_slice(&raw_lookup);
    stream.set_char_table(&lookup);

    // 3. Verify "SPSSPORT" signature.
    let sig_bytes = stream.read_chars(8)?;
    if sig_bytes != b"SPSSPORT" {
        return Err(Error::ParseError(format!(
            "not a POR file: signature {:?}",
            String::from_utf8_lossy(&sig_bytes)
        )));
    }

    // 4. Version byte + date string + time string.
    let _version_byte = stream.read_char()?;
    let _date = stream.read_string_field()?;
    let _time = stream.read_string_field()?;

    // 5. Tag dispatch loop (metadata records).
    let mut variables: Vec<PorVariable> = Vec::new();
    let mut precision = 20u32;

    loop {
        let tag = stream.read_char()?;
        match tag {
            b'1' | b'2' | b'3' => {
                // product/author/sub-product ID strings
                let _ = stream.read_string_field()?;
            }
            b'4' => {
                // variable count
                let _ = stream.read_integer()?;
            }
            b'5' => {
                precision = stream.read_integer()? as u32;
            }
            b'6' => {
                // case weight variable name
                let _ = stream.read_string_field()?;
            }
            b'7' => {
                // variable record: width, name, print_format(3), write_format(3)
                let width = stream.read_integer()? as u32;
                let name = stream.read_string_field()?;
                let pft = stream.read_integer()? as u32;
                let pfw = stream.read_integer()? as u32;
                let pfd = stream.read_integer()? as u32;
                let wft = stream.read_integer()? as u32;
                let wfw = stream.read_integer()? as u32;
                let wfd = stream.read_integer()? as u32;
                let format_class = if width == 0 { format_class_from_por_type(pft) } else { None };
                variables.push(PorVariable {
                    name,
                    width,
                    print_format_type: pft,
                    print_format_width: pfw,
                    print_format_decimals: pfd,
                    write_format_type: wft,
                    write_format_width: wfw,
                    write_format_decimals: wfd,
                    label: None,
                    format_class,
                });
            }
            b'8' => {
                // single missing value — read and discard
                if let Some(var) = variables.last() {
                    if var.is_string() { let _ = stream.read_string_field()?; }
                    else { let _ = stream.read_double()?; }
                } else {
                    let _ = stream.read_double()?;
                }
            }
            b'9' => {
                // LO THRU x: single value
                let _ = stream.read_double()?;
            }
            b'A' => {
                // x THRU HI: single value
                let _ = stream.read_double()?;
            }
            b'B' => {
                // missing value range: lo, hi
                if let Some(var) = variables.last() {
                    if var.is_string() {
                        let _ = stream.read_string_field()?;
                        let _ = stream.read_string_field()?;
                    } else {
                        let _ = stream.read_double()?;
                        let _ = stream.read_double()?;
                    }
                } else {
                    let _ = stream.read_double()?;
                    let _ = stream.read_double()?;
                }
            }
            b'C' => {
                // variable label
                let label = stream.read_string_field()?;
                if let Some(var) = variables.last_mut() {
                    var.label = Some(label);
                }
            }
            b'D' => {
                // value labels: n_vars, [var_names], n_labels, [key, label]...
                let n_vars = stream.read_integer()?;
                // Determine type from first var
                let is_string = {
                    let mut s = false;
                    for j in 0..n_vars {
                        let vname = stream.read_string_field()?;
                        if j == 0 {
                            s = variables.iter().any(|v| v.name == vname && v.is_string());
                        }
                    }
                    s
                };
                let n_labels = stream.read_integer()?;
                for _ in 0..n_labels {
                    if is_string { let _ = stream.read_string_field()?; }
                    else { let _ = stream.read_double()?; }
                    let _ = stream.read_string_field()?; // label text
                }
            }
            b'E' => {
                // document record: n_lines, [strings]
                let n = stream.read_integer()?;
                for _ in 0..n { let _ = stream.read_string_field()?; }
            }
            b'F' => break, // start of data
            _ => {
                return Err(Error::ParseError(format!(
                    "POR: unexpected tag '{}' (0x{:02x})", tag as char, tag
                )));
            }
        }
    }

    let meta = PorMetadata {
        file_label,
        variables: variables.clone(),
        precision,
        row_count: None,
    };

    // 6. Data rows.
    let df = read_por_data(&mut stream, &variables)?;

    Ok((meta, df))
}

fn read_por_data<R: Read>(
    stream: &mut PorStream<R>,
    variables: &[PorVariable],
) -> Result<DataFrame> {
    if variables.is_empty() {
        return Ok(DataFrame::empty());
    }

    // Pre-allocate builders.
    enum ColBuilder {
        Float(PrimitiveChunkedBuilder<Float64Type>),
        DateI32(PrimitiveChunkedBuilder<Int32Type>),
        DatetimeI64(PrimitiveChunkedBuilder<Int64Type>),
        TimeI64(PrimitiveChunkedBuilder<Int64Type>),
        Str(StringChunkedBuilder),
    }

    let initial_cap = 4096usize;
    let mut builders: Vec<ColBuilder> = variables
        .iter()
        .map(|v| {
            if v.is_string() {
                ColBuilder::Str(StringChunkedBuilder::new(v.name.as_str().into(), initial_cap))
            } else {
                match v.format_class {
                    Some(FormatClass::Date) => ColBuilder::DateI32(
                        PrimitiveChunkedBuilder::new(v.name.as_str().into(), initial_cap),
                    ),
                    Some(FormatClass::DateTime) => ColBuilder::DatetimeI64(
                        PrimitiveChunkedBuilder::new(v.name.as_str().into(), initial_cap),
                    ),
                    Some(FormatClass::Time) => ColBuilder::TimeI64(
                        PrimitiveChunkedBuilder::new(v.name.as_str().into(), initial_cap),
                    ),
                    None => ColBuilder::Float(PrimitiveChunkedBuilder::new(
                        v.name.as_str().into(),
                        initial_cap,
                    )),
                }
            }
        })
        .collect();

    'rows: loop {
        for (i, var) in variables.iter().enumerate() {
            if var.is_string() {
                match stream.maybe_read_string_field()? {
                    None => {
                        if i == 0 { break 'rows; }
                        return Err(Error::ParseError("POR: Z in middle of row".into()));
                    }
                    Some(s) => {
                        if let ColBuilder::Str(b) = &mut builders[i] {
                            b.append_value(&s);
                        }
                    }
                }
            } else {
                match stream.maybe_read_double()? {
                    None => {
                        if i == 0 { break 'rows; }
                        return Err(Error::ParseError("POR: Z in middle of row".into()));
                    }
                    Some(v) => {
                        let is_null = v.is_nan();
                        match &mut builders[i] {
                            ColBuilder::Float(b) => {
                                if is_null { b.append_null(); } else { b.append_value(v); }
                            }
                            ColBuilder::DateI32(b) => {
                                if is_null {
                                    b.append_null();
                                } else {
                                    let days = ((v as i64) - SPSS_SEC_SHIFT) / SEC_PER_DAY;
                                    b.append_value(days as i32);
                                }
                            }
                            ColBuilder::DatetimeI64(b) => {
                                if is_null {
                                    b.append_null();
                                } else {
                                    let ms = ((v as i64) - SPSS_SEC_SHIFT) * 1_000;
                                    b.append_value(ms);
                                }
                            }
                            ColBuilder::TimeI64(b) => {
                                if is_null {
                                    b.append_null();
                                } else {
                                    let ns = (v as i64) * 1_000_000_000;
                                    b.append_value(ns);
                                }
                            }
                            ColBuilder::Str(_) => unreachable!(),
                        }
                    }
                }
            }
        }
    }

    // Finalise columns.
    let series: Vec<Series> = builders
        .into_iter()
        .map(|b| match b {
            ColBuilder::Float(b) => b.finish().into_series(),
            ColBuilder::DateI32(b) => b
                .finish()
                .into_series()
                .cast(&DataType::Date)
                .expect("cast to Date"),
            ColBuilder::DatetimeI64(b) => b
                .finish()
                .into_series()
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .expect("cast to Datetime"),
            ColBuilder::TimeI64(b) => b
                .finish()
                .into_series()
                .cast(&DataType::Time)
                .expect("cast to Time"),
            ColBuilder::Str(b) => b.finish().into_series(),
        })
        .collect();

    DataFrame::new_infer_height(series.into_iter().map(Column::from).collect())
        .map_err(|e| Error::ParseError(e.to_string()))
}

// ─── LazyFrame scan ──────────────────────────────────────────────────────────

pub fn scan_por(path: impl Into<PathBuf>, _opts: crate::ScanOptions) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let scan = Arc::new(PorScan { path });
    LazyFrame::anonymous_scan(scan, Default::default())
}

struct PorScan { path: PathBuf }

impl AnonymousScan for PorScan {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let (_, mut df) = read_por(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        if let Some(cols) = opts.with_columns {
            let names: Vec<PlSmallStr> = cols.iter().cloned().collect();
            df = df
                .select(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        }

        if let Some(n) = opts.n_rows {
            df = df.slice(0, n);
        }

        Ok(df)
    }

    fn schema(&self, _n_rows: Option<usize>) -> PolarsResult<SchemaRef> {
        let file = File::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut stream = PorStream::new(reader);
        let meta = read_por_metadata_only(&mut stream)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut schema = Schema::with_capacity(meta.variables.len());
        for v in &meta.variables {
            let dtype = if v.is_string() {
                DataType::String
            } else {
                match v.format_class {
                    Some(FormatClass::Date) => DataType::Date,
                    Some(FormatClass::DateTime) => DataType::Datetime(TimeUnit::Milliseconds, None),
                    Some(FormatClass::Time) => DataType::Time,
                    None => DataType::Float64,
                }
            };
            schema.with_column(v.name.as_str().into(), dtype);
        }
        Ok(Arc::new(schema))
    }
}

fn read_por_metadata_only<R: Read>(stream: &mut PorStream<R>) -> Result<PorMetadata> {
    let vanity = stream.read_n_raw(200)?;
    let label_bytes = &vanity[60..80];
    let file_label = String::from_utf8_lossy(label_bytes)
        .trim_end_matches(' ')
        .to_string();

    let raw_lookup = stream.read_n_raw(256)?;
    let mut lookup = [0u8; 256];
    lookup.copy_from_slice(&raw_lookup);
    stream.set_char_table(&lookup);

    let sig = stream.read_chars(8)?;
    if sig != b"SPSSPORT" {
        return Err(Error::ParseError("not a POR file".into()));
    }

    let _version = stream.read_char()?;
    let _ = stream.read_string_field()?;
    let _ = stream.read_string_field()?;

    let mut variables: Vec<PorVariable> = Vec::new();
    let mut precision = 20u32;

    loop {
        let tag = stream.read_char()?;
        match tag {
            b'1' | b'2' | b'3' => { let _ = stream.read_string_field()?; }
            b'4' => { let _ = stream.read_integer()?; }
            b'5' => { precision = stream.read_integer()? as u32; }
            b'6' => { let _ = stream.read_string_field()?; }
            b'7' => {
                let width = stream.read_integer()? as u32;
                let name = stream.read_string_field()?;
                let pft = stream.read_integer()? as u32;
                let pfw = stream.read_integer()? as u32;
                let pfd = stream.read_integer()? as u32;
                let wft = stream.read_integer()? as u32;
                let wfw = stream.read_integer()? as u32;
                let wfd = stream.read_integer()? as u32;
                let format_class = if width == 0 { format_class_from_por_type(pft) } else { None };
                variables.push(PorVariable {
                    name, width,
                    print_format_type: pft, print_format_width: pfw, print_format_decimals: pfd,
                    write_format_type: wft, write_format_width: wfw, write_format_decimals: wfd,
                    label: None, format_class,
                });
            }
            b'8' => {
                if let Some(v) = variables.last() {
                    if v.is_string() { let _ = stream.read_string_field()?; }
                    else { let _ = stream.read_double()?; }
                } else { let _ = stream.read_double()?; }
            }
            b'9' | b'A' => { let _ = stream.read_double()?; }
            b'B' => {
                if let Some(v) = variables.last() {
                    if v.is_string() {
                        let _ = stream.read_string_field()?;
                        let _ = stream.read_string_field()?;
                    } else {
                        let _ = stream.read_double()?;
                        let _ = stream.read_double()?;
                    }
                } else {
                    let _ = stream.read_double()?;
                    let _ = stream.read_double()?;
                }
            }
            b'C' => {
                let label = stream.read_string_field()?;
                if let Some(v) = variables.last_mut() { v.label = Some(label); }
            }
            b'D' => {
                let n_vars = stream.read_integer()?;
                let mut is_string = false;
                for j in 0..n_vars {
                    let vname = stream.read_string_field()?;
                    if j == 0 {
                        is_string = variables.iter().any(|v| v.name == vname && v.is_string());
                    }
                }
                let n_labels = stream.read_integer()?;
                for _ in 0..n_labels {
                    if is_string { let _ = stream.read_string_field()?; }
                    else { let _ = stream.read_double()?; }
                    let _ = stream.read_string_field()?;
                }
            }
            b'E' => {
                let n = stream.read_integer()?;
                for _ in 0..n { let _ = stream.read_string_field()?; }
            }
            b'F' | b'Z' => break,
            _ => return Err(Error::ParseError(format!("POR schema: unexpected tag '{}'", tag as char))),
        }
    }

    Ok(PorMetadata { file_label, variables, precision, row_count: None })
}

// ─── Metadata JSON ────────────────────────────────────────────────────────────

pub fn metadata_json_por<P: AsRef<Path>>(path: P) -> Result<String> {
    use serde_json::{json, Map, Value};
    let file = File::open(path.as_ref())?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut stream = PorStream::new(reader);
    let meta = read_por_metadata_only(&mut stream)?;

    let variables: Vec<Value> = meta.variables.iter().map(|v| {
        let mut obj = Map::new();
        obj.insert("name".into(), json!(v.name));
        obj.insert("type".into(), json!(if v.is_string() { "Str" } else { "Numeric" }));
        obj.insert("width".into(), json!(v.width));
        obj.insert("format_type".into(), json!(v.print_format_type));
        obj.insert("format_width".into(), json!(v.print_format_width));
        obj.insert("format_decimals".into(), json!(v.print_format_decimals));
        obj.insert("label".into(), json!(v.label));
        Value::Object(obj)
    }).collect();

    Ok(json!({
        "file_label": meta.file_label,
        "precision": meta.precision,
        "variables": variables,
    }).to_string())
}

pub fn metadata_por<P: AsRef<Path>>(path: P) -> Result<PorMetadata> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut stream = PorStream::new(reader);
    read_por_metadata_only(&mut stream)
}

// ─── Writer ───────────────────────────────────────────────────────────────────

struct PorLineWriter<W: Write> {
    inner: W,
    pos: usize,
}

impl<W: Write> PorLineWriter<W> {
    fn new(inner: W) -> Self { Self { inner, pos: 0 } }

    fn write_byte(&mut self, b: u8) -> Result<()> {
        self.inner.write_all(&[b])?;
        self.pos += 1;
        if self.pos == POR_LINE_LEN {
            self.inner.write_all(b"\r\n")?;
            self.pos = 0;
        }
        Ok(())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        for &b in bytes { self.write_byte(b)?; }
        Ok(())
    }

    fn write_base30_int(&mut self, mut n: u64) -> Result<()> {
        if n == 0 {
            self.write_byte(b'0')?;
            return Ok(());
        }
        let mut digits = [0u8; 20];
        let mut len = 0usize;
        while n > 0 {
            let d = (n % 30) as u8;
            digits[len] = if d < 10 { b'0' + d } else { b'A' + d - 10 };
            n /= 30;
            len += 1;
        }
        for &d in digits[..len].iter().rev() { self.write_byte(d)?; }
        Ok(())
    }

    fn write_double(&mut self, v: f64) -> Result<()> {
        if v.is_nan() {
            self.write_byte(b'*')?;
            self.write_byte(b'.')?;
            return Ok(());
        }
        if v.is_infinite() {
            if v < 0.0 { self.write_byte(b'-')?; }
            // Write 1+TT/ (very large number in base-30)
            self.write_bytes(b"1+TT/")?;
            return Ok(());
        }
        if v < 0.0 { self.write_byte(b'-')?; }
        let abs = v.abs();
        let int_part = abs.trunc() as u64;
        let mut frac = abs.fract();

        // Compress trailing zeros from integer part via exponent
        let mut exponent: u64 = 0;
        let mut int = int_part;
        if int == 0 {
            self.write_byte(b'0')?;
        } else {
            while frac == 0.0 && int != 0 && int % 30 == 0 {
                int /= 30;
                exponent += 1;
            }
            self.write_base30_int(int)?;
        }

        if frac != 0.0 {
            self.write_byte(b'.')?;
            let mut printed = count_base30_digits(int_part);
            while frac != 0.0 && printed < BASE30_PRECISION {
                frac *= 30.0;
                let d = frac.trunc() as u64;
                frac = frac.fract();
                let ch = if d < 10 { b'0' + d as u8 } else { b'A' + d as u8 - 10 };
                self.write_byte(ch)?;
                printed += 1;
            }
        }

        if exponent > 0 {
            self.write_byte(b'+')?;
            self.write_base30_int(exponent)?;
        }
        self.write_byte(b'/')?;
        Ok(())
    }

    fn write_string_field(&mut self, s: &str) -> Result<()> {
        let bytes = s.as_bytes();
        self.write_base30_int(bytes.len() as u64)?;
        self.write_byte(b'/')?;
        self.write_bytes(bytes)?;
        Ok(())
    }

    // Pad current line to 80 chars with 'Z' and emit CRLF.
    fn finish_line(&mut self) -> Result<()> {
        while self.pos != 0 { self.write_byte(b'Z')?; }
        Ok(())
    }
}

fn count_base30_digits(mut n: u64) -> usize {
    if n == 0 { return 1; }
    let mut c = 0;
    while n > 0 { n /= 30; c += 1; }
    c
}

// ─── Public write API ─────────────────────────────────────────────────────────

pub struct PorWriteOptions {
    pub file_label: Option<String>,
    pub variable_labels: Option<std::collections::HashMap<String, String>>,
}

impl Default for PorWriteOptions { fn default() -> Self { Self { file_label: None, variable_labels: None } } }

enum PorWriteData {
    String(StringChunked),
    Float64(Float64Chunked),
    Float32(Float32Chunked),
    Date(Int32Chunked),
    Datetime(Int64Chunked),
    Time(Int64Chunked),
    Numeric(Float64Chunked),
}

struct PorWritePlan {
    original_name: String,
    por_name: String,
    str_width: u32,
    fmt_type: u32,
    fmt_width: u32,
    fmt_dec: u32,
    data: PorWriteData,
}

fn polars_err(err: PolarsError) -> Error {
    Error::ParseError(err.to_string())
}

pub fn write_por<P: AsRef<Path>>(
    df: &DataFrame,
    path: P,
    opts: PorWriteOptions,
) -> Result<()> {
    let file = File::create(path.as_ref())?;
    let writer = BufWriter::with_capacity(1 << 20, file);
    let mut w = PorLineWriter::new(writer);

    let file_label = opts.file_label.as_deref().unwrap_or("").to_string();
    let var_labels = opts.variable_labels.unwrap_or_default();

    // 1. Vanity (5 × 40 = 200 bytes written raw, line-wrapped by PorLineWriter).
    let mut vanity = [b'0'; 200];
    // Row 0 (bytes 0-39): leave as '0'
    // Row 1 (bytes 40-79): "ASCII SPSS PORT FILE" + file_label (space-padded to 20)
    let header_str = b"ASCII SPSS PORT FILE";
    vanity[40..60].copy_from_slice(header_str);
    let label_bytes = file_label.as_bytes();
    let label_len = label_bytes.len().min(20);
    vanity[60..60 + label_len].copy_from_slice(&label_bytes[..label_len]);
    for b in &mut vanity[60 + label_len..80] { *b = b' '; }
    // Rows 2-4: '0'
    w.write_bytes(&vanity)?;

    // 2. 256-byte lookup table: lookup[i] = por_ascii_lookup[i] if non-zero, else '0'.
    let mut lookup = [b'0'; 256];
    for i in 0..256 {
        if POR_ASCII_LOOKUP[i] != 0 { lookup[i] = POR_ASCII_LOOKUP[i]; }
    }
    w.write_bytes(&lookup)?;

    // 3. "SPSSPORT" signature.
    w.write_bytes(b"SPSSPORT")?;

    // 4. Version 'A' + date + time strings.
    w.write_byte(b'A')?;
    let now = chrono::Local::now();
    w.write_string_field(&now.format("%Y%m%d").to_string())?;
    w.write_string_field(&now.format("%H%M%S").to_string())?;

    // 5. Product identification.
    w.write_byte(b'1')?;
    w.write_string_field("polars_readstat")?;

    // 6. Variable count.
    w.write_byte(b'4')?;
    w.write_double(df.width() as f64)?;

    // 7. Precision.
    w.write_byte(b'5')?;
    w.write_double(BASE30_PRECISION as f64)?;

    // 8. Variable records.
    let columns = df.columns();

    // Validate names and prepare typed/cast column access once up front.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut plans = Vec::with_capacity(columns.len());
    for col in columns {
        let name_raw = col.name().as_str();
        if name_raw.chars().count() > 8 {
            return Err(Error::ParseError(format!(
                "POR variable name '{}' exceeds 8 characters", name_raw
            )));
        }
        let upper: String = name_raw
            .chars()
            .map(|c: char| if c.is_ascii_alphabetic() { c.to_ascii_uppercase() } else if c.is_ascii_digit() || "_@#$.".contains(c) { c } else { '_' })
            .collect();
        if !seen.insert(upper.clone()) {
            return Err(Error::ParseError(format!(
                "POR variable name '{}' collides with another column after uppercase mapping", name_raw
            )));
        }

        let dtype = col.dtype();
        let is_string = matches!(dtype, DataType::String);

        let (str_width, data) = match dtype {
            DataType::String => {
                let ca = col.str().map_err(polars_err)?.clone();
                let width = ca.iter().filter_map(|o| o).map(|s| s.len()).max().unwrap_or(8) as u32;
                (width, PorWriteData::String(ca))
            }
            DataType::Float64 => (0, PorWriteData::Float64(col.f64().map_err(polars_err)?.clone())),
            DataType::Float32 => (0, PorWriteData::Float32(col.f32().map_err(polars_err)?.clone())),
            DataType::Date => {
                let s = col.as_materialized_series().cast(&DataType::Int32).map_err(polars_err)?;
                (0, PorWriteData::Date(s.i32().map_err(polars_err)?.clone()))
            }
            DataType::Datetime(_, _) => {
                let s = col.as_materialized_series().cast(&DataType::Int64).map_err(polars_err)?;
                (0, PorWriteData::Datetime(s.i64().map_err(polars_err)?.clone()))
            }
            DataType::Time => {
                let s = col.as_materialized_series().cast(&DataType::Int64).map_err(polars_err)?;
                (0, PorWriteData::Time(s.i64().map_err(polars_err)?.clone()))
            }
            _ => {
                let s = col.as_materialized_series().cast(&DataType::Float64).map_err(polars_err)?;
                (0, PorWriteData::Numeric(s.f64().map_err(polars_err)?.clone()))
            }
        };

        let fmt_type: u32 = match dtype {
            DataType::String => 1,
            DataType::Date => 20,
            DataType::Datetime(_, _) => 22,
            DataType::Time => 21,
            _ => 5,
        };
        let fmt_width: u32 = if is_string { str_width.max(1) } else { 8 };
        let fmt_dec: u32 = if is_string || fmt_type != 5 { 0 } else { 2 };

        plans.push(PorWritePlan {
            original_name: name_raw.to_string(),
            por_name: upper,
            str_width,
            fmt_type,
            fmt_width,
            fmt_dec,
            data,
        });
    }

    for plan in &plans {

        w.write_byte(b'7')?;
        w.write_double(plan.str_width as f64)?;       // width (0=numeric)
        w.write_string_field(&plan.por_name)?;         // name
        w.write_double(plan.fmt_type as f64)?;         // print format type
        w.write_double(plan.fmt_width as f64)?;        // print format width
        w.write_double(plan.fmt_dec as f64)?;          // print format decimals
        w.write_double(plan.fmt_type as f64)?;         // write format type (same)
        w.write_double(plan.fmt_width as f64)?;
        w.write_double(plan.fmt_dec as f64)?;

        // Optional variable label (tag 'C').
        let label = var_labels.get(&plan.original_name).or_else(|| var_labels.get(&plan.por_name));
        if let Some(lbl) = label {
            w.write_byte(b'C')?;
            w.write_string_field(lbl)?;
        }
    }

    // 9. Data tag.
    w.write_byte(b'F')?;

    // 10. Data rows.
    let n_rows = df.height();
    for row_idx in 0..n_rows {
        for plan in &plans {
            match &plan.data {
                PorWriteData::String(ca) => {
                    let s = ca.get(row_idx).unwrap_or("");
                    w.write_string_field(if s.is_empty() { " " } else { s })?;
                }
                PorWriteData::Float64(ca) => w.write_double(ca.get(row_idx).unwrap_or(f64::NAN))?,
                PorWriteData::Float32(ca) => {
                    w.write_double(ca.get(row_idx).map(|x| x as f64).unwrap_or(f64::NAN))?
                }
                PorWriteData::Date(ca) => {
                    let v = ca.get(row_idx)
                        .map(|d| (d as i64 * SEC_PER_DAY + SPSS_SEC_SHIFT) as f64)
                        .unwrap_or(f64::NAN);
                    w.write_double(v)?;
                }
                PorWriteData::Datetime(ca) => {
                    let v = ca.get(row_idx)
                        .map(|ms| (ms / 1_000 + SPSS_SEC_SHIFT) as f64)
                        .unwrap_or(f64::NAN);
                    w.write_double(v)?;
                }
                PorWriteData::Time(ca) => {
                    let v = ca.get(row_idx)
                        .map(|ns| (ns / 1_000_000_000) as f64)
                        .unwrap_or(f64::NAN);
                    w.write_double(v)?;
                }
                PorWriteData::Numeric(ca) => w.write_double(ca.get(row_idx).unwrap_or(f64::NAN))?,
            }
        }
    }

    // 11. End-of-data 'Z' + pad line.
    w.write_byte(b'Z')?;
    w.finish_line()?;

    Ok(())
}
