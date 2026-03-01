use crate::spss::error::{Error, Result};
use crate::spss::types::{Endian, FormatClass, Header, Metadata, VarType, Variable};
use std::io::{Read, Seek, SeekFrom};

/// Discard `n` bytes by reading them into a stack buffer.
/// Uses `read_exact` which is zero-syscall for a buffered reader,
/// unlike `seek(SeekFrom::Current(n))` which calls `stream_position()` for every seek.
#[inline]
fn drain<R: Read>(reader: &mut R, n: usize) -> std::io::Result<()> {
    // Stack buffer; large enough for all typical label/record skips.
    let mut buf = [0u8; 512];
    let mut remaining = n;
    while remaining > 0 {
        let chunk = remaining.min(buf.len());
        reader.read_exact(&mut buf[..chunk])?;
        remaining -= chunk;
    }
    Ok(())
}

const REC_TYPE_VARIABLE: u32 = 2;
const REC_TYPE_VALUE_LABEL: u32 = 3;
const REC_TYPE_VALUE_LABEL_VARIABLES: u32 = 4;
const REC_TYPE_DOCUMENT: u32 = 6;
const REC_TYPE_HAS_DATA: u32 = 7;
const REC_TYPE_DICT_TERMINATION: u32 = 999;

const SUBTYPE_CHAR_ENCODING: u32 = 20;
const SUBTYPE_INTEGER_INFO: u32 = 3;
const SUBTYPE_LONG_VAR_NAME: u32 = 13;
const SUBTYPE_VERY_LONG_STR: u32 = 14;
const SUBTYPE_LONG_STRING_VALUE_LABELS: u32 = 21;
const SUBTYPE_LONG_STRING_MISSING_VALUES: u32 = 22;

/// Pre-scan the record stream to find the character encoding, then reset to the start.
/// This ensures variable labels are decoded with the correct encoding from the outset.
fn prescan_char_encoding<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
) -> Option<&'static encoding_rs::Encoding> {
    let start = reader.stream_position().ok()?;
    let result = prescan_char_encoding_inner(reader, header);
    reader.seek(SeekFrom::Start(start)).ok()?;
    result
}

fn prescan_char_encoding_inner<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
) -> Option<&'static encoding_rs::Encoding> {
    let mut fallback_encoding: Option<&'static encoding_rs::Encoding> = None;
    loop {
        let rec_type = read_u32(reader, header.endian).ok()?;
        match rec_type {
            REC_TYPE_VARIABLE => {
                // Skip: read fixed 28 bytes, then optionally a label
                let mut buf = [0u8; 28];
                reader.read_exact(&mut buf).ok()?;
                let has_label = read_i32(&buf[4..8], header.endian);
                let n_missing = read_i32(&buf[8..12], header.endian);
                if has_label != 0 {
                    let len = read_u32(reader, header.endian).ok()? as usize;
                    let padded = ((len + 3) / 4) * 4;
                    drain(reader, padded).ok()?;
                }
                let abs_missing = n_missing.unsigned_abs() as usize;
                if abs_missing > 0 {
                    drain(reader, abs_missing * 8).ok()?;
                }
            }
            REC_TYPE_VALUE_LABEL => {
                // Mirror the actual read_value_label_record format:
                // N entries of (8 bytes key + 1 byte len + padded label),
                // followed by an embedded REC_TYPE_VALUE_LABEL_VARIABLES record.
                let count = read_u32(reader, header.endian).ok()? as usize;
                for _ in 0..count {
                    drain(reader, 8).ok()?; // 8-byte raw value
                    let vlen = read_u8(reader).ok()? as usize;
                    let padded = ((vlen + 8) / 8) * 8 - 1;
                    drain(reader, padded).ok()?;
                }
                // Consume embedded value_label_variables record
                let _rec = read_u32(reader, header.endian).ok()?; // should be 4
                let var_count = read_u32(reader, header.endian).ok()? as usize;
                drain(reader, var_count * 4).ok()?;
            }
            REC_TYPE_VALUE_LABEL_VARIABLES => {
                // Should not normally appear here (consumed inside VALUE_LABEL), but skip it.
                let var_count = read_u32(reader, header.endian).ok()? as usize;
                drain(reader, var_count * 4).ok()?;
            }
            REC_TYPE_DOCUMENT => {
                let line_count = read_u32(reader, header.endian).ok()? as usize;
                drain(reader, line_count * 80).ok()?;
            }
            REC_TYPE_HAS_DATA => {
                let subtype = read_u32(reader, header.endian).ok()?;
                let size = read_u32(reader, header.endian).ok()? as usize;
                let count = read_u32(reader, header.endian).ok()? as usize;
                let data_len = size * count;
                if subtype == SUBTYPE_CHAR_ENCODING && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf).ok()?;
                    if let Ok(codepage) = std::str::from_utf8(&buf) {
                        if let Some(enc) =
                            encoding_rs::Encoding::for_label(codepage.trim().as_bytes())
                        {
                            return Some(enc);
                        }
                    }
                } else if subtype == SUBTYPE_INTEGER_INFO && data_len >= 8 * 4 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf).ok()?;
                    let character_code = read_i32_from(&buf, 28, header.endian).ok()?;
                    if character_code > 0 {
                        if let Some(enc) = encoding_for_code(character_code) {
                            fallback_encoding = Some(enc);
                        }
                    }
                } else {
                    drain(reader, data_len).ok()?;
                }
            }
            REC_TYPE_DICT_TERMINATION | _ => break,
        }
    }
    fallback_encoding
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_metadata<R: Read + Seek>(reader: &mut R, header: &Header) -> Result<Metadata> {
    let mut metadata = Metadata::default();
    // Pre-scan to find character encoding before reading variable labels,
    // since the encoding info record comes after the variable records in the file.
    if let Some(enc) = prescan_char_encoding(reader, header) {
        metadata.encoding = enc;
    }
    metadata.row_count = header.row_count.max(0) as u64;

    let mut last_var_index: Option<usize> = None;
    let mut label_set_index = 0usize;
    let mut current_offset = 0usize;

    loop {
        let rec_type = read_u32(reader, header.endian)?;
        match rec_type {
            REC_TYPE_VARIABLE => {
                let variable = read_variable_record(
                    reader,
                    header,
                    last_var_index,
                    &mut metadata,
                    &mut current_offset,
                )?;
                if let Some(var) = variable {
                    metadata.variables.push(var);
                    last_var_index = Some(metadata.variables.len() - 1);
                }
            }
            REC_TYPE_VALUE_LABEL => {
                read_value_label_record(reader, header, &mut metadata, &mut label_set_index)?;
            }
            REC_TYPE_VALUE_LABEL_VARIABLES => {
                // part of value-label record; skip count and var indexes
                let var_count = read_u32(reader, header.endian)? as usize;
                drain(reader, var_count * 4)?;
            }
            REC_TYPE_DOCUMENT => {
                let line_count = read_u32(reader, header.endian)? as usize;
                drain(reader, line_count * 80)?;
            }
            REC_TYPE_HAS_DATA => {
                let subtype = read_u32(reader, header.endian)?;
                let size = read_u32(reader, header.endian)? as usize;
                let count = read_u32(reader, header.endian)? as usize;
                let data_len = size * count;
                if subtype == SUBTYPE_INTEGER_INFO && data_len >= 8 * 4 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_integer_info(&buf, header, &mut metadata)?;
                } else if subtype == SUBTYPE_CHAR_ENCODING && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    if let Ok(codepage) = std::str::from_utf8(&buf) {
                        if let Some(enc) =
                            encoding_rs::Encoding::for_label(codepage.trim().as_bytes())
                        {
                            update_encoding(&mut metadata, enc);
                        }
                    }
                } else if subtype == SUBTYPE_VERY_LONG_STR && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_very_long_string_record(&buf, &mut metadata)?;
                } else if subtype == SUBTYPE_LONG_VAR_NAME && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_long_variable_names_record(&buf, &mut metadata)?;
                } else if subtype == SUBTYPE_LONG_STRING_VALUE_LABELS && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_long_string_value_labels(&buf, header, &mut metadata)?;
                } else if subtype == SUBTYPE_LONG_STRING_MISSING_VALUES && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_long_string_missing_values(&buf, header, &mut metadata)?;
                } else {
                    drain(reader, data_len)?;
                }
            }
            REC_TYPE_DICT_TERMINATION => {
                let _filler = read_u32(reader, header.endian)?;
                metadata.data_offset = Some(reader.stream_position()?);
                break;
            }
            _ => {
                return Err(Error::ParseError(format!(
                    "unknown SPSS record type {}",
                    rec_type
                )))
            }
        }
    }

    coalesce_very_long_strings(&mut metadata)?;
    Ok(metadata)
}

fn coalesce_very_long_strings(metadata: &mut Metadata) -> Result<()> {
    let mut i = 0usize;
    while i < metadata.variables.len() {
        let string_len = metadata.variables[i].string_len;
        let is_long = metadata.variables[i].var_type == VarType::Str && string_len > 255;
        if !is_long {
            i += 1;
            continue;
        }

        let n_segments = (string_len + 251) / 252;
        if n_segments <= 1 {
            i += 1;
            continue;
        }

        let end = i + n_segments;
        if end > metadata.variables.len() {
            return Err(Error::ParseError(format!(
                "invalid very long string segment count for {}",
                metadata.variables[i].name
            )));
        }

        let total_width: usize = metadata.variables[i..end].iter().map(|v| v.width).sum();
        metadata.variables[i].width = total_width;
        metadata.variables.drain(i + 1..end);
        i += 1;
    }
    Ok(())
}

fn read_variable_record<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    last_var_index: Option<usize>,
    metadata: &mut Metadata,
    current_offset: &mut usize,
) -> Result<Option<Variable>> {
    let mut buf = [0u8; 28];
    reader.read_exact(&mut buf)?;

    let typ = read_i32(&buf[0..4], header.endian);
    let has_label = read_i32(&buf[4..8], header.endian);
    let n_missing = read_i32(&buf[8..12], header.endian);
    let print_format = read_i32(&buf[12..16], header.endian);
    let _write = read_i32(&buf[16..20], header.endian);
    let name = read_name(&buf[20..28]);

    if typ < 0 {
        let idx = last_var_index.ok_or_else(|| {
            Error::ParseError("string continuation without base variable".to_string())
        })?;
        metadata.variables[idx].width += 1;
        *current_offset += 1;
        return Ok(None);
    }

    let var_type = if typ == 0 {
        VarType::Numeric
    } else {
        VarType::Str
    };
    let string_len = if typ > 0 { typ as usize } else { 0 };
    let width = 1;
    let offset = *current_offset;
    *current_offset += width;

    let format_type = ((print_format as u32) >> 16) as u8;
    let format_class = format_class_from_type(format_type);

    let mut label: Option<String> = None;
    if has_label != 0 {
        let len = read_u32(reader, header.endian)? as usize;
        let padded = ((len + 3) / 4) * 4;
        let mut label_buf = vec![0u8; padded];
        reader.read_exact(&mut label_buf)?;
        let raw = &label_buf[..len.min(label_buf.len())];
        let text = metadata
            .encoding
            .decode_without_bom_handling(raw)
            .0
            .to_string();
        let text = text.trim().to_string();
        if !text.is_empty() {
            label = Some(text);
        }
    }

    let mut missing_range = false;
    let mut missing_doubles = Vec::new();
    let mut missing_double_bits = Vec::new();
    let mut missing_strings = Vec::new();
    if n_missing != 0 {
        let mut n = n_missing;
        if n < 0 {
            missing_range = true;
            n = -n;
        }
        let n = n as usize;
        for _ in 0..n {
            let mut raw = [0u8; 8];
            reader.read_exact(&mut raw)?;
            if var_type == VarType::Numeric {
                let v = read_f64(&raw, header.endian);
                missing_doubles.push(v);
                missing_double_bits.push(v.to_bits());
            } else {
                let s = decode_string(&raw, header.endian, metadata.encoding);
                missing_strings.push(s);
            }
        }
    }

    Ok(Some(Variable {
        name: name.clone(),
        short_name: name,
        var_type,
        width,
        string_len,
        format_type,
        format_class,
        label,
        value_label: None,
        offset,
        missing_range,
        missing_doubles,
        missing_double_bits,
        missing_strings,
    }))
}

fn format_class_from_type(code: u8) -> Option<FormatClass> {
    match code {
        // DATE, ADATE, JDATE, EDATE, SDATE
        20 | 23 | 24 | 38 | 39 => Some(FormatClass::Date),
        // TIME, DTIME
        21 | 25 => Some(FormatClass::Time),
        // DATETIME, YMDHMS
        22 | 41 => Some(FormatClass::DateTime),
        _ => None,
    }
}

fn read_value_label_record<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    metadata: &mut Metadata,
    label_set_index: &mut usize,
) -> Result<()> {
    let entry_count = read_u32(reader, header.endian)? as usize;
    let mut raw_values = Vec::with_capacity(entry_count);
    let mut labels = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        let mut raw = [0u8; 8];
        reader.read_exact(&mut raw)?;
        let mut len_buf = [0u8; 1];
        reader.read_exact(&mut len_buf)?;
        let unpadded = len_buf[0] as usize;
        let padded = ((unpadded + 8) / 8) * 8 - 1;
        let mut label_buf = vec![0u8; padded];
        reader.read_exact(&mut label_buf)?;
        let label = decode_string(&label_buf, header.endian, metadata.encoding);
        raw_values.push(raw);
        labels.push(label);
    }

    let rec_type = read_u32(reader, header.endian)?;
    if rec_type != REC_TYPE_VALUE_LABEL_VARIABLES {
        return Err(Error::ParseError(
            "invalid value label variables record".to_string(),
        ));
    }
    let var_count = read_u32(reader, header.endian)? as usize;
    let mut var_offsets = Vec::with_capacity(var_count);
    for _ in 0..var_count {
        let off = read_u32(reader, header.endian)?;
        var_offsets.push(off);
    }

    let name = format!("labels{}", *label_set_index);
    *label_set_index += 1;

    let mut mapping = Vec::with_capacity(entry_count);
    let mut is_string = false;
    for off in &var_offsets {
        let target = off.saturating_sub(1) as usize;
        if let Some(var) = metadata.variables.iter().find(|v| v.offset == target) {
            if var.var_type == VarType::Str {
                is_string = true;
                break;
            }
        }
    }
    for (raw, label) in raw_values.into_iter().zip(labels.into_iter()) {
        if label.is_empty() {
            continue;
        }
        if is_string {
            let s = decode_string(&raw, header.endian, metadata.encoding);
            mapping.push((crate::spss::types::ValueLabelKey::Str(s), label));
        } else {
            let v = read_f64(&raw, header.endian);
            mapping.push((crate::spss::types::ValueLabelKey::Double(v), label));
        }
    }

    metadata.value_labels.push(crate::spss::types::ValueLabel {
        name: name.clone(),
        mapping,
    });
    for off in var_offsets {
        let target = off.saturating_sub(1) as usize;
        if let Some(var) = metadata.variables.iter_mut().find(|v| v.offset == target) {
            var.value_label = Some(name.clone());
        }
    }
    Ok(())
}

fn read_u32<R: Read>(reader: &mut R, endian: Endian) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(match endian {
        Endian::Little => u32::from_le_bytes(buf),
        Endian::Big => u32::from_be_bytes(buf),
    })
}

fn read_i32(buf: &[u8], endian: Endian) -> i32 {
    let bytes: [u8; 4] = buf.try_into().expect("i32 slice");
    match endian {
        Endian::Little => i32::from_le_bytes(bytes),
        Endian::Big => i32::from_be_bytes(bytes),
    }
}

fn read_name(buf: &[u8]) -> String {
    let s = String::from_utf8_lossy(buf).trim().to_string();
    s.trim_end_matches('\u{0}').to_ascii_uppercase()
}

fn read_f64(buf: &[u8], endian: Endian) -> f64 {
    let bytes: [u8; 8] = buf.try_into().expect("f64 slice");
    match endian {
        Endian::Little => f64::from_le_bytes(bytes),
        Endian::Big => f64::from_be_bytes(bytes),
    }
}

fn decode_string(buf: &[u8], _endian: Endian, encoding: &'static encoding_rs::Encoding) -> String {
    let mut end = buf.len();
    while end > 0 && (buf[end - 1] == 0 || buf[end - 1] == b' ') {
        end -= 1;
    }
    let s = encoding.decode_without_bom_handling(&buf[..end]).0;
    s.trim().to_string()
}

fn redecode_string(
    value: &str,
    from: &'static encoding_rs::Encoding,
    to: &'static encoding_rs::Encoding,
) -> String {
    let (bytes, _, _) = from.encode(value);
    let s = to.decode_without_bom_handling(&bytes).0;
    s.trim().to_string()
}

fn redecode_metadata_strings(
    metadata: &mut Metadata,
    from: &'static encoding_rs::Encoding,
    to: &'static encoding_rs::Encoding,
) {
    for var in &mut metadata.variables {
        var.name = redecode_string(&var.name, from, to);
        var.short_name = redecode_string(&var.short_name, from, to);
        if let Some(label) = var.label.as_mut() {
            *label = redecode_string(label, from, to);
        }
        if let Some(label_name) = var.value_label.as_mut() {
            *label_name = redecode_string(label_name, from, to);
        }
        if !var.missing_strings.is_empty() {
            var.missing_strings = var
                .missing_strings
                .iter()
                .map(|s| redecode_string(s, from, to))
                .collect();
        }
    }

    for value_label in &mut metadata.value_labels {
        value_label.name = redecode_string(&value_label.name, from, to);
        for (key, label) in value_label.mapping.iter_mut() {
            if let crate::spss::types::ValueLabelKey::Str(s) = key {
                *s = redecode_string(s, from, to);
            }
            *label = redecode_string(label, from, to);
        }
    }
}

fn update_encoding(metadata: &mut Metadata, new_encoding: &'static encoding_rs::Encoding) {
    if metadata.encoding != new_encoding {
        let previous = metadata.encoding;
        metadata.encoding = new_encoding;
        redecode_metadata_strings(metadata, previous, new_encoding);
    }
}

fn parse_integer_info(data: &[u8], header: &Header, metadata: &mut Metadata) -> Result<()> {
    if data.len() < 32 {
        return Ok(());
    }
    let character_code = read_i32_from(data, 28, header.endian)?;
    if character_code > 0 {
        if let Some(enc) = encoding_for_code(character_code) {
            update_encoding(metadata, enc);
        }
    }
    Ok(())
}

fn encoding_for_code(code: i32) -> Option<&'static encoding_rs::Encoding> {
    let label = match code {
        2 | 3 | 1252 => "windows-1252",
        65001 => "utf-8",
        1200 => "utf-16le",
        1201 => "utf-16be",
        65000 => "utf-7",
        437 => "cp437",
        850 => "cp850",
        852 => "cp852",
        855 => "cp855",
        857 => "cp857",
        858 => "cp858",
        860 => "cp860",
        861 => "cp861",
        862 => "cp862",
        863 => "cp863",
        864 => "cp864",
        865 => "cp865",
        866 => "cp866",
        869 => "cp869",
        874 => "cp874",
        932 => "shift_jis",
        936 => "gbk",
        949 => "euc-kr",
        950 => "big5",
        1250 => "windows-1250",
        1251 => "windows-1251",
        1253 => "windows-1253",
        1254 => "windows-1254",
        1255 => "windows-1255",
        1256 => "windows-1256",
        1257 => "windows-1257",
        1258 => "windows-1258",
        28591 => "iso-8859-1",
        28592 => "iso-8859-2",
        28593 => "iso-8859-3",
        28594 => "iso-8859-4",
        28595 => "iso-8859-5",
        28596 => "iso-8859-6",
        28597 => "iso-8859-7",
        28598 => "iso-8859-8",
        28599 => "iso-8859-9",
        28605 => "iso-8859-15",
        20866 => "koi8-r",
        21866 => "koi8-u",
        51932 => "euc-jp",
        51936 => "gbk",
        51949 => "euc-kr",
        54936 => "gb18030",
        _ => return None,
    };
    encoding_rs::Encoding::for_label(label.as_bytes())
}

fn parse_very_long_string_record(data: &[u8], metadata: &mut Metadata) -> Result<()> {
    let mut pos = 0usize;
    while pos < data.len() {
        let end = data[pos..]
            .iter()
            .position(|&b| b == b'\t')
            .map(|i| pos + i)
            .unwrap_or(data.len());
        let entry: Vec<u8> = data[pos..end].iter().copied().filter(|b| *b != 0).collect();
        pos = if end < data.len() { end + 1 } else { end };
        if entry.is_empty() {
            continue;
        }
        if let Some(eq) = entry.iter().position(|&b| b == b'=') {
            let key = String::from_utf8_lossy(&entry[..eq]).to_string();
            let val = String::from_utf8_lossy(&entry[eq + 1..]).trim().to_string();
            if let Ok(len) = val.parse::<usize>() {
                if let Some(var) = metadata.variables.iter_mut().find(|v| {
                    v.short_name.eq_ignore_ascii_case(&key) || v.name.eq_ignore_ascii_case(&key)
                }) {
                    var.string_len = len;
                }
            }
        }
    }
    Ok(())
}

fn parse_long_variable_names_record(data: &[u8], metadata: &mut Metadata) -> Result<()> {
    let mut pos = 0usize;
    while pos < data.len() {
        let end = data[pos..]
            .iter()
            .position(|&b| b == b'\t')
            .map(|i| pos + i)
            .unwrap_or(data.len());
        let entry: Vec<u8> = data[pos..end].iter().copied().filter(|b| *b != 0).collect();
        pos = if end < data.len() { end + 1 } else { end };
        if entry.is_empty() {
            continue;
        }
        if let Some(eq) = entry.iter().position(|&b| b == b'=') {
            let key = String::from_utf8_lossy(&entry[..eq]).trim().to_string();
            let val = String::from_utf8_lossy(&entry[eq + 1..]).trim().to_string();
            if key.is_empty() || val.is_empty() {
                continue;
            }
            if let Some(var) = metadata
                .variables
                .iter_mut()
                .find(|v| v.name.eq_ignore_ascii_case(&key))
            {
                var.name = val;
            }
        }
    }
    Ok(())
}

fn parse_long_string_value_labels(
    data: &[u8],
    header: &Header,
    metadata: &mut Metadata,
) -> Result<()> {
    let mut pos = 0usize;
    let mut label_set_index = metadata.value_labels.len();
    while pos < data.len() {
        let (var_name, next) = read_pascal_string(data, pos, header.endian)?;
        pos = next;
        if pos >= data.len() {
            break;
        }
        if pos + 8 > data.len() {
            return Err(Error::ParseError(
                "invalid long string value label header".to_string(),
            ));
        }
        let string_len = read_u32_from(data, pos, header.endian)? as usize;
        pos += 4;
        let label_count = read_u32_from(data, pos, header.endian)? as usize;
        pos += 4;
        let mut mapping = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            let value_len = read_u32_from(data, pos, header.endian)? as usize;
            pos += 4;
            if pos + value_len > data.len() {
                return Err(Error::ParseError(
                    "invalid long string value label value".to_string(),
                ));
            }
            let value = decode_string(
                &data[pos..pos + value_len],
                header.endian,
                metadata.encoding,
            );
            pos += value_len;
            let label_len = read_u32_from(data, pos, header.endian)? as usize;
            pos += 4;
            if pos + label_len > data.len() {
                return Err(Error::ParseError(
                    "invalid long string value label".to_string(),
                ));
            }
            let label = decode_string(
                &data[pos..pos + label_len],
                header.endian,
                metadata.encoding,
            );
            pos += label_len;
            if !label.is_empty() {
                mapping.push((crate::spss::types::ValueLabelKey::Str(value), label));
            }
        }
        let name = format!("labels{}", label_set_index);
        label_set_index += 1;
        metadata.value_labels.push(crate::spss::types::ValueLabel {
            name: name.clone(),
            mapping,
        });
        if let Some(var) = metadata.variables.iter_mut().find(|v| {
            v.name.eq_ignore_ascii_case(&var_name) || v.short_name.eq_ignore_ascii_case(&var_name)
        }) {
            if string_len > 0 && var.string_len < string_len {
                var.string_len = string_len;
            }
            var.value_label = Some(name);
        }
    }
    Ok(())
}

fn parse_long_string_missing_values(
    data: &[u8],
    header: &Header,
    metadata: &mut Metadata,
) -> Result<()> {
    let mut pos = 0usize;
    while pos < data.len() {
        let (name, next) = read_pascal_string(data, pos, header.endian)?;
        pos = next;
        if pos >= data.len() {
            return Err(Error::ParseError(
                "unexpected end in long string missing values".to_string(),
            ));
        }
        let n_missing = data[pos] as usize;
        pos += 1;
        if n_missing == 0 || n_missing > 3 {
            return Err(Error::ParseError(
                "invalid long string missing count".to_string(),
            ));
        }
        if pos + 4 > data.len() {
            return Err(Error::ParseError(
                "invalid long string missing value length".to_string(),
            ));
        }
        let len = read_u32_from(data, pos, header.endian)? as usize;
        pos += 4;
        let mut values = Vec::with_capacity(n_missing);
        for _ in 0..n_missing {
            if pos + len > data.len() {
                return Err(Error::ParseError(
                    "invalid long string missing value".to_string(),
                ));
            }
            let s = decode_string(&data[pos..pos + len], header.endian, metadata.encoding);
            values.push(s);
            pos += len;
        }
        if let Some(var) = metadata.variables.iter_mut().find(|v| v.name == name) {
            var.missing_strings = values;
        }
    }
    Ok(())
}

fn read_pascal_string(data: &[u8], pos: usize, endian: Endian) -> Result<(String, usize)> {
    if pos + 4 > data.len() {
        return Err(Error::ParseError("invalid pascal string".to_string()));
    }
    let len = read_u32_from(data, pos, endian)? as usize;
    let start = pos + 4;
    let end = start + len;
    if end > data.len() {
        return Err(Error::ParseError(
            "invalid pascal string length".to_string(),
        ));
    }
    let s = String::from_utf8_lossy(&data[start..end]).to_string();
    Ok((s, end))
}

fn read_u32_from(data: &[u8], pos: usize, endian: Endian) -> Result<u32> {
    let bytes: [u8; 4] = data[pos..pos + 4].try_into().expect("u32 slice");
    Ok(match endian {
        Endian::Little => u32::from_le_bytes(bytes),
        Endian::Big => u32::from_be_bytes(bytes),
    })
}

fn read_i32_from(data: &[u8], pos: usize, endian: Endian) -> Result<i32> {
    let bytes: [u8; 4] = data[pos..pos + 4].try_into().expect("i32 slice");
    Ok(match endian {
        Endian::Little => i32::from_le_bytes(bytes),
        Endian::Big => i32::from_be_bytes(bytes),
    })
}
