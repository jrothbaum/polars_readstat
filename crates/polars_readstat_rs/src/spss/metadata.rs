use crate::metadata_df::MetadataAccumulator;
use crate::spss::error::{Error, Result};
use crate::spss::types::{
    ColumnPlan, Endian, FormatClass, Header, Metadata, VarType,
};
use std::io::{Read, Seek, SeekFrom};

/// Discard `n` bytes by reading them into a stack buffer.
/// Uses `read_exact` which is zero-syscall for a buffered reader,
/// unlike `seek(SeekFrom::Current(n))` which calls `stream_position()` for every seek.
#[inline]
fn drain<R: Read>(reader: &mut R, n: usize) -> std::io::Result<()> {
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
const SUBTYPE_VAR_DISPLAY: u32 = 11;
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
                let count = read_u32(reader, header.endian).ok()? as usize;
                for _ in 0..count {
                    drain(reader, 8).ok()?;
                    let vlen = read_u8(reader).ok()? as usize;
                    let padded = ((vlen + 8) / 8) * 8 - 1;
                    drain(reader, padded).ok()?;
                }
                let _rec = read_u32(reader, header.endian).ok()?;
                let var_count = read_u32(reader, header.endian).ok()? as usize;
                drain(reader, var_count * 4).ok()?;
            }
            REC_TYPE_VALUE_LABEL_VARIABLES => {
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
    let capacity = header.nominal_case_size.max(0) as usize;
    let encoding = prescan_char_encoding(reader, header)
        .unwrap_or(encoding_rs::WINDOWS_1252);

    let mut variables: Vec<ColumnPlan> = Vec::with_capacity(capacity);
    let mut acc = MetadataAccumulator::with_capacity(capacity);

    let mut row_count = header.row_count.max(0) as u64;
    let mut data_offset: Option<u64> = None;
    let mut value_labels_out: Vec<crate::spss::types::ValueLabel> = Vec::new();

    let mut last_var_index: Option<usize> = None;
    let mut label_set_index = 0usize;
    let mut current_offset = 0usize;
    let mut offset_to_idx: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();

    loop {
        let rec_type = read_u32(reader, header.endian)?;
        match rec_type {
            REC_TYPE_VARIABLE => {
                if let Some(plan) = read_variable_record(
                    reader,
                    header,
                    encoding,
                    last_var_index,
                    &mut variables,
                    &mut acc,
                    &mut current_offset,
                )? {
                    offset_to_idx.insert(plan.offset, variables.len());
                    last_var_index = Some(variables.len());
                    variables.push(plan);
                }
            }
            REC_TYPE_VALUE_LABEL => {
                read_value_label_record(
                    reader,
                    header,
                    encoding,
                    &mut variables,
                    &mut acc,
                    &mut value_labels_out,
                    &mut label_set_index,
                    &offset_to_idx,
                )?;
            }
            REC_TYPE_VALUE_LABEL_VARIABLES => {
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
                    if let Some(new_enc) = parse_integer_info_encoding(&buf, header)? {
                        if new_enc != encoding {
                            redecode_all(&mut variables, &mut acc, &mut value_labels_out, encoding, new_enc);
                        }
                    }
                } else if subtype == SUBTYPE_VAR_DISPLAY && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_variable_display_parameters(&buf, header, count, &mut variables, &mut acc)?;
                } else if subtype == SUBTYPE_CHAR_ENCODING && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    if let Ok(codepage) = std::str::from_utf8(&buf) {
                        if let Some(new_enc) =
                            encoding_rs::Encoding::for_label(codepage.trim().as_bytes())
                        {
                            if new_enc != encoding {
                                redecode_all(&mut variables, &mut acc, &mut value_labels_out, encoding, new_enc);
                            }
                        }
                    }
                } else if subtype == SUBTYPE_VERY_LONG_STR && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_very_long_string_record(&buf, &mut variables)?;
                } else if subtype == SUBTYPE_LONG_VAR_NAME && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_long_variable_names_record(&buf, &mut variables, &mut acc)?;
                } else if subtype == SUBTYPE_LONG_STRING_VALUE_LABELS && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_long_string_value_labels(&buf, header, encoding, &mut variables, &mut acc, &mut value_labels_out, &mut label_set_index)?;
                } else if subtype == SUBTYPE_LONG_STRING_MISSING_VALUES && data_len > 0 {
                    let mut buf = vec![0u8; data_len];
                    reader.read_exact(&mut buf)?;
                    parse_long_string_missing_values(&buf, header, encoding, &mut variables)?;
                } else {
                    drain(reader, data_len)?;
                }
            }
            REC_TYPE_DICT_TERMINATION => {
                let _filler = read_u32(reader, header.endian)?;
                data_offset = Some(reader.stream_position()?);
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

    // Coalesce very-long-string continuation segments.
    coalesce_very_long_strings(&mut variables, &mut acc)?;

    // Row count from the number-of-cases extension record overrides the header value.
    // (We don't parse subtype 16 above, but keep the header-derived count as a fallback.)
    let _ = row_count; // already set from header

    let metadata_df = acc
        .into_dataframe()
        .map_err(|e| Error::ParseError(e.to_string()))?;

    Ok(Metadata {
        variables,
        metadata_df,
        row_count,
        data_offset,
        encoding,
        value_labels: value_labels_out,
    })
}

fn coalesce_very_long_strings(
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
) -> Result<()> {
    let mut i = 0usize;
    while i < variables.len() {
        let string_len = variables[i].string_len;
        let is_long = variables[i].var_type == VarType::Str && string_len > 255;
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
        if end > variables.len() {
            return Err(Error::ParseError(format!(
                "invalid very long string segment count for {}",
                variables[i].name
            )));
        }

        let total_width: usize = variables[i..end].iter().map(|v| v.width).sum();
        variables[i].width = total_width;
        variables.drain(i + 1..end);
        acc.drain(i + 1..end);
        acc.set_string_width_bytes(i, Some(string_len as i32));
        i += 1;
    }
    Ok(())
}

/// Returns `(plan, pushed_to_acc)` — `None` if this is a string-continuation record.
fn read_variable_record<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    encoding: &'static encoding_rs::Encoding,
    last_var_index: Option<usize>,
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
    current_offset: &mut usize,
) -> Result<Option<ColumnPlan>> {
    let mut buf = [0u8; 28];
    reader.read_exact(&mut buf)?;

    let typ = read_i32(&buf[0..4], header.endian);
    let has_label = read_i32(&buf[4..8], header.endian);
    let n_missing = read_i32(&buf[8..12], header.endian);
    let print_format = read_i32(&buf[12..16], header.endian);
    let write_format = read_i32(&buf[16..20], header.endian);
    let name = read_name(&buf[20..28]);

    if typ < 0 {
        let idx = last_var_index.ok_or_else(|| {
            Error::ParseError("string continuation without base variable".to_string())
        })?;
        variables[idx].width += 1;
        *current_offset += 1;
        return Ok(None);
    }

    let var_type = if typ == 0 { VarType::Numeric } else { VarType::Str };
    let string_len = if typ > 0 { typ as usize } else { 0 };
    let width = 1;
    let offset = *current_offset;
    *current_offset += width;

    let (format_type, format_width, format_decimals) = decode_format(print_format);
    let (write_format_type, write_format_width, write_format_decimals) = decode_format(write_format);
    let format_class = format_class_from_type(format_type);

    let mut label: Option<String> = None;
    if has_label != 0 {
        let len = read_u32(reader, header.endian)? as usize;
        let padded = ((len + 3) / 4) * 4;
        let mut label_buf = vec![0u8; padded];
        reader.read_exact(&mut label_buf)?;
        let raw = &label_buf[..len.min(label_buf.len())];
        let text = encoding.decode_without_bom_handling(raw).0.trim().to_string();
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
                let s = decode_string(&raw, header.endian, encoding);
                missing_strings.push(s);
            }
        }
    }

    acc.push(
        name.clone(),
        label,
        None, // format (SPSS uses format_type/width/decimals, not a string format)
        Some(format_type as i32),
        Some(format_width as i32),
        Some(format_decimals as i32),
    );
    let acc_idx = acc.len() - 1;
    acc.set_string_width_bytes(acc_idx, if string_len > 0 { Some(string_len as i32) } else { None });

    Ok(Some(ColumnPlan {
        name: name.clone(),
        short_name: name,
        var_type,
        width,
        string_len,
        format_class,
        write_format_type,
        write_format_width,
        write_format_decimals,
        value_label: None,
        offset,
        missing_range,
        missing_doubles,
        missing_double_bits,
        missing_strings,
    }))
}

fn decode_format(format: i32) -> (u8, u8, u8) {
    let format = format as u32;
    (
        ((format >> 16) & 0xff) as u8,
        ((format >> 8) & 0xff) as u8,
        (format & 0xff) as u8,
    )
}

fn parse_variable_display_parameters(
    data: &[u8],
    header: &Header,
    count: usize,
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
) -> Result<()> {
    if count == 0 || data.len() < count * 4 {
        return Ok(());
    }

    let var_count = variables.len();
    if var_count == 0 {
        return Ok(());
    }

    let total_segments: usize = variables.iter().map(|v| v.width.max(1)).sum();
    let (params_per_var, segment_based) = if count == var_count * 3 {
        (3, false)
    } else if count == var_count * 2 {
        (2, false)
    } else if count == total_segments * 3 {
        (3, true)
    } else if count == total_segments * 2 {
        (2, true)
    } else {
        return Ok(());
    };

    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        values.push(read_i32_from(data, i * 4, header.endian)?);
    }

    let mut pos = 0usize;
    for (i, var) in variables.iter().enumerate() {
        if pos + params_per_var > values.len() {
            break;
        }
        acc.set_measure(i, measure_str_from_i32(values[pos]));
        if params_per_var == 3 {
            acc.set_display_width(i, Some(values[pos + 1]));
            acc.set_alignment(i, alignment_str_from_i32(values[pos + 2]));
        } else {
            acc.set_alignment(i, alignment_str_from_i32(values[pos + 1]));
        }
        pos += params_per_var * if segment_based { var.width.max(1) } else { 1 };
    }

    Ok(())
}

fn measure_str_from_i32(value: i32) -> Option<&'static str> {
    match value {
        0 => Some("Unknown"),
        1 => Some("Nominal"),
        2 => Some("Ordinal"),
        3 => Some("Scale"),
        _ => None,
    }
}

fn alignment_str_from_i32(value: i32) -> Option<&'static str> {
    match value {
        0 => Some("Left"),
        1 => Some("Right"),
        2 => Some("Center"),
        _ => None,
    }
}

fn format_class_from_type(code: u8) -> Option<FormatClass> {
    match code {
        20 | 23 | 24 | 38 | 39 => Some(FormatClass::Date),
        21 | 25 => Some(FormatClass::Time),
        22 | 41 => Some(FormatClass::DateTime),
        _ => None,
    }
}

fn read_value_label_record<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    encoding: &'static encoding_rs::Encoding,
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
    value_labels_out: &mut Vec<crate::spss::types::ValueLabel>,
    label_set_index: &mut usize,
    offset_to_idx: &std::collections::HashMap<usize, usize>,
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
        let label = decode_string(&label_buf, header.endian, encoding);
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

    let mut is_string = false;
    for off in &var_offsets {
        let target = off.saturating_sub(1) as usize;
        if let Some(&idx) = offset_to_idx.get(&target) {
            if variables[idx].var_type == VarType::Str {
                is_string = true;
                break;
            }
        }
    }

    let mut mapping = Vec::with_capacity(entry_count);
    let mut codes = Vec::with_capacity(entry_count);
    let mut code_labels = Vec::with_capacity(entry_count);

    for (raw, label) in raw_values.into_iter().zip(labels.into_iter()) {
        if label.is_empty() {
            continue;
        }
        if is_string {
            let s = decode_string(&raw, header.endian, encoding);
            codes.push(s.clone());
            code_labels.push(label.clone());
            mapping.push((crate::spss::types::ValueLabelKey::Str(s), label));
        } else {
            let v = read_f64(&raw, header.endian);
            codes.push(v.to_string());
            code_labels.push(label.clone());
            mapping.push((crate::spss::types::ValueLabelKey::Double(v), label));
        }
    }

    acc.add_value_label_group(name.clone(), codes, code_labels);
    value_labels_out.push(crate::spss::types::ValueLabel { name: name.clone(), mapping });

    for off in var_offsets {
        let target = off.saturating_sub(1) as usize;
        if let Some(&idx) = offset_to_idx.get(&target) {
            variables[idx].value_label = Some(name.clone());
            acc.set_value_label_name(idx, name.clone());
        }
    }
    Ok(())
}

fn parse_integer_info_encoding(
    data: &[u8],
    header: &Header,
) -> Result<Option<&'static encoding_rs::Encoding>> {
    if data.len() < 32 {
        return Ok(None);
    }
    let character_code = read_i32_from(data, 28, header.endian)?;
    if character_code > 0 {
        return Ok(encoding_for_code(character_code));
    }
    Ok(None)
}

fn redecode_all(
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
    value_labels: &mut Vec<crate::spss::types::ValueLabel>,
    from: &'static encoding_rs::Encoding,
    to: &'static encoding_rs::Encoding,
) {
    for var in variables.iter_mut() {
        var.name = redecode_string(&var.name, from, to);
        var.short_name = redecode_string(&var.short_name, from, to);
        if let Some(vl) = var.value_label.as_mut() {
            *vl = redecode_string(vl, from, to);
        }
        var.missing_strings = var
            .missing_strings
            .iter()
            .map(|s| redecode_string(s, from, to))
            .collect();
    }
    acc.redecode_strings(from, to);
    for vl in value_labels.iter_mut() {
        vl.name = redecode_string(&vl.name, from, to);
        for (key, label) in vl.mapping.iter_mut() {
            if let crate::spss::types::ValueLabelKey::Str(s) = key {
                *s = redecode_string(s, from, to);
            }
            *label = redecode_string(label, from, to);
        }
    }
}

fn parse_very_long_string_record(data: &[u8], variables: &mut Vec<ColumnPlan>) -> Result<()> {
    let mut key_to_idx: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (i, v) in variables.iter().enumerate() {
        key_to_idx
            .entry(v.short_name.to_ascii_lowercase())
            .or_insert(i);
        key_to_idx
            .entry(v.name.to_ascii_lowercase())
            .or_insert(i);
    }
    let mut pos = 0usize;
    while pos < data.len() {
        let end = data[pos..]
            .iter()
            .position(|&b| b == b'\t')
            .map(|i| pos + i)
            .unwrap_or(data.len());
        let entry: Vec<u8> = data[pos..end]
            .iter()
            .copied()
            .filter(|b| *b != 0)
            .collect();
        pos = if end < data.len() { end + 1 } else { end };
        if entry.is_empty() {
            continue;
        }
        if let Some(eq) = entry.iter().position(|&b| b == b'=') {
            let key = String::from_utf8_lossy(&entry[..eq]).to_ascii_lowercase();
            let val = String::from_utf8_lossy(&entry[eq + 1..]).trim().to_string();
            if let Ok(len) = val.parse::<usize>() {
                if let Some(&idx) = key_to_idx.get(&key) {
                    variables[idx].string_len = len;
                }
            }
        }
    }
    Ok(())
}

fn parse_long_variable_names_record(
    data: &[u8],
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
) -> Result<()> {
    let name_to_idx: std::collections::HashMap<String, usize> = variables
        .iter()
        .enumerate()
        .map(|(i, v)| (v.name.to_ascii_lowercase(), i))
        .collect();
    let mut pos = 0usize;
    while pos < data.len() {
        let end = data[pos..]
            .iter()
            .position(|&b| b == b'\t')
            .map(|i| pos + i)
            .unwrap_or(data.len());
        let entry: Vec<u8> = data[pos..end]
            .iter()
            .copied()
            .filter(|b| *b != 0)
            .collect();
        pos = if end < data.len() { end + 1 } else { end };
        if entry.is_empty() {
            continue;
        }
        if let Some(eq) = entry.iter().position(|&b| b == b'=') {
            let key = String::from_utf8_lossy(&entry[..eq])
                .trim()
                .to_ascii_lowercase();
            let val = String::from_utf8_lossy(&entry[eq + 1..])
                .trim()
                .to_string();
            if key.is_empty() || val.is_empty() {
                continue;
            }
            if let Some(&idx) = name_to_idx.get(&key) {
                variables[idx].name = val.clone();
                acc.rename(idx, val);
            }
        }
    }
    Ok(())
}

fn parse_long_string_value_labels(
    data: &[u8],
    header: &Header,
    encoding: &'static encoding_rs::Encoding,
    variables: &mut Vec<ColumnPlan>,
    acc: &mut MetadataAccumulator,
    value_labels_out: &mut Vec<crate::spss::types::ValueLabel>,
    label_set_index: &mut usize,
) -> Result<()> {
    let mut key_to_idx: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (i, v) in variables.iter().enumerate() {
        key_to_idx
            .entry(v.name.to_ascii_lowercase())
            .or_insert(i);
        key_to_idx
            .entry(v.short_name.to_ascii_lowercase())
            .or_insert(i);
    }
    let mut pos = 0usize;
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
        let mut codes = Vec::with_capacity(label_count);
        let mut code_labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            let value_len = read_u32_from(data, pos, header.endian)? as usize;
            pos += 4;
            if pos + value_len > data.len() {
                return Err(Error::ParseError(
                    "invalid long string value label value".to_string(),
                ));
            }
            let value = decode_string(&data[pos..pos + value_len], header.endian, encoding);
            pos += value_len;
            let label_len = read_u32_from(data, pos, header.endian)? as usize;
            pos += 4;
            if pos + label_len > data.len() {
                return Err(Error::ParseError("invalid long string value label".to_string()));
            }
            let label = decode_string(&data[pos..pos + label_len], header.endian, encoding);
            pos += label_len;
            if !label.is_empty() {
                codes.push(value.clone());
                code_labels.push(label.clone());
                mapping.push((crate::spss::types::ValueLabelKey::Str(value), label));
            }
        }
        let name = format!("labels{}", *label_set_index);
        *label_set_index += 1;
        acc.add_value_label_group(name.clone(), codes, code_labels);
        value_labels_out.push(crate::spss::types::ValueLabel {
            name: name.clone(),
            mapping,
        });
        if let Some(&idx) = key_to_idx.get(&var_name.to_ascii_lowercase()) {
            if string_len > 0 && variables[idx].string_len < string_len {
                variables[idx].string_len = string_len;
            }
            variables[idx].value_label = Some(name.clone());
            acc.set_value_label_name(idx, name);
        }
    }
    Ok(())
}

fn parse_long_string_missing_values(
    data: &[u8],
    header: &Header,
    encoding: &'static encoding_rs::Encoding,
    variables: &mut Vec<ColumnPlan>,
) -> Result<()> {
    let name_to_idx: std::collections::HashMap<String, usize> = variables
        .iter()
        .enumerate()
        .map(|(i, v)| (v.name.clone(), i))
        .collect();
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
            let s = decode_string(&data[pos..pos + len], header.endian, encoding);
            values.push(s);
            pos += len;
        }
        if let Some(&idx) = name_to_idx.get(name.as_str()) {
            variables[idx].missing_strings = values;
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

fn read_i32_from(data: &[u8], offset: usize, endian: Endian) -> Result<i32> {
    if offset + 4 > data.len() {
        return Err(Error::ParseError("read_i32_from out of bounds".to_string()));
    }
    Ok(read_i32(&data[offset..offset + 4], endian))
}

fn read_u32_from(data: &[u8], offset: usize, endian: Endian) -> Result<u32> {
    if offset + 4 > data.len() {
        return Err(Error::ParseError("read_u32_from out of bounds".to_string()));
    }
    let bytes: [u8; 4] = data[offset..offset + 4].try_into().expect("u32 slice");
    Ok(match endian {
        Endian::Little => u32::from_le_bytes(bytes),
        Endian::Big => u32::from_be_bytes(bytes),
    })
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
