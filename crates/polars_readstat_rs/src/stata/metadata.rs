use crate::stata::encoding;
use crate::stata::error::{Error, Result};
use crate::stata::types::{
    Endian, Header, Metadata, NumericType, ValueLabel, ValueLabelKey, VarType, Variable,
};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::{Read, Seek, SeekFrom};

pub fn read_metadata<R: Read + Seek>(reader: &mut R, header: &Header) -> Result<Metadata> {
    let layout = layout_for_version(header.version)?;

    let mut metadata = Metadata::default();
    metadata.row_count = header.nobs;
    metadata.byte_order = header.endian;
    metadata.encoding = encoding::default_encoding(header.version);

    read_label_and_timestamp(reader, header.endian, &layout, &mut metadata)?;

    if layout.file_is_xmlish {
        read_tag(reader, b"</header>")?;
        let (data_offset, strls_offset, value_labels_offset) = read_map(reader, header.endian)?;
        metadata.data_offset = Some(data_offset);
        metadata.strls_offset = Some(strls_offset);
        metadata.value_labels_offset = Some(value_labels_offset);
    }

    let typlist = read_typlist(reader, header.endian, header.nvars as usize, &layout)?;
    let varnames = read_string_table(
        reader,
        header.nvars as usize,
        layout.variable_name_len,
        layout.file_is_xmlish,
        b"<varnames>",
        b"</varnames>",
        metadata.encoding,
    )?;
    let sort_order = read_sortlist(
        reader,
        header.endian,
        header.nvars as usize,
        layout.file_is_xmlish,
        layout.srtlist_entry_len,
    )?;
    let formats = read_string_table(
        reader,
        header.nvars as usize,
        layout.fmtlist_entry_len,
        layout.file_is_xmlish,
        b"<formats>",
        b"</formats>",
        metadata.encoding,
    )?;
    let value_label_names = read_string_table(
        reader,
        header.nvars as usize,
        layout.lbllist_entry_len,
        layout.file_is_xmlish,
        b"<value_label_names>",
        b"</value_label_names>",
        metadata.encoding,
    )?;
    let variable_labels = read_string_table(
        reader,
        header.nvars as usize,
        layout.variable_labels_entry_len,
        layout.file_is_xmlish,
        b"<variable_labels>",
        b"</variable_labels>",
        metadata.encoding,
    )?;

    metadata.sort_order = sort_order;
    metadata.formats = formats.clone();
    metadata.variable_labels = variable_labels.clone();

    let mut variables = Vec::with_capacity(header.nvars as usize);
    let mut storage_widths = Vec::with_capacity(header.nvars as usize);

    for i in 0..header.nvars as usize {
        let (var_type, storage_width) = typecode_to_vartype(typlist[i], &layout)?;
        storage_widths.push(storage_width);
        let name = varnames.get(i).cloned().unwrap_or_default();
        let format = formats.get(i).cloned().filter(|s| !s.is_empty());
        let label = variable_labels.get(i).cloned().filter(|s| !s.is_empty());
        let value_label_name = value_label_names.get(i).cloned().filter(|s| !s.is_empty());
        variables.push(Variable {
            name,
            var_type,
            format,
            label,
            value_label_name,
        });
    }

    metadata.variables = variables;
    metadata.storage_widths = storage_widths;
    if !layout.file_is_xmlish {
        let data_offset = skip_expansion_fields(reader, header.endian, &layout)? as u64;
        metadata.data_offset = Some(data_offset);
    }

    if metadata.value_labels_offset.is_none() {
        if let Some(data_offset) = metadata.data_offset {
            let record_len: u64 = metadata.storage_widths.iter().map(|v| *v as u64).sum();
            let data_bytes = record_len
                .checked_mul(metadata.row_count)
                .ok_or_else(|| Error::ParseError("data size overflow".to_string()))?;
            metadata.value_labels_offset = Some(data_offset + data_bytes);
        }
    }

    metadata.value_labels = read_value_labels(reader, header, &layout, &metadata)?;

    Ok(metadata)
}

#[derive(Debug, Clone)]
struct Layout {
    file_is_xmlish: bool,
    typlist_version: u16,
    typlist_entry_len: usize,
    variable_name_len: usize,
    fmtlist_entry_len: usize,
    lbllist_entry_len: usize,
    variable_labels_entry_len: usize,
    data_label_len: usize,
    data_label_len_len: usize,
    timestamp_len: usize,
    srtlist_entry_len: usize,
    expansion_len_len: usize,
    value_label_table_len_len: usize,
    value_label_table_labname_len: usize,
    value_label_table_padding_len: usize,
}

fn layout_for_version(ds_format: u16) -> Result<Layout> {
    if !(102..=119).contains(&ds_format) {
        return Err(Error::UnsupportedFormat(format!(
            "unsupported Stata version: {ds_format}"
        )));
    }

    let fmtlist_entry_len = if ds_format < 105 {
        7
    } else if ds_format < 114 {
        12
    } else if ds_format < 118 {
        49
    } else {
        57
    };

    let typlist_version = if ds_format >= 117 {
        117
    } else if ds_format >= 111 {
        111
    } else {
        0
    };

    let (data_label_len_len, timestamp_len) = if ds_format >= 118 {
        (2, 18)
    } else if ds_format >= 117 {
        (1, 18)
    } else {
        (0, if ds_format < 105 { 0 } else { 18 })
    };

    let (lbllist_entry_len, variable_name_len) = if ds_format < 110 {
        (9, 9)
    } else if ds_format < 118 {
        (33, 33)
    } else {
        (129, 129)
    };

    let (variable_labels_entry_len, data_label_len) = if ds_format < 108 {
        (32, 32)
    } else if ds_format < 118 {
        (81, 81)
    } else {
        (321, 321)
    };

    let typlist_entry_len = if ds_format < 117 { 1 } else { 2 };
    let srtlist_entry_len = if ds_format < 119 { 2 } else { 4 };
    let expansion_len_len = if ds_format < 105 {
        0
    } else if ds_format < 110 {
        2
    } else {
        4
    };
    let (value_label_table_len_len, value_label_table_labname_len, value_label_table_padding_len) =
        if ds_format < 105 {
            (2, 12, 2)
        } else {
            let labname_len = if ds_format < 118 { 33 } else { 129 };
            (4, labname_len, 3)
        };

    Ok(Layout {
        file_is_xmlish: ds_format >= 117,
        typlist_version,
        typlist_entry_len,
        variable_name_len,
        fmtlist_entry_len,
        lbllist_entry_len,
        variable_labels_entry_len,
        data_label_len,
        data_label_len_len,
        timestamp_len,
        srtlist_entry_len,
        expansion_len_len,
        value_label_table_len_len,
        value_label_table_labname_len,
        value_label_table_padding_len,
    })
}

fn read_label_and_timestamp<R: Read>(
    reader: &mut R,
    endian: Endian,
    layout: &Layout,
    metadata: &mut Metadata,
) -> Result<()> {
    let label_len = if layout.file_is_xmlish {
        read_tag(reader, b"<label>")?;
        if layout.data_label_len_len == 2 {
            read_u16_endian(reader, endian)? as usize
        } else if layout.data_label_len_len == 1 {
            reader.read_u8()? as usize
        } else {
            layout.data_label_len
        }
    } else {
        layout.data_label_len
    };

    if label_len > 0 {
        let mut buf = vec![0u8; label_len];
        reader.read_exact(&mut buf)?;
        let label = read_string(&buf, metadata.encoding);
        if !label.is_empty() {
            metadata.data_label = Some(label);
        }
    }

    if layout.file_is_xmlish {
        read_tag(reader, b"</label>")?;
        read_tag(reader, b"<timestamp>")?;
    }

    let ts_len = if layout.file_is_xmlish {
        reader.read_u8()? as usize
    } else {
        layout.timestamp_len
    };

    if ts_len > 0 {
        let mut buf = vec![0u8; ts_len];
        reader.read_exact(&mut buf)?;
        let ts = read_string(&buf, metadata.encoding);
        if !ts.is_empty() {
            metadata.timestamp = Some(ts);
        }
    }

    if layout.file_is_xmlish {
        read_tag(reader, b"</timestamp>")?;
    }

    Ok(())
}

fn read_map<R: Read>(reader: &mut R, endian: Endian) -> Result<(u64, u64, u64)> {
    read_tag(reader, b"<map>")?;
    let mut map = [0u64; 14];
    for i in 0..14 {
        map[i] = read_u64_endian(reader, endian)?;
    }
    read_tag(reader, b"</map>")?;
    Ok((map[9], map[10], map[11]))
}

fn read_typlist<R: Read>(
    reader: &mut R,
    endian: Endian,
    nvar: usize,
    layout: &Layout,
) -> Result<Vec<u16>> {
    let mut buf = vec![0u8; nvar * layout.typlist_entry_len];
    read_chunk(
        reader,
        layout.file_is_xmlish,
        b"<variable_types>",
        &mut buf,
        b"</variable_types>",
    )?;
    let mut typlist = vec![0u16; nvar];
    if layout.typlist_entry_len == 1 {
        for i in 0..nvar {
            typlist[i] = buf[i] as u16;
        }
    } else {
        let mut cursor = std::io::Cursor::new(&buf);
        for i in 0..nvar {
            typlist[i] = read_u16_endian(&mut cursor, endian)?;
        }
    }
    Ok(typlist)
}

fn read_string_table<R: Read>(
    reader: &mut R,
    nvar: usize,
    entry_len: usize,
    xmlish: bool,
    start_tag: &[u8],
    end_tag: &[u8],
    encoding: &'static encoding_rs::Encoding,
) -> Result<Vec<String>> {
    let mut buf = vec![0u8; nvar * entry_len];
    read_chunk(reader, xmlish, start_tag, &mut buf, end_tag)?;
    let mut out = Vec::with_capacity(nvar);
    for i in 0..nvar {
        let start = i * entry_len;
        let end = start + entry_len;
        out.push(read_string(&buf[start..end], encoding));
    }
    Ok(out)
}

fn read_sortlist<R: Read>(
    reader: &mut R,
    endian: Endian,
    nvar: usize,
    xmlish: bool,
    entry_len: usize,
) -> Result<Vec<u32>> {
    let count = nvar + 1;
    let size = count * entry_len;
    let mut buf = vec![0u8; size];
    read_chunk(reader, xmlish, b"<sortlist>", &mut buf, b"</sortlist>")?;

    let mut out = Vec::with_capacity(count);
    let mut cursor = std::io::Cursor::new(&buf);
    if entry_len == 2 {
        for _ in 0..count {
            out.push(read_u16_endian(&mut cursor, endian)? as u32);
        }
    } else if entry_len == 4 {
        for _ in 0..count {
            out.push(read_u32_endian(&mut cursor, endian)?);
        }
    } else {
        return Err(Error::ParseError(format!(
            "invalid sortlist entry length: {entry_len}"
        )));
    }
    Ok(out)
}

fn typecode_to_vartype(typecode: u16, layout: &Layout) -> Result<(VarType, u16)> {
    match layout.typlist_version {
        111 => match typecode {
            0xFB => Ok((VarType::Numeric(NumericType::Byte), 1)),
            0xFC => Ok((VarType::Numeric(NumericType::Int), 2)),
            0xFD => Ok((VarType::Numeric(NumericType::Long), 4)),
            0xFE => Ok((VarType::Numeric(NumericType::Float), 4)),
            0xFF => Ok((VarType::Numeric(NumericType::Double), 8)),
            _ => Ok((VarType::Str(typecode), typecode)),
        },
        117 => match typecode {
            0xFFFA => Ok((VarType::Numeric(NumericType::Byte), 1)),
            0xFFF9 => Ok((VarType::Numeric(NumericType::Int), 2)),
            0xFFF8 => Ok((VarType::Numeric(NumericType::Long), 4)),
            0xFFF7 => Ok((VarType::Numeric(NumericType::Float), 4)),
            0xFFF6 => Ok((VarType::Numeric(NumericType::Double), 8)),
            0x8000 => Ok((VarType::StrL, 8)),
            _ => Ok((VarType::Str(typecode), typecode)),
        },
        _ => {
            if typecode < 0x7F {
                let var = match typecode as u8 {
                    b'b' => VarType::Numeric(NumericType::Byte),
                    b'i' => VarType::Numeric(NumericType::Int),
                    b'l' => VarType::Numeric(NumericType::Long),
                    b'f' => VarType::Numeric(NumericType::Float),
                    b'd' => VarType::Numeric(NumericType::Double),
                    _ => return Err(Error::InvalidTypeCode(typecode)),
                };
                let width = match var {
                    VarType::Numeric(NumericType::Byte) => 1,
                    VarType::Numeric(NumericType::Int) => 2,
                    VarType::Numeric(NumericType::Long) => 4,
                    VarType::Numeric(NumericType::Float) => 4,
                    VarType::Numeric(NumericType::Double) => 8,
                    _ => 0,
                };
                Ok((var, width))
            } else {
                let len = typecode - 0x7F;
                Ok((VarType::Str(len), len))
            }
        }
    }
}

fn read_string(bytes: &[u8], encoding: &'static encoding_rs::Encoding) -> String {
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    encoding::decode_string(&bytes[..len], encoding)
}

fn read_chunk<R: Read>(
    reader: &mut R,
    xmlish: bool,
    start: &[u8],
    dst: &mut [u8],
    end: &[u8],
) -> Result<()> {
    if xmlish {
        read_tag(reader, start)?;
    }
    reader.read_exact(dst)?;
    if xmlish {
        read_tag(reader, end)?;
    }
    Ok(())
}

fn read_tag<R: Read>(reader: &mut R, tag: &[u8]) -> Result<()> {
    let mut buf = vec![0u8; tag.len()];
    reader.read_exact(&mut buf)?;
    if buf != tag {
        return Err(Error::ParseError(format!(
            "expected tag {:?}, got {:?}",
            String::from_utf8_lossy(tag),
            String::from_utf8_lossy(&buf)
        )));
    }
    Ok(())
}

fn read_u16_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u16> {
    Ok(match endian {
        Endian::Little => reader.read_u16::<LittleEndian>()?,
        Endian::Big => reader.read_u16::<BigEndian>()?,
    })
}

fn read_u32_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u32> {
    Ok(match endian {
        Endian::Little => reader.read_u32::<LittleEndian>()?,
        Endian::Big => reader.read_u32::<BigEndian>()?,
    })
}

fn read_u64_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u64> {
    Ok(match endian {
        Endian::Little => reader.read_u64::<LittleEndian>()?,
        Endian::Big => reader.read_u64::<BigEndian>()?,
    })
}

fn read_value_labels<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    layout: &Layout,
    metadata: &Metadata,
) -> Result<Vec<ValueLabel>> {
    let Some(offset) = metadata.value_labels_offset else {
        return Ok(Vec::new());
    };

    reader.seek(SeekFrom::Start(offset))?;
    if layout.file_is_xmlish {
        read_tag(reader, b"<value_labels>")?;
    }

    let mut labels = Vec::new();
    let rules = crate::stata::value::missing_rules(header.version);

    loop {
        let len = if layout.value_label_table_len_len == 2 {
            let mut buf = [0u8; 2];
            if reader.read(&mut buf)? < 2 {
                break;
            }
            let mut cursor = std::io::Cursor::new(&buf);
            read_u16_endian(&mut cursor, header.endian)? as usize
        } else {
            if layout.file_is_xmlish {
                if let Err(_) = read_tag(reader, b"<lbl>") {
                    break;
                }
            }
            let mut buf = [0u8; 4];
            if reader.read(&mut buf)? < 4 {
                break;
            }
            let mut cursor = std::io::Cursor::new(&buf);
            read_u32_endian(&mut cursor, header.endian)? as usize
        };

        let mut labname_buf = vec![0u8; layout.value_label_table_labname_len];
        if reader.read(&mut labname_buf)? < labname_buf.len() {
            break;
        }
        let labname = read_string(&labname_buf, metadata.encoding);

        reader.seek(SeekFrom::Current(
            layout.value_label_table_padding_len as i64,
        ))?;

        let mut table = vec![0u8; len];
        if reader.read(&mut table)? < table.len() {
            break;
        }

        if layout.value_label_table_len_len == 2 {
            let n = len / 8;
            let mut mapping = Vec::with_capacity(n);
            for i in 0..n {
                let start = 8 * i;
                let end = start + 8;
                let label = read_string(&table[start..end], metadata.encoding);
                if !label.is_empty() {
                    mapping.push((ValueLabelKey::Integer(i as i32), label));
                }
            }
            labels.push(ValueLabel {
                name: labname,
                mapping: std::sync::Arc::new(mapping),
            });
        } else if len >= 8 {
            if layout.file_is_xmlish {
                read_tag(reader, b"</lbl>")?;
            }

            let mut cursor = std::io::Cursor::new(&table);
            let n = read_u32_endian(&mut cursor, header.endian)? as usize;
            let txtlen = read_u32_endian(&mut cursor, header.endian)? as usize;

            if txtlen > len.saturating_sub(8) || n > (len.saturating_sub(8 + txtlen) / 8) {
                break;
            }

            let mut off = vec![0u32; n];
            for i in 0..n {
                off[i] = read_u32_endian(&mut cursor, header.endian)?;
            }

            let mut vals = vec![0u8; n * 4];
            cursor.read_exact(&mut vals)?;

            let txt_start = 8 + (n * 8);
            let txt = &table[txt_start..txt_start + txtlen];

            let mut mapping = Vec::with_capacity(n);
            for i in 0..n {
                let offset = off[i] as usize;
                if offset >= txtlen {
                    continue;
                }
                let label = read_string(&txt[offset..], metadata.encoding);
                if label.is_empty() {
                    continue;
                }
                let val_bytes = &vals[i * 4..i * 4 + 4];
                if let Some(v) = crate::stata::value::read_i32(val_bytes, header.endian, rules) {
                    mapping.push((ValueLabelKey::Integer(v), label.clone()));
                    mapping.push((ValueLabelKey::Double(v as f64), label));
                }
            }
            labels.push(ValueLabel {
                name: labname,
                mapping: std::sync::Arc::new(mapping),
            });
        } else if layout.file_is_xmlish {
            read_tag(reader, b"</lbl>")?;
        }
    }

    Ok(labels)
}

fn skip_expansion_fields<R: Read + Seek>(
    reader: &mut R,
    endian: Endian,
    layout: &Layout,
) -> Result<u64> {
    if layout.expansion_len_len == 0 {
        return Ok(reader.stream_position()?);
    }

    loop {
        let mut data_type = [0u8; 1];
        reader.read_exact(&mut data_type)?;

        let len = if layout.expansion_len_len == 2 {
            read_u16_endian(reader, endian)? as usize
        } else {
            read_u32_endian(reader, endian)? as usize
        };

        if data_type[0] == 0 && len == 0 {
            break;
        }

        if data_type[0] != 1 || len > (1 << 20) {
            return Err(Error::ParseError("invalid expansion field".to_string()));
        }

        reader.seek(SeekFrom::Current(len as i64))?;
    }

    Ok(reader.stream_position()?)
}
