use crate::stata::error::{Error, Result};
use crate::stata::types::{Endian, Header};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::{Read, Seek, SeekFrom};

const XMLISH_TAG: &[u8] = b"<stata_dta>";

pub fn read_header<R: Read + Seek>(reader: &mut R) -> Result<Header> {
    let mut probe = [0u8; XMLISH_TAG.len()];
    reader.read_exact(&mut probe)?;

    if probe == *XMLISH_TAG {
        read_xmlish_header(reader)
    } else {
        reader.seek(SeekFrom::Start(0))?;
        read_binary_header(reader)
    }
}

fn read_binary_header<R: Read>(reader: &mut R) -> Result<Header> {
    let ds_format = reader.read_u8()? as u16;
    let byteorder = reader.read_u8()?;
    let _filetype = reader.read_u8()?;
    let _unused = reader.read_u8()?;

    let endian = match byteorder {
        0x00 => Endian::Little, // legacy 102/103 files
        0x01 => Endian::Big,    // DTA_HILO
        0x02 => Endian::Little, // DTA_LOHI
        _ => {
            return Err(Error::UnsupportedFormat(format!(
                "invalid byteorder: {byteorder}"
            )))
        }
    };

    let nvars = read_u16_endian(reader, endian)? as u32;
    let nobs = if ds_format == 102 {
        read_u16_endian(reader, endian)? as u64
    } else {
        read_u32_endian(reader, endian)? as u64
    };

    Ok(Header {
        version: ds_format,
        endian,
        nvars,
        nobs,
        data_label: None,
        timestamp: None,
    })
}

fn read_xmlish_header<R: Read>(reader: &mut R) -> Result<Header> {
    read_tag(reader, b"<header>")?;

    let mut release = [0u8; 3];
    read_chunk(reader, b"<release>", &mut release, b"</release>")?;
    let ds_format = 100 * (release[0] - b'0') as u16
        + 10 * (release[1] - b'0') as u16
        + (release[2] - b'0') as u16;

    let mut byteorder = [0u8; 3];
    read_chunk(reader, b"<byteorder>", &mut byteorder, b"</byteorder>")?;
    let endian = if &byteorder == b"MSF" {
        Endian::Big
    } else if &byteorder == b"LSF" {
        Endian::Little
    } else {
        return Err(Error::UnsupportedFormat(
            "invalid byteorder tag".to_string(),
        ));
    };

    let nvars = if ds_format >= 119 {
        let n = read_u32_tagged(reader, endian, b"<K>", b"</K>")?;
        n
    } else {
        let n = read_u16_tagged(reader, endian, b"<K>", b"</K>")? as u32;
        n
    };

    let nobs = if ds_format >= 118 {
        let n = read_u64_tagged(reader, endian, b"<N>", b"</N>")?;
        n
    } else {
        let n = read_u32_tagged(reader, endian, b"<N>", b"</N>")? as u64;
        n
    };

    Ok(Header {
        version: ds_format,
        endian,
        nvars,
        nobs,
        data_label: None,
        timestamp: None,
    })
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

fn read_chunk<R: Read>(reader: &mut R, start: &[u8], dst: &mut [u8], end: &[u8]) -> Result<()> {
    read_tag(reader, start)?;
    reader.read_exact(dst)?;
    read_tag(reader, end)?;
    Ok(())
}

fn read_u16_tagged<R: Read>(
    reader: &mut R,
    endian: Endian,
    start: &[u8],
    end: &[u8],
) -> Result<u16> {
    read_tag(reader, start)?;
    let v = read_u16_endian(reader, endian)?;
    read_tag(reader, end)?;
    Ok(v)
}

fn read_u32_tagged<R: Read>(
    reader: &mut R,
    endian: Endian,
    start: &[u8],
    end: &[u8],
) -> Result<u32> {
    read_tag(reader, start)?;
    let v = read_u32_endian(reader, endian)?;
    read_tag(reader, end)?;
    Ok(v)
}

fn read_u64_tagged<R: Read>(
    reader: &mut R,
    endian: Endian,
    start: &[u8],
    end: &[u8],
) -> Result<u64> {
    read_tag(reader, start)?;
    let v = read_u64_endian(reader, endian)?;
    read_tag(reader, end)?;
    Ok(v)
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
