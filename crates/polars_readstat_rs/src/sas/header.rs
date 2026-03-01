use crate::buffer::Buffer;
use crate::constants::*;
use crate::error::{Error, Result};
use crate::types::{Endian, Format, Header, Platform};
use chrono::{DateTime, TimeZone, Utc};
use std::io::{Read, Seek, SeekFrom};

/// Check the file header, validate magic number, and detect endian/format
pub fn check_header<R: Read + Seek>(reader: &mut R) -> Result<(Endian, Format)> {
    // Read initial header bytes
    let mut header_bytes = vec![0u8; HEADER_SIZE];
    reader.read_exact(&mut header_bytes)?;

    // Check magic number
    if &header_bytes[0..32] != MAGIC_NUMBER {
        return Err(Error::InvalidMagicNumber);
    }

    // Detect format: byte 32 == '3' means 64-bit
    let format = if header_bytes[32] == b'3' {
        Format::Bit64
    } else {
        Format::Bit32
    };

    // Detect endianness: byte 37 == 0x01 means little-endian
    let endian = if header_bytes[37] == 0x01 {
        Endian::Little
    } else {
        Endian::Big
    };

    Ok((endian, format))
}

/// Read the full header including all metadata
pub fn read_header<R: Read + Seek>(
    reader: &mut R,
    endian: Endian,
    format: Format,
) -> Result<Header> {
    // Seek back to start and read header again
    reader.seek(SeekFrom::Start(0))?;
    let mut header_bytes = vec![0u8; HEADER_SIZE];
    reader.read_exact(&mut header_bytes)?;

    let buf = Buffer::from_vec(header_bytes, endian);

    // Calculate alignment offsets
    let align1 = if buf.get_u8(35)? == b'3' { 4 } else { 0 };
    let align2 = if buf.get_u8(32)? == b'3' { 4 } else { 0 };
    let total_align = align1 + align2;

    // Read platform
    let platform = match buf.get_u8(39)? {
        b'1' => Platform::Unix,
        b'2' => Platform::Windows,
        _ => Platform::Unknown,
    };

    // Read header length and extend buffer if needed
    let header_length = buf.get_u32(196 + align1)? as usize;
    if header_length > HEADER_SIZE {
        let mut extended_bytes = vec![0u8; header_length - HEADER_SIZE];
        reader.read_exact(&mut extended_bytes)?;
        let mut full_header = buf.data().to_vec();
        full_header.extend(extended_bytes);
        let buf = Buffer::from_vec(full_header, endian);

        // Extract all header fields
        extract_header_fields(
            buf,
            format,
            endian,
            platform,
            header_length,
            align1,
            total_align,
        )
    } else {
        extract_header_fields(
            buf,
            format,
            endian,
            platform,
            header_length,
            align1,
            total_align,
        )
    }
}

fn extract_header_fields(
    buf: Buffer,
    format: Format,
    endian: Endian,
    platform: Platform,
    header_length: usize,
    align1: usize,
    total_align: usize,
) -> Result<Header> {
    // Extract dataset name and file type
    let dataset_name = buf.get_string(92, 64)?;
    let file_type = buf.get_string(156, 8)?;

    // Extract dates (SAS epoch: 1960-01-01)
    let date_created = sas_date_to_datetime(buf.get_f64(164 + align1)?);
    let date_modified = sas_date_to_datetime(buf.get_f64(172 + align1)?);

    // Extract page information
    let page_length = buf.get_u32(200 + align1)? as usize;
    let page_count = buf.get_u32(204 + align1)? as usize;

    // Extract encoding byte at offset 70
    let encoding_byte = buf.get_u8(70)?;

    // Extract SAS version and OS information
    let sas_release = buf.get_string(216 + total_align, 8)?;
    let sas_server_type = buf.get_string(224 + total_align, 16)?;
    let os_type = buf.get_string(240 + total_align, 16)?;

    // OS name can be at two different locations
    let os_name = if buf.get_u8(272 + total_align).is_ok() && buf.get_u8(272 + total_align)? != 0 {
        buf.get_string(272 + total_align, 16)?
    } else {
        buf.get_string(256 + total_align, 16)?
    };

    Ok(Header {
        format,
        endian,
        platform,
        date_created,
        date_modified,
        header_length,
        page_length,
        page_count,
        dataset_name,
        file_type,
        sas_release,
        sas_server_type,
        os_type,
        os_name,
        encoding_byte,
    })
}

/// Convert SAS datetime (seconds since 1960-01-01 00:00:00) to UTC DateTime
fn sas_date_to_datetime(sas_seconds: f64) -> DateTime<Utc> {
    if sas_seconds.is_nan() || sas_seconds == 0.0 {
        return Utc.timestamp_opt(0, 0).unwrap();
    }

    // Convert from SAS epoch (1960-01-01) to Unix epoch (1970-01-01)
    let unix_seconds = sas_seconds as i64 - (SAS_EPOCH_OFFSET_DAYS as i64 * SECONDS_PER_DAY);
    Utc.timestamp_opt(unix_seconds, 0).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_sas_date_conversion() {
        // Test that SAS epoch (0) converts correctly
        let dt = sas_date_to_datetime(0.0);
        assert_eq!(dt.year(), 1970); // Should be Unix epoch since 0 is treated as missing

        // Test a known date: 2020-01-01 00:00:00 is 1893456000 seconds from 1960-01-01
        let dt = sas_date_to_datetime(1893456000.0);
        assert_eq!(dt.year(), 2020);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }
}
