use crate::spss::error::{Error, Result};
use crate::spss::types::{Endian, Header};
use std::io::Read;

const HEADER_LEN: usize = 176;

pub fn read_header<R: Read>(reader: &mut R) -> Result<Header> {
    let mut buf = [0u8; HEADER_LEN];
    reader.read_exact(&mut buf)?;

    let rec_type = &buf[0..4];
    if rec_type != b"$FL2" && rec_type != b"$FL3" {
        return Err(Error::ParseError("invalid SPSS header".to_string()));
    }

    let layout_little = read_i32(&buf[64..68], Endian::Little);
    let layout_big = read_i32(&buf[64..68], Endian::Big);

    let endian = if matches!(layout_little, 2 | 3) {
        Endian::Little
    } else if matches!(layout_big, 2 | 3) {
        Endian::Big
    } else {
        return Err(Error::ParseError("unknown SPSS layout code".to_string()));
    };

    let nominal_case_size = read_i32(&buf[68..72], endian);
    let compression = read_i32(&buf[72..76], endian);
    let row_count = read_i32(&buf[80..84], endian);
    let bias = read_f64(&buf[84..92], endian);

    // SAV header layout: creation_date[9] at 92, creation_time[8] at 101, file_label[64] at 109
    let file_label = read_string(&buf[109..173]);
    let data_label = if file_label.is_empty() {
        None
    } else {
        Some(file_label)
    };

    let version = if rec_type == b"$FL2" { 2 } else { 3 };

    Ok(Header {
        version,
        endian,
        compression,
        nominal_case_size,
        row_count,
        bias,
        data_label,
    })
}

fn read_i32(buf: &[u8], endian: Endian) -> i32 {
    let bytes: [u8; 4] = buf.try_into().expect("i32 slice");
    match endian {
        Endian::Little => i32::from_le_bytes(bytes),
        Endian::Big => i32::from_be_bytes(bytes),
    }
}

fn read_f64(buf: &[u8], endian: Endian) -> f64 {
    let bytes: [u8; 8] = buf.try_into().expect("f64 slice");
    match endian {
        Endian::Little => f64::from_le_bytes(bytes),
        Endian::Big => f64::from_be_bytes(bytes),
    }
}

fn read_string(buf: &[u8]) -> String {
    let trimmed = buf
        .iter()
        .take_while(|&&b| b != 0)
        .cloned()
        .collect::<Vec<u8>>();
    let s = String::from_utf8_lossy(&trimmed).trim().to_string();
    s.trim_end_matches('\u{0}').to_string()
}
