use crate::error::{Error, Result};
use crate::types::{Endian, Format};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::Cursor;

/// Buffer for reading bytes with endian-awareness
pub struct Buffer {
    data: Vec<u8>,
    endian: Endian,
}

impl Buffer {
    pub fn from_vec(data: Vec<u8>, endian: Endian) -> Self {
        Self { data, endian }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn get_bytes(&self, offset: usize, length: usize) -> Result<&[u8]> {
        self.data
            .get(offset..offset + length)
            .ok_or(Error::BufferOutOfBounds { offset, length })
    }

    pub fn get_u8(&self, offset: usize) -> Result<u8> {
        self.data
            .get(offset)
            .copied()
            .ok_or(Error::BufferOutOfBounds { offset, length: 1 })
    }

    pub fn get_u16(&self, offset: usize) -> Result<u16> {
        let slice = self.get_bytes(offset, 2)?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_u16::<BigEndian>(),
            Endian::Little => cursor.read_u16::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    pub fn get_u32(&self, offset: usize) -> Result<u32> {
        let slice = self.get_bytes(offset, 4)?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_u32::<BigEndian>(),
            Endian::Little => cursor.read_u32::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    pub fn get_u64(&self, offset: usize) -> Result<u64> {
        let slice = self.get_bytes(offset, 8)?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_u64::<BigEndian>(),
            Endian::Little => cursor.read_u64::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    pub fn get_f64(&self, offset: usize) -> Result<f64> {
        let slice = self.get_bytes(offset, 8)?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_f64::<BigEndian>(),
            Endian::Little => cursor.read_f64::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    /// Get integer value based on format (32-bit or 64-bit)
    pub fn get_integer(&self, offset: usize, format: Format) -> Result<u64> {
        match format {
            Format::Bit32 => self.get_u32(offset).map(|v| v as u64),
            Format::Bit64 => self.get_u64(offset),
        }
    }

    /// Get string from buffer, trimming trailing whitespace and nulls
    pub fn get_string(&self, offset: usize, length: usize) -> Result<String> {
        let bytes = self.get_bytes(offset, length)?;
        let trimmed = trim_sas_string(bytes);
        String::from_utf8(trimmed.to_vec())
            .map_err(|_| Error::Encoding("Invalid UTF-8".to_string()))
    }
}

/// Trim SAS string (remove leading/trailing whitespace and trailing nulls)
pub fn trim_sas_string(bytes: &[u8]) -> &[u8] {
    // Trim trailing whitespace and nulls
    let end = bytes
        .iter()
        .rposition(|&b| b != 0 && !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(0);

    // If end is 0, the entire string is whitespace/null
    if end == 0 {
        return &bytes[0..0];
    }

    // Trim leading whitespace (only search up to end)
    let start = bytes[..end]
        .iter()
        .position(|&b| !b.is_ascii_whitespace())
        .unwrap_or(0);

    &bytes[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_sas_string() {
        assert_eq!(trim_sas_string(b"hello  "), b"hello");
        assert_eq!(trim_sas_string(b"  hello  "), b"hello");
        assert_eq!(trim_sas_string(b"hello\0\0"), b"hello");
        assert_eq!(trim_sas_string(b"  hello\0\0  "), b"hello");
        assert_eq!(trim_sas_string(b""), b"");
    }

    #[test]
    fn test_buffer_reading() {
        let data = vec![0x01, 0x02, 0x03, 0x04];
        let buf = Buffer::from_vec(data, Endian::Big);

        assert_eq!(buf.get_u8(0).unwrap(), 0x01);
        assert_eq!(buf.get_u16(0).unwrap(), 0x0102);
        assert_eq!(buf.get_u32(0).unwrap(), 0x01020304);
    }
}
