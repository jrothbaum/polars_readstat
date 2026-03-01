use crate::encoding;
use crate::error::{Error, Result};
use crate::types::{Column, ColumnType, Endian};

/// Represents a parsed value from a SAS file
#[derive(Debug, Clone)]
pub enum Value {
    Numeric(Option<f64>),
    Character(Option<String>),
}

/// Value parser for extracting column values from row bytes.
/// Retained for unit tests; the hot path uses `add_row_raw` instead.
#[allow(dead_code)]
pub struct ValueParser {
    endian: Endian,
    encoding: &'static encoding_rs::Encoding,
    encoding_byte: u8,
    missing_string_as_null: bool,
}

#[allow(dead_code)]
impl ValueParser {
    pub fn new(endian: Endian, encoding_byte: u8, missing_string_as_null: bool) -> Self {
        let encoding = encoding::get_encoding(encoding_byte);
        Self {
            endian,
            encoding,
            encoding_byte,
            missing_string_as_null,
        }
    }

    /// Parse a single column value from row bytes
    pub fn parse_value(&self, row_bytes: &[u8], column: &Column) -> Result<Value> {
        // Check bounds
        if column.offset + column.length > row_bytes.len() {
            eprintln!("DEBUG: parse_value BufferOutOfBounds - column '{}' offset={}, length={}, row_bytes.len()={}",
                      column.name, column.offset, column.length, row_bytes.len());
            return Err(Error::BufferOutOfBounds {
                offset: column.offset,
                length: column.length,
            });
        }

        let value_bytes = &row_bytes[column.offset..column.offset + column.length];

        match column.col_type {
            ColumnType::Numeric => self.parse_numeric(value_bytes),
            ColumnType::Character => self.parse_character(value_bytes),
        }
    }

    /// Parse numeric value (f64)
    /// SAS stores truncated IEEE 754 doubles (3-7 bytes) for narrow numeric columns.
    /// The stored bytes are the most significant bytes; we pad with zeros to reconstruct
    /// the full 8-byte double.
    fn parse_numeric(&self, bytes: &[u8]) -> Result<Value> {
        if bytes.is_empty() {
            return Ok(Value::Numeric(None));
        }

        let buf = if bytes.len() >= 8 {
            // SAFETY: we checked len >= 8
            <[u8; 8]>::try_from(&bytes[..8]).unwrap()
        } else {
            // Short numeric: pad to 8 bytes following C++ get_incomplete_double pattern.
            let mut buf = [0u8; 8];
            match self.endian {
                Endian::Little => {
                    let start = 8 - bytes.len();
                    buf[start..].copy_from_slice(bytes);
                }
                Endian::Big => {
                    buf[..bytes.len()].copy_from_slice(bytes);
                }
            }
            buf
        };

        let value = match self.endian {
            Endian::Little => f64::from_le_bytes(buf),
            Endian::Big => f64::from_be_bytes(buf),
        };

        if value.is_nan() || value.is_infinite() {
            Ok(Value::Numeric(None))
        } else {
            Ok(Value::Numeric(Some(value)))
        }
    }

    /// Parse character value (String)
    fn parse_character(&self, bytes: &[u8]) -> Result<Value> {
        // SAS strings are space-padded and may contain null bytes
        // Trim trailing spaces and null bytes
        let mut end = bytes.len();
        while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\x00') {
            end -= 1;
        }

        // Stop at the first NUL to match ReadStat's C-string behavior
        if let Some(pos) = bytes[..end].iter().position(|&b| b == 0) {
            end = pos;
        }

        // Check for empty or all-space/null string
        if end == 0 {
            return if self.missing_string_as_null {
                Ok(Value::Character(None))
            } else {
                Ok(Value::Character(Some(String::new())))
            };
        }

        // Decode using the file's encoding
        let s = encoding::decode_string(&bytes[..end], self.encoding_byte, self.encoding);

        Ok(Value::Character(Some(s)))
    }
}

const SAS_MISSING_MIN: u64 = 0x7ff0_0000_0000_0000;

/// Decode numeric bytes into (value, is_missing) without allocating Value.
pub fn decode_numeric_bytes_mask(endian: Endian, bytes: &[u8]) -> (f64, bool) {
    if bytes.is_empty() {
        return (0.0, true);
    }
    let bits = if bytes.len() >= 8 {
        let slice = &bytes[..8];
        match endian {
            Endian::Little => u64::from_le_bytes(slice.try_into().unwrap_or([0u8; 8])),
            Endian::Big => u64::from_be_bytes(slice.try_into().unwrap_or([0u8; 8])),
        }
    } else {
        let mut buf = [0u8; 8];
        match endian {
            Endian::Little => {
                let start = 8 - bytes.len();
                buf[start..].copy_from_slice(bytes);
            }
            Endian::Big => {
                buf[..bytes.len()].copy_from_slice(bytes);
            }
        }
        match endian {
            Endian::Little => u64::from_le_bytes(buf),
            Endian::Big => u64::from_be_bytes(buf),
        }
    };
    let abs_bits = bits & 0x7fff_ffff_ffff_ffff;
    // Treat any NaN/Inf as missing (matches prior behavior and polars_readstat).
    let is_missing = abs_bits >= SAS_MISSING_MIN;
    (f64::from_bits(bits), is_missing)
}

/// Decode numeric bytes into (value, missing_offset) for informative-null tracking.
///
/// Returns:
///   `(value, None)`       — valid value
///   `(NAN,  None)`        — system missing (`.`) or unknown NaN → plain null, no indicator
///   `(NAN,  Some(1..=26))` — user missing `.A`..`.Z` (1=.A, 26=.Z)
///   `(NAN,  Some(27))`    — underscore missing `._`
///
/// SAS7BDAT NaN encoding (bits [47:40] of the logical u64 after endian reconstruction):
///   `0xA5`..=`0xBE` → `.Z`..`.A`  (reverse: offset = `(0xFF ^ type_byte) - 0x40`)
///   `0xD1`           → `.`  (system missing, no indicator)
///   `0xD2`           → `._` (underscore, offset 27)
///   everything else  → system missing (no indicator)
pub fn decode_numeric_bytes_mask_tagged(endian: Endian, bytes: &[u8]) -> (f64, Option<u8>) {
    if bytes.is_empty() {
        return (f64::NAN, None);
    }
    let bits = if bytes.len() >= 8 {
        let slice = &bytes[..8];
        match endian {
            Endian::Little => u64::from_le_bytes(slice.try_into().unwrap_or([0u8; 8])),
            Endian::Big => u64::from_be_bytes(slice.try_into().unwrap_or([0u8; 8])),
        }
    } else {
        let mut buf = [0u8; 8];
        match endian {
            Endian::Little => {
                let start = 8 - bytes.len();
                buf[start..].copy_from_slice(bytes);
            }
            Endian::Big => {
                buf[..bytes.len()].copy_from_slice(bytes);
            }
        }
        match endian {
            Endian::Little => u64::from_le_bytes(buf),
            Endian::Big => u64::from_be_bytes(buf),
        }
    };
    let abs_bits = bits & 0x7fff_ffff_ffff_ffff;
    if abs_bits < SAS_MISSING_MIN {
        return (f64::from_bits(bits), None); // valid value
    }
    // NaN — classify by type byte at bits [47:40]
    let type_byte = ((bits >> 40) & 0xFF) as u8;
    let offset = if type_byte >= 0xA5 && type_byte <= 0xBE {
        // .A (0xBE) through .Z (0xA5) — reversed because SAS XORs with 0xFF
        // offset = (0xFF ^ type_byte) - 0x40: 0xBE→1(.A), 0xA5→26(.Z)
        let letter_code = 0xFF ^ type_byte; // 0x41='A' through 0x5A='Z'
        Some(letter_code - 0x40)            // 1 through 26
    } else if type_byte == 0xD2 {
        Some(27u8) // ._ (underscore)
    } else {
        None // system missing (.) at 0xD1, or any other NaN → no indicator
    };
    (f64::NAN, offset)
}

/// Convert a SAS missing offset to the label string.
/// `offset` 1–26 → `.A`–`.Z`, 27 → `._`, anything else → `.`
pub fn sas_offset_to_label(offset: u8) -> String {
    match offset {
        1..=26 => format!(".{}", (b'A' + offset - 1) as char),
        27 => "._".to_string(),
        _ => ".".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_numeric() {
        // Use UTF-8 encoding (byte 20)
        let parser = ValueParser::new(Endian::Little, 20, true);

        // Test normal value
        let bytes = 42.5f64.to_le_bytes();
        let value = parser.parse_numeric(&bytes).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 42.5).abs() < 0.001),
            _ => panic!("Expected numeric value"),
        }

        // Test missing value (NaN)
        let bytes = f64::NAN.to_le_bytes();
        let value = parser.parse_numeric(&bytes).unwrap();
        match value {
            Value::Numeric(None) => {}
            _ => panic!("Expected missing numeric value"),
        }
    }

    #[test]
    fn test_parse_short_numeric_little_endian() {
        let parser = ValueParser::new(Endian::Little, 20, true);

        // Value 1.0 as f64 LE bytes: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]
        // SAS stores the 3 most significant bytes: [0xF0, 0x3F] would be 2 bytes,
        // but for 3 bytes it stores [0x00, 0xF0, 0x3F] (bytes[5..8] of the full LE double)
        let full_bytes = 1.0f64.to_le_bytes();
        // 3-byte truncated: last 3 bytes of the LE representation
        let short = &full_bytes[5..8]; // [0x00, 0xF0, 0x3F]
        let value = parser.parse_numeric(short).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 1.0).abs() < 0.001, "Expected ~1.0, got {}", v),
            _ => panic!(
                "Expected numeric value for short 3-byte LE, got {:?}",
                value
            ),
        }

        // Value 2.0 as f64 LE bytes: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40]
        let full_bytes = 2.0f64.to_le_bytes();
        let short = &full_bytes[5..8];
        let value = parser.parse_numeric(short).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 2.0).abs() < 0.001, "Expected ~2.0, got {}", v),
            _ => panic!("Expected numeric value for short 3-byte LE"),
        }

        // Empty bytes → None
        let value = parser.parse_numeric(&[]).unwrap();
        match value {
            Value::Numeric(None) => {}
            _ => panic!("Expected None for empty bytes"),
        }
    }

    #[test]
    fn test_parse_short_numeric_big_endian() {
        let parser = ValueParser::new(Endian::Big, 20, true);

        // Value 1.0 as f64 BE bytes: [0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        // 3-byte truncated: first 3 bytes [0x3F, 0xF0, 0x00]
        let full_bytes = 1.0f64.to_be_bytes();
        let short = &full_bytes[0..3];
        let value = parser.parse_numeric(short).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 1.0).abs() < 0.001, "Expected ~1.0, got {}", v),
            _ => panic!(
                "Expected numeric value for short 3-byte BE, got {:?}",
                value
            ),
        }
    }

    #[test]
    fn test_parse_character() {
        // Use UTF-8 encoding (byte 20)
        let parser = ValueParser::new(Endian::Little, 20, true);
        let parser_keep_empty = ValueParser::new(Endian::Little, 20, false);

        // Test normal string
        let bytes = b"hello   ";
        let value = parser.parse_character(bytes).unwrap();
        match value {
            Value::Character(Some(s)) => assert_eq!(s, "hello"),
            _ => panic!("Expected character value"),
        }

        // Test empty string (all spaces)
        let bytes = b"        ";
        let value = parser.parse_character(bytes).unwrap();
        match value {
            Value::Character(None) => {}
            _ => panic!("Expected missing character value"),
        }

        // Leading NUL should truncate to empty (ReadStat C-string behavior)
        let bytes = b"\0@f";
        let value = parser_keep_empty.parse_character(bytes).unwrap();
        match value {
            Value::Character(Some(s)) => assert_eq!(s, ""),
            _ => panic!("Expected empty character value"),
        }
    }
}
