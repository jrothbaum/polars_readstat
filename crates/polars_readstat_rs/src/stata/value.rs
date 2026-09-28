use crate::stata::types::Endian;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

const MISSING_FLOAT_STEP: u32 = 0x800;
const MISSING_DOUBLE_STEP: u64 = 1_u64 << 40;

#[derive(Debug, Clone, Copy)]
pub struct MissingRules {
    pub max_int8: i8,
    pub max_int16: i16,
    pub max_int32: i32,
    pub system_missing_enabled: bool,
    pub system_missing_int8: i8,
    pub system_missing_int16: i16,
    pub system_missing_int32: i32,
    pub max_float: u32,
    pub max_double: u64,
    pub missing_float: u32,
    pub missing_double: u64,
}

pub fn missing_rules(ds_format: u16) -> MissingRules {
    if ds_format < 113 {
        MissingRules {
            max_int8: 0x7e,
            max_int16: 0x7ffe,
            max_int32: 0x7ffffffe,
            system_missing_enabled: false,
            system_missing_int8: 0,
            system_missing_int16: 0,
            system_missing_int32: 0,
            max_float: 0x7effffff,
            max_double: 0x7fdfffffffffffff,
            missing_float: 0x7f000000,
            missing_double: 0x7fe0000000000000,
        }
    } else {
        // Match pyreadstat/readstat-style behavior for Stata 113+:
        // treat system missing (.) and extended missing values (.a-.z) as missing
        // for integer storage types.
        MissingRules {
            max_int8: 0x7f,
            max_int16: 0x7fff,
            max_int32: 0x7fffffff,
            system_missing_enabled: true,
            system_missing_int8: 0x65,
            system_missing_int16: 0x7fe5,
            system_missing_int32: 0x7fffffe5,
            max_float: 0x7effffff,
            max_double: 0x7fdfffffffffffff,
            missing_float: 0x7f000000,
            missing_double: 0x7fe0000000000000,
        }
    }
}

pub fn read_i8(buf: &[u8], rules: MissingRules) -> Option<i8> {
    let v = buf[0] as i8;
    if rules.system_missing_enabled && v >= rules.system_missing_int8 {
        return None;
    }
    if v > rules.max_int8 {
        None
    } else {
        Some(v)
    }
}

pub fn read_i16(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<i16> {
    let mut cursor = std::io::Cursor::new(buf);
    let v = match endian {
        Endian::Little => cursor.read_i16::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_i16::<BigEndian>().ok()?,
    };
    if rules.system_missing_enabled && v >= rules.system_missing_int16 {
        return None;
    }
    if v > rules.max_int16 {
        None
    } else {
        Some(v)
    }
}

pub fn read_i32(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<i32> {
    let mut cursor = std::io::Cursor::new(buf);
    let v = match endian {
        Endian::Little => cursor.read_i32::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_i32::<BigEndian>().ok()?,
    };
    if rules.system_missing_enabled && v >= rules.system_missing_int32 {
        return None;
    }
    if v > rules.max_int32 {
        None
    } else {
        Some(v)
    }
}

pub fn read_f32(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<f32> {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => cursor.read_u32::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_u32::<BigEndian>().ok()?,
    };
    let v = f32::from_bits(bits);
    let sign = (bits & 0x8000_0000) != 0;
    if !sign && bits > rules.max_float {
        None
    } else {
        Some(v)
    }
}

pub fn read_f64(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<f64> {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => cursor.read_u64::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_u64::<BigEndian>().ok()?,
    };
    let v = f64::from_bits(bits);
    let sign = (bits & 0x8000_0000_0000_0000) != 0;
    if !sign && bits > rules.max_double {
        None
    } else {
        Some(v)
    }
}

// --- Tagged variants for informative null tracking ---
//
// Return (value, missing_offset) where:
//   (Some(v), None)       = valid value
//   (None,    None)       = system missing (.) → plain null, no indicator
//   (None,    Some(k))    = user missing, k in 1..=26 → .a (1) through .z (26)
//
// These are only called from the informative-null code path; the fast path uses the untagged
// variants above, which remain completely unchanged.

#[inline]
fn int_missing_offset_i8(v: i8, rules: MissingRules) -> Option<u8> {
    if !rules.system_missing_enabled {
        return None;
    }
    if v < rules.system_missing_int8 {
        return None;
    }
    let offset = (v as i32 - rules.system_missing_int8 as i32) as u8;
    // 0 = system missing (.), 1-26 = .a-.z
    if offset == 0 {
        None
    } else {
        Some(offset)
    }
}

#[inline]
fn int_missing_offset_i16(v: i16, rules: MissingRules) -> Option<u8> {
    if !rules.system_missing_enabled {
        return None;
    }
    if v < rules.system_missing_int16 {
        return None;
    }
    let offset = (v as i32 - rules.system_missing_int16 as i32) as u8;
    if offset == 0 {
        None
    } else {
        Some(offset)
    }
}

#[inline]
fn int_missing_offset_i32(v: i32, rules: MissingRules) -> Option<u8> {
    if !rules.system_missing_enabled {
        return None;
    }
    if v < rules.system_missing_int32 {
        return None;
    }
    let offset = (v - rules.system_missing_int32) as u8;
    if offset == 0 {
        None
    } else {
        Some(offset)
    }
}

pub fn read_i8_tagged(buf: &[u8], rules: MissingRules) -> (Option<i8>, Option<u8>) {
    let v = buf[0] as i8;
    if rules.system_missing_enabled && v >= rules.system_missing_int8 {
        let offset = int_missing_offset_i8(v, rules);
        return (None, offset);
    }
    if v > rules.max_int8 {
        (None, None)
    } else {
        (Some(v), None)
    }
}

pub fn read_i16_tagged(
    buf: &[u8],
    endian: Endian,
    rules: MissingRules,
) -> (Option<i16>, Option<u8>) {
    let mut cursor = std::io::Cursor::new(buf);
    let v = match endian {
        Endian::Little => match cursor.read_i16::<LittleEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
        Endian::Big => match cursor.read_i16::<BigEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
    };
    if rules.system_missing_enabled && v >= rules.system_missing_int16 {
        let offset = int_missing_offset_i16(v, rules);
        return (None, offset);
    }
    if v > rules.max_int16 {
        (None, None)
    } else {
        (Some(v), None)
    }
}

pub fn read_i32_tagged(
    buf: &[u8],
    endian: Endian,
    rules: MissingRules,
) -> (Option<i32>, Option<u8>) {
    let mut cursor = std::io::Cursor::new(buf);
    let v = match endian {
        Endian::Little => match cursor.read_i32::<LittleEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
        Endian::Big => match cursor.read_i32::<BigEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
    };
    if rules.system_missing_enabled && v >= rules.system_missing_int32 {
        let offset = int_missing_offset_i32(v, rules);
        return (None, offset);
    }
    if v > rules.max_int32 {
        (None, None)
    } else {
        (Some(v), None)
    }
}

pub fn read_f32_tagged(
    buf: &[u8],
    endian: Endian,
    rules: MissingRules,
) -> (Option<f32>, Option<u8>) {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => match cursor.read_u32::<LittleEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
        Endian::Big => match cursor.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
    };
    let v = f32::from_bits(bits);
    let sign = (bits & 0x8000_0000) != 0;
    if !sign && bits > rules.max_float {
        // Stata float missing: missing_float = system missing; values above that are user missing.
        // Float/double user-missing offsets are encoded in the bits above missing_float.
        let offset = if bits == rules.missing_float {
            None // system missing
        } else {
            // For Stata floats, each user-missing increments by 0x800.
            let diff = bits.wrapping_sub(rules.missing_float);
            let k = diff / MISSING_FLOAT_STEP;
            if diff % MISSING_FLOAT_STEP == 0 && (1..=26).contains(&k) {
                Some(k as u8)
            } else {
                None
            }
        };
        (None, offset)
    } else {
        (Some(v), None)
    }
}

pub fn read_f64_tagged(
    buf: &[u8],
    endian: Endian,
    rules: MissingRules,
) -> (Option<f64>, Option<u8>) {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => match cursor.read_u64::<LittleEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
        Endian::Big => match cursor.read_u64::<BigEndian>() {
            Ok(v) => v,
            Err(_) => return (None, None),
        },
    };
    let v = f64::from_bits(bits);
    let sign = (bits & 0x8000_0000_0000_0000) != 0;
    if !sign && bits > rules.max_double {
        // For Stata doubles, each user-missing increments by 2^40.
        let offset = if bits == rules.missing_double {
            None // system missing
        } else {
            let diff = bits.wrapping_sub(rules.missing_double);
            let k = diff / MISSING_DOUBLE_STEP;
            if diff % MISSING_DOUBLE_STEP == 0 && (1..=26).contains(&k) {
                Some(k as u8)
            } else {
                None
            }
        };
        (None, offset)
    } else {
        (Some(v), None)
    }
}

/// Convert a missing offset (1=.a, 2=.b … 26=.z) to the Stata native notation string.
pub fn offset_to_stata_label(offset: u8) -> String {
    if offset == 0 || offset > 26 {
        return ".".to_string();
    }
    let letter = (b'a' + offset - 1) as char;
    format!(".{}", letter)
}

// Stata numeric values are stored in two's complement in practice.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untagged_float_readers_convert_extended_missing_values_to_null() {
        let rules = missing_rules(118);

        for offset in [1, 26] {
            let float_bits = rules.missing_float + offset * MISSING_FLOAT_STEP;
            assert_eq!(
                read_f32(&float_bits.to_le_bytes(), Endian::Little, rules),
                None
            );

            let double_bits = rules.missing_double + (offset as u64) * MISSING_DOUBLE_STEP;
            assert_eq!(
                read_f64(&double_bits.to_le_bytes(), Endian::Little, rules),
                None
            );
        }
    }

    #[test]
    fn tagged_float_readers_decode_extended_missing_offsets() {
        let rules = missing_rules(118);

        for offset in [1_u8, 26] {
            let float_bits = rules.missing_float + u32::from(offset) * MISSING_FLOAT_STEP;
            assert_eq!(
                read_f32_tagged(&float_bits.to_le_bytes(), Endian::Little, rules),
                (None, Some(offset))
            );

            let double_bits = rules.missing_double + u64::from(offset) * MISSING_DOUBLE_STEP;
            assert_eq!(
                read_f64_tagged(&double_bits.to_le_bytes(), Endian::Little, rules),
                (None, Some(offset))
            );
        }

        let invalid_float = rules.missing_float + 1;
        assert_eq!(
            read_f32_tagged(&invalid_float.to_le_bytes(), Endian::Little, rules),
            (None, None)
        );
        let invalid_double = rules.missing_double + 1;
        assert_eq!(
            read_f64_tagged(&invalid_double.to_le_bytes(), Endian::Little, rules),
            (None, None)
        );
    }
}
