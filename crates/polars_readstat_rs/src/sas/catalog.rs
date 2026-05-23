use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::sas::constants::MAGIC_NUMBER;
use crate::sas::encoding;
use crate::sas::error::{Error, Result};

pub const CATALOG_MAGIC_NUMBER: &[u8; 32] = &[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xc2, 0xea, 0x81, 0x63,
    0xb3, 0x14, 0x11, 0xcf, 0xbd, 0x92, 0x08, 0x00,
    0x09, 0xc7, 0x31, 0x8c, 0x18, 0x1f, 0x10, 0x11,
];

const CATALOG_FIRST_INDEX_PAGE: i64 = 1;
const CATALOG_USELESS_PAGES: i64 = 3;

/// A SAS value-label key: either a numeric code or a character string.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogKey {
    Numeric(f64),
    Text(String),
}

/// Parsed SAS format catalog: format_name → [(code, label)]
pub type CatalogMap = HashMap<String, Vec<(CatalogKey, String)>>;

struct Ctx {
    u64_mode: bool,
    /// Header alignment pad (0 or 4, from header byte 35)
    pad1: usize,
    /// True when file byte order differs from machine byte order
    bswap: bool,
    xlsr_size: usize,
    xlsr_o_offset: usize,
    page_count: i64,
    page_size: i64,
    header_size: i64,
    encoding_byte: u8,
    block_pointers: Vec<u64>,
}

// ---------------------------------------------------------------------------
// Low-level readers
// ---------------------------------------------------------------------------

#[inline]
fn r2(data: &[u8], offset: usize, bswap: bool) -> u16 {
    let v = u16::from_ne_bytes(data[offset..offset + 2].try_into().unwrap());
    if bswap { v.swap_bytes() } else { v }
}

#[inline]
fn r4(data: &[u8], offset: usize, bswap: bool) -> u32 {
    let v = u32::from_ne_bytes(data[offset..offset + 4].try_into().unwrap());
    if bswap { v.swap_bytes() } else { v }
}

#[inline]
fn r8(data: &[u8], offset: usize, bswap: bool) -> u64 {
    let v = u64::from_ne_bytes(data[offset..offset + 8].try_into().unwrap());
    if bswap { v.swap_bytes() } else { v }
}

/// Doubles in SAS catalogs are always stored big-endian.
#[inline]
fn r_double_be(data: &[u8], offset: usize) -> f64 {
    f64::from_be_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn decode(bytes: &[u8], encoding_byte: u8) -> String {
    let enc = encoding::get_encoding(encoding_byte);
    let s = encoding::decode_string(bytes, encoding_byte, enc);
    s.trim_end_matches(|c| c == '\0' || c == ' ').to_string()
}

// ---------------------------------------------------------------------------
// XLSR index parsing
// ---------------------------------------------------------------------------

/// Scan a contiguous region for XLSR entries and collect block pointers.
fn augment_index(page: &[u8], start: usize, ctx: &mut Ctx) {
    let end = page.len();
    let mut off = start;

    while off + ctx.xlsr_size <= end {
        // Handle up to 8 bytes of padding before an XLSR entry.
        if off + 4 > end || &page[off..off + 4] != b"XLSR" {
            off += 8;
            if off + 4 > end || &page[off..off + 4] != b"XLSR" {
                break;
            }
        }

        if off + ctx.xlsr_o_offset >= end {
            break;
        }

        if page[off + ctx.xlsr_o_offset] == b'O' {
            let (page_num, pos) = if ctx.u64_mode {
                if off + 18 > end { break; }
                (r8(page, off + 8, ctx.bswap), r2(page, off + 16, ctx.bswap) as u64)
            } else {
                if off + 10 > end { break; }
                (r4(page, off + 4, ctx.bswap) as u64, r2(page, off + 8, ctx.bswap) as u64)
            };
            if page_num > 0 && pos > 0 {
                ctx.block_pointers.push((page_num << 32) | pos);
            }
        }

        off += ctx.xlsr_size;
    }
}

// ---------------------------------------------------------------------------
// Chain-linked block reading
// ---------------------------------------------------------------------------

/// Follow the chain of linked segments and assemble a block's full data.
fn read_block<R: Read + Seek>(reader: &mut R, start_page: i64, start_pos: u16, ctx: &Ctx) -> Result<Vec<u8>> {
    let chain_hdr_len: usize = if ctx.u64_mode { 32 } else { 16 };
    let mut next_page = start_page;
    let mut next_pos = start_pos as i64;
    let mut data: Vec<u8> = Vec::new();
    let mut link_count = 0i64;
    let mut chain_hdr = vec![0u8; chain_hdr_len];

    while next_page > 0 && next_pos > 0 && next_page <= ctx.page_count && link_count < ctx.page_count {
        let seek = ctx.header_size + (next_page - 1) * ctx.page_size + next_pos;
        reader.seek(SeekFrom::Start(seek as u64))?;
        reader.read_exact(&mut chain_hdr)?;

        let (np, npp, ll) = if ctx.u64_mode {
            (r4(&chain_hdr, 0, ctx.bswap) as i64,
             r2(&chain_hdr, 8, ctx.bswap) as i64,
             r2(&chain_hdr, 10, ctx.bswap) as usize)
        } else {
            (r4(&chain_hdr, 0, ctx.bswap) as i64,
             r2(&chain_hdr, 4, ctx.bswap) as i64,
             r2(&chain_hdr, 6, ctx.bswap) as usize)
        };

        if ll > 0 {
            let old_len = data.len();
            data.resize(old_len + ll, 0);
            reader.read_exact(&mut data[old_len..])?;
        }

        next_page = np;
        next_pos = npp;
        link_count += 1;
    }

    Ok(data)
}

// ---------------------------------------------------------------------------
// Block and value-label parsing
// ---------------------------------------------------------------------------

/// Parse the value-label entries from the raw payload of a catalog block.
///
/// Two-pass algorithm translated directly from readstat_sas7bcat_read.c:
/// pass 1 maps label_pos → byte-offset-of-value-entry, pass 2 reads pairs.
fn parse_value_labels(
    payload: &[u8],
    label_count_used: usize,
    label_count_capacity: usize,
    is_string: bool,
    encoding_byte: u8,
    bswap: bool,
    pad1: usize,
) -> Vec<(CatalogKey, String)> {
    if payload.is_empty() || label_count_capacity == 0 || label_count_used == 0 {
        return Vec::new();
    }

    // Pass 1: walk value entries to build value_offsets[label_pos] = byte_offset
    let mut value_offsets = vec![0usize; label_count_used];
    let mut vpos = 0usize;

    for i in 0..label_count_capacity {
        if vpos + 4 > payload.len() { break; }
        let entry_extra = r2(payload, vpos + 2, bswap) as usize;
        let entry_len = 6 + entry_extra;

        if i < label_count_used {
            let lp_off = vpos + 10 + pad1;
            if lp_off + 4 <= payload.len() {
                let label_pos = r4(payload, lp_off, bswap) as usize;
                if label_pos < label_count_used {
                    value_offsets[label_pos] = vpos;
                }
            }
        }

        vpos += entry_len;
        if vpos > payload.len() { break; }
    }

    // lbp2 starts just after the value entries (at vpos)
    let mut lpos = vpos;
    let mut result = Vec::with_capacity(label_count_used);

    // Pass 2: read (value, label) pairs
    for i in 0..label_count_used {
        if lpos + 10 > payload.len() { break; }

        let voff = value_offsets[i];

        let key = if is_string {
            if voff + 4 > payload.len() { break; }
            let entry_extra = r2(payload, voff + 2, bswap) as usize;
            let entry_len = 6 + entry_extra;
            if entry_len < 16 || voff + entry_len > payload.len() { break; }
            let str_bytes = &payload[voff + entry_len - 16..voff + entry_len];
            CatalogKey::Text(decode(str_bytes, encoding_byte))
        } else {
            if voff + 30 > payload.len() { break; }
            let raw_val = r_double_be(payload, voff + 22);
            let bits = raw_val.to_bits();
            // SAS missing/tag values: lower 40 bits are all 1, upper 24 bits 0
            if (bits | 0xFF_0000_0000_00) == 0xFF_FFFF_FFFF_FF {
                // Skip: advance label pointer and continue
                let lbl_len = r2(payload, lpos + 8, bswap) as usize;
                lpos += 10 + lbl_len + 1;
                continue;
            }
            CatalogKey::Numeric(raw_val * -1.0)
        };

        let lbl_len = r2(payload, lpos + 8, bswap) as usize;
        if lpos + 10 + lbl_len > payload.len() { break; }
        let lbl = decode(&payload[lpos + 10..lpos + 10 + lbl_len], encoding_byte);

        result.push((key, lbl));
        lpos += 10 + lbl_len + 1;
    }

    result
}

/// Parse one catalog block and return `(format_name, labels)` if non-empty.
fn parse_block(data: &[u8], ctx: &Ctx) -> Option<(String, Vec<(CatalogKey, String)>)> {
    if data.len() < 106 { return None; }

    let flags = r2(data, 2, ctx.bswap);
    // Local pad: flag 0x08 adds 4; if set, total block-local pad becomes 4+16=20
    let local_pad_base: usize = if flags & 0x08 != 0 { 4 } else { 0 };

    let (label_count_cap, label_count_used) = if ctx.u64_mode {
        if data.len() < 58 + local_pad_base { return None; }
        (r8(data, 42 + local_pad_base, ctx.bswap) as usize,
         r8(data, 50 + local_pad_base, ctx.bswap) as usize)
    } else {
        if data.len() < 46 + local_pad_base { return None; }
        (r4(data, 38 + local_pad_base, ctx.bswap) as usize,
         r4(data, 42 + local_pad_base, ctx.bswap) as usize)
    };

    // Short format name at offset 8 (8 bytes)
    let mut name = decode(&data[8..16.min(data.len())], ctx.encoding_byte);

    let payload_off = 106usize + if ctx.u64_mode { 32 } else { 0 };
    // Effective block-local pad (adds 16 when the base pad is non-zero)
    let mut eff_pad = if local_pad_base > 0 { local_pad_base + 16 } else { 0 };

    // Long name flag
    let has_long = if ctx.u64_mode { flags & 0x20 != 0 } else { flags & 0x80 != 0 };
    if has_long {
        let long_start = payload_off + eff_pad;
        if long_start + 32 > data.len() { return None; }
        name = decode(&data[long_start..long_start + 32], ctx.encoding_byte);
        eff_pad += 32;
    }

    if label_count_used == 0 { return None; }

    let vl_start = payload_off + eff_pad;
    if vl_start >= data.len() { return None; }

    let is_string = name.starts_with('$');
    let labels = parse_value_labels(
        &data[vl_start..],
        label_count_used,
        label_count_cap,
        is_string,
        ctx.encoding_byte,
        ctx.bswap,
        ctx.pad1,
    );

    if labels.is_empty() { return None; }

    // Normalise: strip trailing dot(s), uppercase
    let norm_name = name.trim_end_matches('.').to_uppercase();
    Some((norm_name, labels))
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Read a `.sas7bcat` file and return a map of `format_name → [(code, label)]`.
///
/// Format names are normalised: trailing `.` stripped, uppercased.
/// Numeric codes have the SAS negation already reversed (`code * -1`).
/// SAS missing-value tags are silently skipped.
pub fn read_sas7bcat(path: &Path) -> Result<CatalogMap> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read enough of the header to detect format/endian/encoding
    let mut hdr = vec![0u8; 288];
    reader.read_exact(&mut hdr)?;

    let magic = &hdr[0..32];
    if magic != CATALOG_MAGIC_NUMBER.as_ref() && magic != MAGIC_NUMBER.as_ref() {
        return Err(Error::InvalidMagicNumber);
    }

    let u64_mode = hdr[32] == b'3';
    let file_le = hdr[37] == 0x01;
    let bswap = cfg!(target_endian = "little") ^ file_le;
    let pad1: usize = if hdr[35] == b'3' { 4 } else { 0 };
    let align1 = pad1;
    let encoding_byte = hdr[70];

    // Header size at offset 196 + align1
    let header_size = r4(&hdr, 196 + align1, bswap) as i64;

    // Read any remaining header bytes needed for page info
    let mut full_hdr = hdr;
    if header_size as usize > 288 {
        full_hdr.resize(header_size as usize, 0);
        reader.read_exact(&mut full_hdr[288..])?;
    }

    let page_size = r4(&full_hdr, 200 + align1, bswap) as i64;
    let page_count = r4(&full_hdr, 204 + align1, bswap) as i64;

    let xlsr_size = 212 + pad1 + if u64_mode { 72 } else { 0 };
    let xlsr_offset = 856 + 2 * pad1 + if u64_mode { 144 } else { 0 };
    let xlsr_o_offset = 50 + pad1 + if u64_mode { 24 } else { 0 };

    let mut ctx = Ctx {
        u64_mode,
        pad1,
        bswap,
        xlsr_size,
        xlsr_o_offset,
        page_count,
        page_size,
        header_size,
        encoding_byte,
        block_pointers: Vec::with_capacity(200),
    };

    let mut page_buf = vec![0u8; page_size as usize];

    // Pass 1a: index page (CATALOG_FIRST_INDEX_PAGE)
    reader.seek(SeekFrom::Start(
        (header_size + CATALOG_FIRST_INDEX_PAGE * page_size) as u64,
    ))?;
    reader.read_exact(&mut page_buf)?;
    if xlsr_offset < page_buf.len() {
        augment_index(&page_buf, xlsr_offset, &mut ctx);
    }

    // Pass 1b: remaining pages may carry XLSR entries starting at byte 16
    for i in CATALOG_USELESS_PAGES..page_count {
        reader.seek(SeekFrom::Start((header_size + i * page_size) as u64))?;
        reader.read_exact(&mut page_buf)?;
        if page_buf.len() >= 20 && &page_buf[16..20] == b"XLSR" {
            augment_index(&page_buf, 16, &mut ctx);
        }
    }

    // Sort and deduplicate block pointers
    ctx.block_pointers.sort_unstable();
    ctx.block_pointers.dedup();

    // Pass 2: read and parse each block
    let mut catalog = CatalogMap::new();
    let pointers = ctx.block_pointers.clone();

    for bp in pointers {
        let start_page = (bp >> 32) as i64;
        let start_pos = (bp & 0xFFFF) as u16;

        if let Ok(block) = read_block(&mut reader, start_page, start_pos, &ctx) {
            if let Some((name, labels)) = parse_block(&block, &ctx) {
                catalog.entry(name).or_default().extend(labels);
            }
        }
    }

    Ok(catalog)
}
