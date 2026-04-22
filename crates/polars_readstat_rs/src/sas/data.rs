use crate::decompressor::Decompressor;
use crate::error::{Error, Result};
use crate::page::{PageHeader, PageReader, PageSubheader};
use crate::types::{Compression, Endian, Format, Metadata, PageType};
use std::io::{Read, Seek};

/// Represents a row of raw bytes from the SAS file
pub type RowBytes = Vec<u8>;

/// Iterator-like reader for SAS data rows
pub struct DataReader<R: Read + Seek> {
    page_reader: PageReader<R>,
    metadata: Metadata,
    decompressor: Decompressor,
    current_row: usize,
    page_state: Option<PageState>,
    /// Reusable buffer for decompressed rows (avoids per-row allocation)
    decompress_buf: Vec<u8>,
    /// When Some(n), stop reading after n more pages (page-limited parallel mode).
    /// In this mode the current_row >= metadata.row_count guard is bypassed; the
    /// page budget is the sole stopping condition.
    remaining_pages: Option<usize>,
}

/// State for tracking position within a page
#[derive(Debug)]
enum PageState {
    /// META page with data subheaders
    Meta {
        data_subheaders: Vec<DataSubheader>,
        current_index: usize,
    },
    /// DATA page with sequential rows
    Data {
        block_count: usize,
        offset: usize,
        row_length: usize,
        current_index: usize,
    },
    /// MIX page with metadata and data
    Mix {
        row_count: usize,
        offset: usize,
        row_length: usize,
        current_index: usize,
    },
}

/// Data subheader from META or MIX pages
#[derive(Debug, Clone)]
pub struct DataSubheader {
    pub offset: usize,
    pub length: usize,
    pub compression: u8, // 4 = compressed, 0 = uncompressed
}

impl<R: Read + Seek> DataReader<R> {
    pub fn new(
        page_reader: PageReader<R>,
        metadata: Metadata,
        _endian: Endian,
        _format: Format,
        initial_data_subheaders: Vec<DataSubheader>,
    ) -> Result<Self> {
        let decompressor = Decompressor::new(metadata.compression);
        let decompress_buf = vec![0u8; metadata.row_length];

        // If we have initial data subheaders from metadata reading, use them as first page
        // (matching C++ behavior)
        let page_state = if !initial_data_subheaders.is_empty() {
            Some(PageState::Meta {
                data_subheaders: initial_data_subheaders,
                current_index: 0,
            })
        } else {
            None
        };

        let mut reader = Self {
            page_reader,
            metadata,
            decompressor,
            current_row: 0,
            page_state,
            decompress_buf,
            remaining_pages: None,
        };

        // Try to read the first page if we don't have initial data subheaders
        if reader.page_state.is_none() {
            reader.advance_page()?;
        }

        Ok(reader)
    }

    /// Limit reading to the next `n` data-bearing pages (page-limited parallel mode).
    /// Once this budget is exhausted `read_rows_bulk` returns 0 regardless of current_row.
    pub fn set_max_pages(&mut self, n: usize) {
        self.remaining_pages = Some(n);
    }

    /// Set the current row counter (for direct-seek parallel reads).
    /// Call this after constructing a DataReader that was seeked to a specific page,
    /// so that the row counter correctly reflects the logical position.
    pub fn set_current_row(&mut self, row: usize) {
        self.current_row = row;
    }

    /// Read the next row (allocating). Used by streaming paths that need owned data.
    pub fn read_row(&mut self) -> Result<Option<RowBytes>> {
        // Check if we've read all rows
        if self.current_row >= self.metadata.row_count {
            return Ok(None);
        }

        // Get row offset, length, and compression flag from current page state
        let (offset, length, compression) = match self.get_current_row_info() {
            Some(info) => info,
            None => {
                // Need to advance to next page
                self.advance_page()?;
                match self.get_current_row_info() {
                    Some(info) => info,
                    None => return Ok(None),
                }
            }
        };

        let row_bytes = self.extract_row_bytes(offset, length, compression)?;

        // Advance to next row
        self.advance_row();

        Ok(Some(row_bytes))
    }

    /// Ensure we're positioned on a valid row, advancing pages if needed.
    /// Returns (offset, length) or None if no more rows.
    fn ensure_row_ready(&mut self) -> Result<Option<(usize, usize)>> {
        if self.current_row >= self.metadata.row_count {
            return Ok(None);
        }
        let (offset, length, _) = match self.get_current_row_info() {
            Some(info) => info,
            None => {
                self.advance_page()?;
                match self.get_current_row_info() {
                    Some(info) => info,
                    None => return Ok(None),
                }
            }
        };
        Ok(Some((offset, length)))
    }

    /// Read the next row without allocating. Returns a reference to either the
    /// page buffer (uncompressed) or the internal decompress buffer (compressed).
    /// The returned slice is valid until the next call to any read method.
    pub fn read_row_borrowed(&mut self) -> Result<Option<&[u8]>> {
        let (offset, length) = match self.ensure_row_ready()? {
            Some(v) => v,
            None => return Ok(None),
        };

        let page_buffer = self.page_reader.page_buffer();
        // ---- TEMP LOGGING ----
        if self.current_row < 3 {
            debug_log(&format!(
                "ROW {} | page_state={:?} | raw_offset={} raw_len={} row_length={} compression={:?}",
                self.current_row,
                match &self.page_state {
                    Some(PageState::Meta { .. }) => "META",
                    Some(PageState::Data { .. }) => "DATA",
                    Some(PageState::Mix { .. }) => "MIX",
                    None => "NONE",
                },
                offset,
                length,
                self.metadata.row_length,
                self.metadata.compression,
            ));
            // Print raw compressed bytes (first 32)
            let end = (offset + length).min(page_buffer.len());
            debug_log(&format!("  raw_bytes[..32]: {:?}", &page_buffer[offset..end.min(offset+32)]));
        }
        // ---- END TEMP LOGGING ----
        if offset + length > page_buffer.len() {
            return Err(Error::BufferOutOfBounds { offset, length });
        }

        let is_compressed = self.metadata.compression != Compression::None;
        debug_log(&format!("=== compressed: {}",is_compressed));
        if is_compressed {
            let raw_bytes = &page_buffer[offset..offset + length];
            self.decompressor.decompress_into(raw_bytes, &mut self.decompress_buf)?;

            // ---- TEMP LOGGING ----
            if self.current_row < 3 {
                debug_log(&format!("=== ROW {} | raw_len={} row_length={} buf_len={} ===",
                    self.current_row, length, self.metadata.row_length, self.decompress_buf.len()));
                for col in &self.metadata.columns {
                    let end = col.offset + col.length;
                    if end <= self.decompress_buf.len() {
                        let bytes = &self.decompress_buf[col.offset..end];
                        debug_log(&format!("  col={} offset={} len={} str={:?}",
                            col.name, col.offset, col.length, String::from_utf8_lossy(bytes)));
                    } else {
                        debug_log(&format!("  col={} offset={} len={} OUT OF BOUNDS (buf={})",
                            col.name, col.offset, col.length, self.decompress_buf.len()));
                    }
                }
            }
            // ---- END TEMP LOGGING ----
            self.advance_row();
            Ok(Some(&self.decompress_buf))
        } else {
            self.advance_row();
            Ok(Some(&self.page_reader.page_buffer()[offset..offset + length]))
        }
    }

    /// Read up to `n` rows into `buf` as flat bytes (`row_length` bytes per row, consecutive).
    /// `buf` is NOT cleared — caller should clear/reserve before calling if needed.
    /// Returns the number of rows actually read.
    ///
    /// For uncompressed DATA pages the inner loop is replaced by a single bulk
    /// `extend_from_slice`, which is important on network filesystems where the
    /// OS page-cache is cold and per-row function-call overhead is significant.
    pub fn read_rows_bulk(&mut self, n: usize, buf: &mut Vec<u8>) -> Result<usize> {
        let row_length = self.metadata.row_length;
        if row_length == 0 || n == 0 {
            return Ok(0);
        }
        buf.reserve(n * row_length);
        let mut count = 0;

        while count < n {
            if self.remaining_pages.is_none() && self.current_row >= self.metadata.row_count {
                break;
            }

            // Fast path: bulk-copy all available rows from an uncompressed DATA page.
            let bulk = self.try_bulk_copy_data_page(n - count, buf, row_length)?;
            if bulk > 0 {
                count += bulk;
                continue;
            }

            // Slow path: per-row copy (MIX/META pages, or compressed data).
            let (offset, length) = match self.ensure_row_ready()? {
                Some(v) => v,
                None => break,
            };
            if offset + length > self.page_reader.page_buffer().len() {
                return Err(Error::BufferOutOfBounds { offset, length });
            }
            
            let is_compressed = self.metadata.compression != Compression::None;
            debug_log(&format!("=== compressed: {}",is_compressed));
            if is_compressed {
                // Compressed: borrow raw bytes, decompress, then copy out.
                let raw_bytes = &self.page_reader.page_buffer()[offset..offset + length];
                self.decompressor
                    .decompress_into(raw_bytes, &mut self.decompress_buf)?;
                buf.extend_from_slice(&self.decompress_buf[..row_length]);
            } else {
                buf.extend_from_slice(&self.page_reader.page_buffer()[offset..offset + length]);
            }
            self.advance_row();
            count += 1;
        }

        Ok(count)
    }

    /// Fast-path bulk copy for uncompressed DATA pages.
    ///
    /// All rows on a DATA page are laid out contiguously in the page buffer, so
    /// we can copy `min(max_rows, available)` rows with a single `extend_from_slice`
    /// instead of `available` individual calls.  Returns 0 if not applicable.
    fn try_bulk_copy_data_page(
        &mut self,
        max_rows: usize,
        buf: &mut Vec<u8>,
        row_length: usize,
    ) -> Result<usize> {
        if self.metadata.compression != crate::types::Compression::None {
            return Ok(0);
        }

        // Extract the byte range while holding only an immutable borrow, so
        // that we can drop the borrow before calling advance_rows (which needs
        // &mut self).
        let (start_byte, available) = match &self.page_state {
            Some(PageState::Data {
                offset,
                row_length: page_rl,
                block_count,
                current_index,
            }) => {
                if *page_rl != row_length || *current_index >= *block_count {
                    return Ok(0);
                }
                let avail = block_count - current_index;
                (offset + current_index * row_length, avail)
            }
            _ => return Ok(0),
        }; // immutable borrow of self.page_state ends here

        let to_copy = max_rows.min(available);
        if to_copy == 0 {
            return Ok(0);
        }

        let byte_end = start_byte + to_copy * row_length;
        let actual = {
            let page_buf = self.page_reader.page_buffer();
            if byte_end <= page_buf.len() {
                buf.extend_from_slice(&page_buf[start_byte..byte_end]);
                to_copy
            } else {
                // Clamp to what actually fits in the page.
                let fits = page_buf.len().saturating_sub(start_byte) / row_length;
                if fits == 0 {
                    return Ok(0);
                }
                buf.extend_from_slice(&page_buf[start_byte..start_byte + fits * row_length]);
                fits
            }
        }; // immutable borrow of page_buf ends here

        self.advance_rows(actual);
        Ok(actual)
    }

    /// Skip n rows
    pub fn skip_rows(&mut self, n: usize) -> Result<()> {
        let mut remaining = n.min(self.metadata.row_count.saturating_sub(self.current_row));

        while remaining > 0 {
            let available = self.rows_remaining_in_page();
            if available == 0 {
                self.advance_page()?;
                if self.rows_remaining_in_page() == 0 {
                    // No more rows available from source.
                    break;
                }
                continue;
            }

            let to_skip = remaining.min(available);
            self.advance_rows(to_skip);
            remaining -= to_skip;
        }

        Ok(())
    }

    fn rows_remaining_in_page(&self) -> usize {
        match &self.page_state {
            Some(PageState::Meta {
                data_subheaders,
                current_index,
            }) => data_subheaders.len().saturating_sub(*current_index),
            Some(PageState::Data {
                block_count,
                offset,
                row_length,
                current_index,
            }) => {
                if *row_length == 0 {
                    return 0;
                }
                let page_size = self.page_reader.header().page_length;
                if *offset >= page_size {
                    return 0;
                }
                let max_fit = (page_size - *offset) / *row_length;
                let valid_rows = (*block_count).min(max_fit);
                valid_rows.saturating_sub(*current_index)
            }
            Some(PageState::Mix {
                row_count,
                offset,
                row_length,
                current_index,
            }) => {
                if *row_length == 0 {
                    return 0;
                }
                let page_size = self.page_reader.header().page_length;
                if *offset >= page_size {
                    return 0;
                }
                let max_fit = (page_size - *offset) / *row_length;
                let valid_rows = (*row_count).min(max_fit);
                valid_rows.saturating_sub(*current_index)
            }
            None => 0,
        }
    }

    fn advance_rows(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.current_row += n;
        match &mut self.page_state {
            Some(PageState::Meta { current_index, .. }) => *current_index += n,
            Some(PageState::Data { current_index, .. }) => *current_index += n,
            Some(PageState::Mix { current_index, .. }) => *current_index += n,
            None => {}
        }
    }

    /// Get current row offset, length, and compression flag from page state
    fn get_current_row_info(&self) -> Option<(usize, usize, u8)> {
        match &self.page_state {
            Some(PageState::Meta {
                data_subheaders,
                current_index,
            }) => {
                if *current_index < data_subheaders.len() {
                    let sub = &data_subheaders[*current_index];
                    Some((sub.offset, sub.length, sub.compression))
                } else {
                    None
                }
            }
            Some(PageState::Data {
                offset,
                row_length,
                current_index,
                block_count,
            }) => {
                if *current_index < *block_count {
                    let row_offset = offset + row_length * current_index;
                    // Check if row would fit in current page
                    let page_size = self.page_reader.header().page_length;
                    if row_offset + row_length <= page_size {
                        // For DATA pages, assume compression=4 if file is compressed
                        Some((row_offset, *row_length, 4))
                    } else {
                        // Row would extend beyond page, stop processing this page
                        None
                    }
                } else {
                    None
                }
            }
            Some(PageState::Mix {
                offset,
                row_length,
                current_index,
                row_count,
            }) => {
                if *current_index < *row_count {
                    let row_offset = offset + row_length * current_index;
                    // Check if row would fit in current page
                    let page_size = self.page_reader.header().page_length;
                    if row_offset + row_length <= page_size {
                        // For MIX pages, assume compression=4 if file is compressed
                        Some((row_offset, *row_length, 4))
                    } else {
                        // Row would extend beyond page, stop processing this page
                        None
                    }
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Advance to next row within current page
    fn advance_row(&mut self) {
        self.current_row += 1;

        match &mut self.page_state {
            Some(PageState::Meta { current_index, .. }) => {
                *current_index += 1;
            }
            Some(PageState::Data { current_index, .. }) => {
                *current_index += 1;
            }
            Some(PageState::Mix { current_index, .. }) => {
                *current_index += 1;
            }
            None => {}
        }
    }

    /// Advance to next page
    fn advance_page(&mut self) -> Result<()> {
        if let Some(0) = self.remaining_pages {
            self.page_state = None;
            return Ok(());
        }
        loop {
            if !self.page_reader.read_page()? {
                return Ok(());
            }

            let page_header = self.page_reader.get_page_header()?;

            if let Some(page_state) = self.build_page_state(&page_header)? {
                if let Some(ref mut rem) = self.remaining_pages {
                    *rem = rem.saturating_sub(1);
                }
                self.page_state = Some(page_state);
                return Ok(());
            }
            // Non-data page: doesn't count against the budget, continue
        }
    }

    /// Build page state based on page type
    fn build_page_state(&mut self, page_header: &PageHeader) -> Result<Option<PageState>> {
        let format = self.page_reader.header().format;
        let page_bit_offset = match format {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };

        match page_header.page_type {
            PageType::Meta | PageType::Mix1 | PageType::Mix2 | PageType::Amd => {
                // Extract data subheaders from META/MIX pages
                let subheaders = self.page_reader.get_subheaders(page_header)?;
                let data_subheaders = self.extract_data_subheaders(&subheaders)?;

                if !data_subheaders.is_empty() {
                    return Ok(Some(PageState::Meta {
                        data_subheaders,
                        current_index: 0,
                    }));
                }

                // If it's a MIX page with no data subheaders, treat as MIX data page
                if matches!(page_header.page_type, PageType::Mix1 | PageType::Mix2) {
                    let integer_size = match format {
                        Format::Bit64 => 8,
                        Format::Bit32 => 4,
                    };
                    let subheader_size = 3 * integer_size;
                    // MIX pages: rows start immediately after the subheader table.
                    let mut offset =
                        page_bit_offset + 8 + page_header.subheader_count as usize * subheader_size;

                    // Match ReadStat MIX-page alignment behavior:
                    // - For non-Stat/Transfer files, always skip 4 bytes when
                    //   the data pointer is 4-byte misaligned.
                    // - For Stat/Transfer files, skip only if the next 4 bytes
                    //   are all zeros or spaces.
                    if offset % 8 == 4 {
                        let page_buffer = self.page_reader.page_buffer();
                        if offset + 4 <= page_buffer.len() {
                            let pad = &page_buffer[offset..offset + 4];
                            let vendor_is_stat_transfer = is_stat_transfer_release(
                                self.page_reader.header().sas_release.as_str(),
                            );
                            let pad_is_zero_or_space =
                                pad == [0, 0, 0, 0] || pad == [b' ', b' ', b' ', b' '];
                            if !vendor_is_stat_transfer || pad_is_zero_or_space {
                                offset += 4;
                            }
                        }
                    }

                    let row_count = self
                        .metadata
                        .row_count
                        .min(self.metadata.mix_page_row_count);

                    return Ok(Some(PageState::Mix {
                        row_count,
                        offset,
                        row_length: self.metadata.row_length,
                        current_index: 0,
                    }));
                }

                Ok(None)
            }
            PageType::Data => {
                // DATA page: rows start at page_bit_offset + 8
                Ok(Some(PageState::Data {
                    block_count: page_header.block_count as usize,
                    offset: page_bit_offset + 8,
                    row_length: self.metadata.row_length,
                    current_index: 0,
                }))
            }
            _ => Ok(None),
        }
    }

    /// Extract data subheaders from page subheaders.
    ///
    /// In compressed SAS files, data rows are stored as subheaders on META pages.
    /// These pages also contain metadata subheaders (ROW_SIZE, COLUMN_SIZE, etc.)
    /// with the same compression/type flags. We distinguish them by:
    /// 1. Length: compressed data rows have length <= row_length
    /// 2. Signature: known metadata subheaders have recognizable 4-byte signatures
    fn extract_data_subheaders(&self, subheaders: &[PageSubheader]) -> Result<Vec<DataSubheader>> {
        let mut data_subheaders = Vec::new();
        let page_buffer = self.page_reader.page_buffer();
        let row_length = self.metadata.row_length;

        for subheader in subheaders {
            if self.metadata.compression == Compression::None {
                continue;
            }
            // Data subheaders have compression == 4 or 0, and type == 1
            if !((subheader.compression == 4 || subheader.compression == 0)
                && subheader.subheader_type == 1)
            {
                continue;
            }

            // A compressed or uncompressed data row can never be longer than row_length.
            // Metadata subheaders (ROW_SIZE, COLUMN_TEXT, etc.) are typically much larger.
            if subheader.length > row_length {
                continue;
            }

            // Check against known metadata signatures. In compressed SAS files,
            // metadata and data subheaders share the same compression/type flags,
            // so we must check the actual content to distinguish them.
            if subheader.length >= 4 && subheader.offset + 8 <= page_buffer.len() {
                let sig8 = &page_buffer[subheader.offset..subheader.offset + 8];
                if is_known_metadata_signature(sig8) {
                    continue;
                }
                // Skip the problematic [00, FC, FF, FF] pattern found in some compressed files
                // This appears to be metadata/padding, not actual data
                if sig8.len() >= 4
                    && sig8[0] == 0x00
                    && sig8[1] == 0xFC
                    && sig8[2] == 0xFF
                    && sig8[3] == 0xFF
                {
                    continue;
                }
                // Skip [FF, FF, FC, 00] pattern - another metadata variant seen in test12
                if sig8.len() >= 4
                    && sig8[0] == 0xFF
                    && sig8[1] == 0xFF
                    && sig8[2] == 0xFC
                    && sig8[3] == 0x00
                {
                    continue;
                }
            } else if subheader.length >= 4 && subheader.offset + 4 <= page_buffer.len() {
                let sig4 = &page_buffer[subheader.offset..subheader.offset + 4];
                if is_known_metadata_signature(sig4) {
                    continue;
                }
                // Skip the problematic [00, FC, FF, FF] pattern
                if sig4.len() >= 4
                    && sig4[0] == 0x00
                    && sig4[1] == 0xFC
                    && sig4[2] == 0xFF
                    && sig4[3] == 0xFF
                {
                    continue;
                }
                // Skip [FF, FF, FC, 00] pattern
                if sig4.len() >= 4
                    && sig4[0] == 0xFF
                    && sig4[1] == 0xFF
                    && sig4[2] == 0xFC
                    && sig4[3] == 0x00
                {
                    continue;
                }
            }

            data_subheaders.push(DataSubheader {
                offset: subheader.offset,
                length: subheader.length,
                compression: subheader.compression,
            });
        }

        Ok(data_subheaders)
    }

    /// Extract row bytes from page buffer
    fn extract_row_bytes(
        &mut self,
        offset: usize,
        length: usize,
        _compression: u8,
    ) -> Result<RowBytes> {
        let page_buffer = self.page_reader.page_buffer();

        // Validate bounds
        if offset + length > page_buffer.len() {
            return Err(Error::BufferOutOfBounds { offset, length });
        }

        let raw_bytes = &page_buffer[offset..offset + length];

        // Match C++ behavior: decompress whenever length < row_length,
        // regardless of the compression flag. The compression flag on the
        // subheader isn't a reliable indicator; length comparison is.
        if length < self.metadata.row_length {
            self.decompressor
                .decompress(raw_bytes, self.metadata.row_length)
        } else {
            Ok(raw_bytes.to_vec())
        }
    }
}

fn is_stat_transfer_release(release: &str) -> bool {
    let bytes = release.as_bytes();
    if bytes.len() < 8 {
        return false;
    }
    let major = bytes[0] as char;
    if major != '8' && major != '9' {
        return false;
    }
    // Expected pattern: X.YYYYMZ (e.g., 9.0202M0)
    if bytes[1] != b'.' || bytes[6] != b'M' {
        return false;
    }
    let minor = std::str::from_utf8(&bytes[2..6])
        .ok()
        .and_then(|s| s.parse::<u32>().ok());
    let revision = bytes
        .get(7)
        .copied()
        .filter(|b| b.is_ascii_digit())
        .map(|b| (b - b'0') as u32);
    matches!((minor, revision), (Some(0), Some(0)))
}

/// Check if an 8-byte signature matches any known metadata subheader type.
/// This covers all known 64-bit and 32-bit signature patterns.
fn is_known_metadata_signature(sig: &[u8]) -> bool {
    if sig.len() < 4 {
        return false;
    }
    // 4-byte signatures (32-bit format, or first 4 bytes of 64-bit)
    let sig4 = &sig[..4];
    if matches!(
        sig4,
        [0xF7, 0xF7, 0xF7, 0xF7]
            | [0xF6, 0xF6, 0xF6, 0xF6]
            | [0xFD, 0xFF, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFF, 0xFD]
            | [0xFF, 0xFF, 0xFF, 0xFF]
            | [0xFC, 0xFF, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFF, 0xFC]
            | [0xFE, 0xFB, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFB, 0xFE]
            | [0xFE, 0xFF, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFF, 0xFE]
    ) {
        return true;
    }
    // 64-bit signatures with zero-padded first half
    if sig.len() >= 8 && sig4 == [0x00, 0x00, 0x00, 0x00] {
        let sig4_hi = &sig[4..8];
        if matches!(
            sig4_hi,
            [0xF7, 0xF7, 0xF7, 0xF7]
                | [0xF6, 0xF6, 0xF6, 0xF6]
                | [0xFD, 0xFF, 0xFF, 0xFF]
                | [0xFC, 0xFF, 0xFF, 0xFF]
                | [0xFE, 0xFB, 0xFF, 0xFF]
                | [0xFE, 0xFF, 0xFF, 0xFF]
        ) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::DataReader;
    use crate::page::PageReader;
    use crate::reader::Sas7bdatReader;
    use std::fs::File;
    use std::io::{BufReader, Seek, SeekFrom};

    #[test]
    #[ignore]
    fn inspect_sas_missing_bits() {
        let path = "tests/sas/data/info_nulls_test_data.sas7bdat";
        if !std::path::Path::new(path).exists() {
            return;
        }
        let reader = Sas7bdatReader::open(path).expect("open");
        let meta = reader.metadata();
        let header = reader.header();
        let endian = reader.endian();
        let format = reader.format();
        let initial_subs = reader.initial_data_subheaders().to_vec();

        println!("Row count: {}", meta.row_count);
        println!("Endian: {:?}", endian);
        println!("Row length: {}", meta.row_length);
        for col in &meta.columns {
            println!(
                "  col name={:?} type={:?} format={:?} offset={} len={}",
                col.name, col.col_type, col.format, col.offset, col.length
            );
        }

        // Collect all distinct NaN bit patterns across ALL rows
        let mut file = BufReader::new(File::open(path).expect("open file"));
        file.seek(SeekFrom::Start(header.header_length as u64))
            .unwrap();
        let page_reader = PageReader::new(file, header.clone(), endian, format);
        let mut data_reader =
            DataReader::new(page_reader, meta.clone(), endian, format, initial_subs)
                .expect("data reader");

        use std::collections::BTreeSet;
        let mut nan_patterns: BTreeSet<u64> = BTreeSet::new();
        let mut valid_count = 0usize;

        loop {
            match data_reader.read_row_borrowed() {
                Ok(Some(row)) => {
                    for col in &meta.columns {
                        if col.col_type != crate::types::ColumnType::Numeric {
                            continue;
                        }
                        let start = col.offset;
                        let end = col.offset + col.length;
                        if end > row.len() {
                            continue;
                        }
                        let bytes = &row[start..end];
                        let bits = if bytes.len() >= 8 {
                            u64::from_le_bytes(bytes[..8].try_into().unwrap())
                        } else {
                            let mut buf = [0u8; 8];
                            let pad = 8 - bytes.len();
                            buf[pad..].copy_from_slice(bytes);
                            u64::from_le_bytes(buf)
                        };
                        let abs = bits & 0x7fff_ffff_ffff_ffff;
                        if abs >= 0x7ff0_0000_0000_0000 {
                            nan_patterns.insert(bits);
                        } else {
                            valid_count += 1;
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    println!("err: {}", e);
                    break;
                }
            }
        }

        println!("Valid values: {}", valid_count);
        println!("Distinct NaN patterns ({}):", nan_patterns.len());
        let mut sorted: Vec<u64> = nan_patterns.into_iter().collect();
        sorted.sort();
        for bits in &sorted {
            let be_bytes = bits.to_be_bytes();
            let sign = (bits >> 63) & 1;
            let type_byte = (bits >> 40) & 0xff;
            let as_char = if type_byte >= 0x41 && type_byte <= 0x7a {
                (type_byte as u8) as char
            } else {
                '?'
            };
            println!(
                "  bits=0x{:016x} sign={} be_bytes={} type_byte=0x{:02x}={} char={}",
                bits,
                sign,
                be_bytes
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join(""),
                type_byte,
                type_byte,
                as_char
            );
        }
    }
}


fn debug_log(msg: &str) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("sas_debug.txt")
        .unwrap();
    writeln!(f, "{}", msg).unwrap();
}
