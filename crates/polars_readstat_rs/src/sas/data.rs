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
    /// The page a moved row was last read from, and its index.
    moved_page: Vec<u8>,
    moved_page_index: Option<usize>,
    /// When Some(n), stop reading after n more data-bearing pages (page-limited parallel mode).
    /// In this mode the current_row >= metadata.row_count guard is bypassed; the
    /// page budget is the sole stopping condition.
    remaining_pages: Option<usize>,
    /// When Some(n), stop reading after n more physical pages regardless of data content.
    /// Used for compressed parallel reads where non-data META pages must still count
    /// against the worker's page-range budget to prevent overlap with adjacent workers.
    max_physical_pages: Option<usize>,
    /// Total physical pages read so far (including during construction before the budget is set).
    /// Used by data_reader_at_page_range to correctly compute the remaining page budget.
    physical_pages_consumed: usize,
}

/// State for tracking position within a page. `current_index` counts the page's live rows:
/// deleted rows are never visited.
#[derive(Debug)]
enum PageState {
    /// Page whose rows are subheaders (compressed files)
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
        /// On a page flagged as having deleted rows: the live rows' physical indices.
        live: Option<Vec<u32>>,
    },
    /// MIX page with metadata and data
    Mix {
        row_count: usize,
        offset: usize,
        row_length: usize,
        current_index: usize,
        /// On a page flagged as having deleted rows: the live rows' physical indices.
        live: Option<Vec<u32>>,
    },
}

/// A row stored as a subheader (compressed files).
#[derive(Debug, Clone)]
pub struct DataSubheader {
    pub offset: usize,
    pub length: usize,
    pub compression: u8, // 4 = compressed, 0 = uncompressed
    /// Set when SAS moved the row: it is read from this page's subheader instead, at this
    /// row's place in the file.
    pub moved_to: Option<MovedRow>,
}

/// Where a moved row now lives: a page index and a subheader index on that page.
#[derive(Debug, Clone, Copy)]
pub struct MovedRow {
    pub page: usize,
    pub subheader: usize,
}

/// Where the current row's bytes are.
#[derive(Debug, Clone, Copy)]
enum RowLocation {
    /// In the current page buffer
    Here { offset: usize, length: usize },
    /// In a subheader of another page
    Moved(MovedRow),
}

// Subheader-pointer compression codes (SAS_COMPRESSION_* in ReadStat).
const SH_NONE: u8 = 0x00;
const SH_TRUNCATED: u8 = 0x01;
const SH_NONE_MOVED: u8 = 0x02;
const SH_REFERENCE: u8 = 0x03;
const SH_ROW: u8 = 0x04;
const SH_DELETED_ROW: u8 = 0x05;
const SH_ROW_MOVED: u8 = 0x06;
const SH_NONE_UNREFERENCED: u8 = 0x09;
const SH_ROW_UNREFERENCED: u8 = 0x0D;

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
            moved_page: Vec::new(),
            moved_page_index: None,
            remaining_pages: None,
            max_physical_pages: None,
            physical_pages_consumed: 0,
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

    /// Limit reading to the next `n` physical pages regardless of data content.
    /// Use this for compressed parallel reads so that non-data META pages still
    /// consume the worker's page-range budget, preventing overlap with adjacent workers.
    pub fn set_max_physical_pages(&mut self, n: usize) {
        self.max_physical_pages = Some(n);
    }

    /// Set the current row counter (for direct-seek parallel reads).
    /// Call this after constructing a DataReader that was seeked to a specific page,
    /// so that the row counter correctly reflects the logical position.
    pub fn set_current_row(&mut self, row: usize) {
        self.current_row = row;
    }

    /// Drop the page loaded during construction, so reading resumes at the next page
    /// (subject to the page budgets).
    pub fn discard_current_page(&mut self) {
        self.page_state = None;
    }

    /// Total physical pages read, including any consumed during construction before the
    /// page budget was set. Used to correctly compute the remaining budget in
    /// data_reader_at_page_range.
    pub fn physical_pages_consumed(&self) -> usize {
        self.physical_pages_consumed
    }

    /// Read the next row (allocating). Used by streaming paths that need owned data.
    pub fn read_row(&mut self) -> Result<Option<RowBytes>> {
        Ok(self.read_row_borrowed()?.map(|row| row.to_vec()))
    }

    /// Ensure we're positioned on a valid row, advancing pages if needed.
    /// Returns its location, or None if no more rows.
    fn ensure_row_ready(&mut self) -> Result<Option<RowLocation>> {
        if self.current_row >= self.metadata.row_count {
            return Ok(None);
        }
        match self.get_current_row_info() {
            Some(loc) => Ok(Some(loc)),
            None => {
                self.advance_page()?;
                Ok(self.get_current_row_info())
            }
        }
    }

    /// Fill `decompress_buf` with the row at `loc` (compressed files).
    fn load_subheader_row(&mut self, loc: RowLocation) -> Result<()> {
        let row_length = self.metadata.row_length;
        let (page, offset, length): (&[u8], usize, usize) = match loc {
            RowLocation::Here { offset, length } => (self.page_reader.page_buffer(), offset, length),
            RowLocation::Moved(moved) => {
                if self.moved_page_index != Some(moved.page) {
                    self.moved_page_index = None;
                    self.page_reader.read_page_at(moved.page, &mut self.moved_page)?;
                    self.moved_page_index = Some(moved.page);
                }
                let target = self
                    .page_reader
                    .subheader_pointer(&self.moved_page, moved.subheader)?;
                if !matches!(target.compression, SH_NONE | SH_NONE_MOVED | SH_ROW | SH_ROW_MOVED)
                    || target.length == 0
                    || target.length > row_length
                {
                    return Err(Error::ParseError(format!(
                        "moved row points to page {} subheader {}, which holds no row",
                        moved.page, moved.subheader
                    )));
                }
                (&self.moved_page, target.offset, target.length)
            }
        };
        if offset + length > page.len() {
            return Err(Error::BufferOutOfBounds { offset, length });
        }
        let raw_bytes = &page[offset..offset + length];
        if length < row_length {
            // Truly compressed subheader: decompress into the reusable buffer.
            self.decompressor
                .decompress_into(raw_bytes, &mut self.decompress_buf)?;
        } else {
            // Uncompressed subheader on a compressed file: copy bytes directly.
            self.decompress_buf[..length].copy_from_slice(raw_bytes);
        }
        Ok(())
    }

    /// Read the next row without allocating. Returns a reference to either the
    /// page buffer (uncompressed) or the internal decompress buffer (compressed).
    /// The returned slice is valid until the next call to any read method.
    pub fn read_row_borrowed(&mut self) -> Result<Option<&[u8]>> {
        let loc = match self.ensure_row_ready()? {
            Some(v) => v,
            None => return Ok(None),
        };

        if self.metadata.compression != Compression::None {
            self.load_subheader_row(loc)?;
            self.advance_row();
            return Ok(Some(&self.decompress_buf));
        }
        let RowLocation::Here { offset, length } = loc else {
            return Err(Error::ParseError("moved row in an uncompressed file".into()));
        };
        if offset + length > self.page_reader.page_buffer().len() {
            return Err(Error::BufferOutOfBounds { offset, length });
        }
        self.advance_row();
        Ok(Some(&self.page_reader.page_buffer()[offset..offset + length]))
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

            // Slow path: per-row copy (MIX/META pages, pages with deleted rows, compressed).
            let loc = match self.ensure_row_ready()? {
                Some(v) => v,
                None => break,
            };

            if self.metadata.compression != Compression::None {
                self.load_subheader_row(loc)?;
                buf.extend_from_slice(&self.decompress_buf[..row_length]);
            } else {
                let RowLocation::Here { offset, length } = loc else {
                    return Err(Error::ParseError("moved row in an uncompressed file".into()));
                };
                if offset + length > self.page_reader.page_buffer().len() {
                    return Err(Error::BufferOutOfBounds { offset, length });
                }
                buf.extend_from_slice(&self.page_reader.page_buffer()[offset..offset + length]);
            }
            self.advance_row();
            count += 1;
        }

        Ok(count)
    }

    /// Fast-path bulk copy for uncompressed DATA pages without deleted rows.
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
                live: None,
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

    /// Rows (or, on a block page, physical row slots) that fit in the page from `offset`.
    fn rows_that_fit(&self, offset: usize, row_length: usize) -> usize {
        let page_size = self.page_reader.header().page_length;
        if row_length == 0 || offset >= page_size {
            return 0;
        }
        (page_size - offset) / row_length
    }

    /// Live rows left on the current page.
    fn rows_remaining_in_page(&self) -> usize {
        match &self.page_state {
            Some(PageState::Meta {
                data_subheaders,
                current_index,
            }) => data_subheaders.len().saturating_sub(*current_index),
            Some(PageState::Data {
                block_count: count,
                offset,
                row_length,
                current_index,
                live,
            })
            | Some(PageState::Mix {
                row_count: count,
                offset,
                row_length,
                current_index,
                live,
            }) => {
                let valid_rows = match live {
                    Some(live) => live.len(),
                    None => (*count).min(self.rows_that_fit(*offset, *row_length)),
                };
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

    /// Location of the current row, or None at the end of the page.
    fn get_current_row_info(&self) -> Option<RowLocation> {
        match &self.page_state {
            Some(PageState::Meta {
                data_subheaders,
                current_index,
            }) => {
                let sub = data_subheaders.get(*current_index)?;
                Some(match sub.moved_to {
                    Some(moved) => RowLocation::Moved(moved),
                    None => RowLocation::Here {
                        offset: sub.offset,
                        length: sub.length,
                    },
                })
            }
            Some(PageState::Data {
                block_count: count,
                offset,
                row_length,
                current_index,
                live,
            })
            | Some(PageState::Mix {
                row_count: count,
                offset,
                row_length,
                current_index,
                live,
            }) => {
                let physical = match live {
                    Some(live) => *live.get(*current_index)? as usize,
                    None if *current_index < *count => *current_index,
                    None => return None,
                };
                let row_offset = offset + row_length * physical;
                // Stop at a row that would run past the end of the page.
                if row_offset + row_length <= self.page_reader.header().page_length {
                    Some(RowLocation::Here {
                        offset: row_offset,
                        length: *row_length,
                    })
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Advance to next row within current page
    fn advance_row(&mut self) {
        self.advance_rows(1);
    }

    /// Advance to next page
    fn advance_page(&mut self) -> Result<()> {
        if let Some(0) = self.remaining_pages {
            self.page_state = None;
            return Ok(());
        }
        loop {
            // Physical page budget: counts every page read, not just data-bearing ones.
            // This prevents compressed workers from straying into adjacent workers' ranges.
            if let Some(0) = self.max_physical_pages {
                self.page_state = None;
                return Ok(());
            }

            if !self.page_reader.read_page()? {
                return Ok(());
            }

            self.physical_pages_consumed += 1;

            if let Some(ref mut rem) = self.max_physical_pages {
                *rem = rem.saturating_sub(1);
            }

            let page_header = self.page_reader.get_page_header()?;

            if let Some(page_state) = self.build_page_state(&page_header)? {
                if let Some(ref mut rem) = self.remaining_pages {
                    *rem = rem.saturating_sub(1);
                }
                self.page_state = Some(page_state);
                return Ok(());
            }
            // Non-data page: doesn't count against the data-bearing budget, continue
        }
    }

    /// For a page flagged as having deleted rows, the physical indices of its live rows.
    fn live_rows(
        &self,
        page_header: &PageHeader,
        offset: usize,
        page_row_count: usize,
    ) -> Result<Option<Vec<u32>>> {
        if !page_header.has_deleted_rows {
            return Ok(None);
        }
        let row_length = self.metadata.row_length;
        let bitmap_rows = page_row_count.min(self.metadata.total_row_count);
        let rows = bitmap_rows.min(self.rows_that_fit(offset, row_length));
        let header = self.page_reader.header();
        live_rows_on_flagged_page(
            self.page_reader.page_buffer(),
            header.format,
            header.endian,
            offset,
            bitmap_rows,
            row_length,
            rows,
        )
        .map(Some)
    }

    /// Build page state based on page type
    fn build_page_state(&mut self, page_header: &PageHeader) -> Result<Option<PageState>> {
        let format = self.page_reader.header().format;
        let page_bit_offset = match format {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };

        match page_header.page_type {
            PageType::Data => {
                // DATA page: rows start at page_bit_offset + 8
                let offset = page_bit_offset + 8;
                let block_count = page_header.block_count as usize;
                let live = self.live_rows(page_header, offset, block_count)?;
                Ok(Some(PageState::Data {
                    block_count,
                    offset,
                    row_length: self.metadata.row_length,
                    current_index: 0,
                    live,
                }))
            }
            page_type if page_type.has_subheaders() => {
                // Rows stored as subheaders (compressed files), on any subheader page.
                let subheaders = self.page_reader.get_subheaders(page_header)?;
                let data_subheaders = self.extract_data_subheaders(&subheaders)?;

                if !data_subheaders.is_empty() {
                    return Ok(Some(PageState::Meta {
                        data_subheaders,
                        current_index: 0,
                    }));
                }

                // A MIX page with no row subheaders holds a block of rows after its table.
                if page_type == PageType::Mix {
                    let integer_size = match format {
                        Format::Bit64 => 8,
                        Format::Bit32 => 4,
                    };
                    let offset = mix_page_data_start(
                        self.page_reader.page_buffer(),
                        self.page_reader.header().sas_release.as_str(),
                        page_bit_offset,
                        page_header.subheader_count as usize,
                        integer_size,
                    );

                    let row_count = self
                        .metadata
                        .total_row_count
                        .min(self.metadata.mix_page_row_count);
                    let live =
                        self.live_rows(page_header, offset, self.metadata.mix_page_row_count)?;

                    return Ok(Some(PageState::Mix {
                        row_count,
                        offset,
                        row_length: self.metadata.row_length,
                        current_index: 0,
                        live,
                    }));
                }

                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// The rows stored as subheaders on this page, in file order (compressed files).
    ///
    /// Each subheader pointer's compression code says what it holds (ReadStat's
    /// SAS_COMPRESSION_* codes): a row (compressed 4, or stored as is 0), a deleted row (5,
    /// skipped), a reference to a row SAS moved to another page (3, read from there in this
    /// row's place), or the moved row itself (2, 6, 9, 13; skipped where it lies, as it is
    /// read through its reference). Code 0 is shared with metadata subheaders, so rows among
    /// them are told apart by their length and signature.
    fn extract_data_subheaders(&self, subheaders: &[PageSubheader]) -> Result<Vec<DataSubheader>> {
        let mut data_subheaders = Vec::new();
        if self.metadata.compression == Compression::None {
            return Ok(data_subheaders);
        }
        let page_buffer = self.page_reader.page_buffer();
        let row_length = self.metadata.row_length;

        for subheader in subheaders {
            match subheader.compression {
                SH_NONE | SH_ROW => {}
                SH_TRUNCATED | SH_DELETED_ROW | SH_NONE_MOVED | SH_ROW_MOVED
                | SH_NONE_UNREFERENCED | SH_ROW_UNREFERENCED => continue,
                SH_REFERENCE => {
                    // offset holds the page index + 1, length the subheader index + 1
                    if subheader.offset == 0 || subheader.length == 0 {
                        return Err(Error::ParseError(
                            "moved-row reference with no page or subheader".into(),
                        ));
                    }
                    data_subheaders.push(DataSubheader {
                        offset: 0,
                        length: 0,
                        compression: subheader.compression,
                        moved_to: Some(MovedRow {
                            page: subheader.offset - 1,
                            subheader: subheader.length - 1,
                        }),
                    });
                    continue;
                }
                code => {
                    return Err(Error::ParseError(format!(
                        "unsupported subheader compression code {code:#04x}"
                    )));
                }
            }
            // Data subheaders have type == 1
            if subheader.subheader_type != 1 {
                continue;
            }

            // A compressed row (4) is never longer than row_length, and a row stored as is (0)
            // is exactly row_length, as ReadStat requires; metadata subheaders, which share
            // code 0 and type 1 (AMD pages carry 0xFFFFFFFA ones), are told apart this way.
            if subheader.length > row_length
                || (subheader.compression == SH_NONE && subheader.length != row_length)
            {
                continue;
            }

            // Check against known metadata signatures. In compressed SAS files,
            // metadata and data subheaders share the same compression/type flags,
            // so we must check the actual content to distinguish them.
            let sig_len = if subheader.offset + 8 <= page_buffer.len() {
                8
            } else {
                4
            };
            if subheader.length >= 4 && subheader.offset + sig_len <= page_buffer.len() {
                let sig = &page_buffer[subheader.offset..subheader.offset + sig_len];
                if is_known_metadata_signature(sig) {
                    continue;
                }
                // Skip the [00, FC, FF, FF] and [FF, FF, FC, 00] patterns found in some
                // compressed files (metadata/padding, not rows; seen in test12)
                if sig[..4] == [0x00, 0xFC, 0xFF, 0xFF] || sig[..4] == [0xFF, 0xFF, 0xFC, 0x00] {
                    continue;
                }
            }

            data_subheaders.push(DataSubheader {
                offset: subheader.offset,
                length: subheader.length,
                compression: subheader.compression,
                moved_to: None,
            });
        }

        Ok(data_subheaders)
    }
}

/// Indices of the rows on a DATA or MIX page whose page type carries the deleted-rows flag
/// that are not deleted.
///
/// SAS marks deleted rows in a bitmap after the page's row block: it starts
/// `row_count * row_length` bytes after the first row, plus the page's unused-bytes count
/// (page offset 24 in 64-bit files, 12 in 32-bit ones), one bit per row, most significant bit
/// first. `row_count` is the page's row count (the page header's for DATA pages, the
/// ROW_SIZE subheader's for MIX pages), capped at the file's total rows, as ReadStat does.
/// `rows` (at most `row_count`) is how many rows to report on.
pub(crate) fn live_rows_on_flagged_page(
    page: &[u8],
    format: Format,
    endian: Endian,
    data_start: usize,
    row_count: usize,
    row_length: usize,
    rows: usize,
) -> Result<Vec<u32>> {
    let (at, n) = match format {
        Format::Bit64 => (24usize, 8usize),
        Format::Bit32 => (12usize, 4usize),
    };
    let field = page
        .get(at..at + n)
        .ok_or(Error::BufferOutOfBounds { offset: at, length: n })?;
    let mut unused = 0u64;
    for k in 0..n {
        let byte = match endian {
            Endian::Little => field[n - 1 - k],
            Endian::Big => field[k],
        };
        unused = (unused << 8) | byte as u64;
    }
    let bitmap_start = (data_start as u64)
        .checked_add(row_count as u64 * row_length as u64)
        .and_then(|v| v.checked_add(unused))
        .filter(|&v| v <= page.len() as u64)
        .ok_or_else(|| Error::ParseError("deleted-row bitmap lies outside its page".into()))?
        as usize;
    let bitmap = page
        .get(bitmap_start..bitmap_start + row_count.div_ceil(8))
        .ok_or_else(|| Error::ParseError("deleted-row bitmap lies outside its page".into()))?;
    Ok((0..rows.min(row_count))
        .filter(|&i| bitmap[i / 8] & (0x80 >> (i % 8)) == 0)
        .map(|i| i as u32)
        .collect())
}

pub(crate) fn is_stat_transfer_release(release: &str) -> bool {
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

/// Compute the byte offset where row data begins on a MIX page.
///
/// MIX pages store rows immediately after the subheader pointer table, but
/// that table isn't always 8-byte aligned. Matches ReadStat's alignment
/// behavior:
/// - For non-Stat/Transfer files, always skip a 4-byte pad when the computed
///   offset is 4-byte misaligned.
/// - For Stat/Transfer-produced files, skip the pad only if it's actually
///   zero- or space-filled (Stat/Transfer sometimes packs real data there).
///
/// Shared by `data.rs` (actual row extraction) and `metadata.rs` (row
/// counting / fast-stop detection during metadata scanning) — previously
/// these computed this offset independently and had drifted out of sync for
/// the Stat/Transfer case.
pub(crate) fn mix_page_data_start(
    page_buffer: &[u8],
    sas_release: &str,
    page_bit_offset: usize,
    subheader_count: usize,
    integer_size: usize,
) -> usize {
    let subheader_size = 3 * integer_size;
    let mut offset = page_bit_offset + 8 + subheader_count * subheader_size;

    if offset % 8 == 4 {
        if let Some(pad) = page_buffer.get(offset..offset + 4) {
            let vendor_is_stat_transfer = is_stat_transfer_release(sas_release);
            let pad_is_zero_or_space = pad == [0, 0, 0, 0] || pad == [b' ', b' ', b' ', b' '];
            if !vendor_is_stat_transfer || pad_is_zero_or_space {
                offset += 4;
            }
        }
    }

    offset
}

/// Check if an 8-byte signature matches any known metadata subheader type.
/// This covers all known 64-bit and 32-bit signature patterns.
///
/// Shared between `data.rs` (distinguishing compressed data-row subheaders
/// from metadata subheaders while reading) and `metadata.rs` (the same
/// distinction while fast-scanning for the first data page) — keep a single
/// copy so the two passes can't drift out of sync on which subheaders count
/// as data.
pub(crate) fn is_known_metadata_signature(sig: &[u8]) -> bool {
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
