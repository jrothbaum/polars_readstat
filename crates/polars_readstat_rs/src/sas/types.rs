use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    Big,
    Little,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Bit32,
    Bit64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Rle, // SASYZCRL
    Rdc, // SASYZCR2
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Unix,
    Windows,
    Unknown,
}

/// What a page holds. The page-type field's high byte is the kind; its low byte carries
/// flags, such as [`PAGE_FLAG_DELETED_ROWS`] (so 0x0180 is a DATA page and 0x0280 a MIX page,
/// each with some rows deleted). Classified as SAS and ReadStat do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// 0x0000: subheaders (metadata, and rows in compressed files)
    Meta,
    /// 0x0100: a block of rows
    Data,
    /// 0x0200: subheaders followed by a block of rows
    Mix,
    /// 0x0400: subheaders added after the data ("amendment")
    Amd,
    /// 0x4000: more subheaders; compressed files can continue their rows here
    Meta2,
    /// 0x9000 / 0x8000: compression and index pages, which hold no rows
    Comp,
    /// Any other kind: searched for subheaders, as ReadStat does
    Other,
}

/// Page-type flag: some rows on this DATA/MIX page are deleted; a bitmap after the rows
/// marks which.
pub const PAGE_FLAG_DELETED_ROWS: u16 = 0x0080;

impl PageType {
    pub fn from_u16(value: u16) -> Self {
        if value & 0x0F00 == 0x0100 {
            return Self::Data;
        }
        if value & 0x9000 != 0 {
            return Self::Comp;
        }
        match value & 0x0F00 {
            0x0200 => Self::Mix,
            0x0400 => Self::Amd,
            0x0000 if value & 0x4000 != 0 => Self::Meta2,
            0x0000 => Self::Meta,
            _ => Self::Other,
        }
    }

    /// Pages carrying a subheader table: everything but DATA and COMP pages.
    pub fn has_subheaders(self) -> bool {
        !matches!(self, Self::Data | Self::Comp)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColumnType {
    #[default]
    Numeric,
    Character,
}

/// Header information from the SAS7BDAT file
#[derive(Debug, Clone)]
pub struct Header {
    pub format: Format,
    pub endian: Endian,
    pub platform: Platform,
    pub date_created: DateTime<Utc>,
    pub date_modified: DateTime<Utc>,
    pub header_length: usize,
    pub page_length: usize,
    pub page_count: usize,
    pub dataset_name: String,
    pub file_type: String,
    pub sas_release: String,
    pub sas_server_type: String,
    pub os_type: String,
    pub os_name: String,
    pub encoding_byte: u8,
}

/// Column metadata
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub format: String,
    pub col_type: ColumnType,
    pub offset: usize,
    pub length: usize,
}

/// Page index entry for fast seeking
#[derive(Debug, Clone)]
pub struct PageIndex {
    pub page_number: usize,
    pub row_start: usize,
    pub row_count: usize,
}

/// File metadata extracted from metadata pages
#[derive(Debug, Clone)]
pub struct Metadata {
    pub compression: Compression,
    /// Rows in the dataset: rows in the file less those SAS marked deleted.
    pub row_count: usize,
    /// Rows physically in the file, deleted ones included.
    pub total_row_count: usize,
    /// Rows marked deleted, which are skipped.
    pub deleted_row_count: usize,
    pub row_length: usize,
    pub mix_page_row_count: usize,
    pub column_count: usize,
    pub columns: Vec<Column>,
    pub metadata_df: polars::prelude::DataFrame,
    pub creator: String,
    pub creator_proc: String,
    pub encoding_byte: u8,
    pub page_index: Vec<PageIndex>,
}
