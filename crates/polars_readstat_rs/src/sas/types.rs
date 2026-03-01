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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Meta, // 0
    Data, // 256
    Mix1, // 512
    Mix2, // 640
    Amd,  // 1024
    Metc, // 16384
    Invalid,
}

impl PageType {
    pub fn from_u16(value: u16) -> Self {
        match value {
            0 => Self::Meta,
            256 => Self::Data,
            512 => Self::Mix1,
            640 => Self::Mix2,
            1024 => Self::Amd,
            16384 => Self::Metc,
            _ => Self::Invalid,
        }
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
    pub label: String,
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
    pub row_count: usize,
    pub row_length: usize,
    pub mix_page_row_count: usize,
    pub column_count: usize,
    pub columns: Vec<Column>,
    pub creator: String,
    pub creator_proc: String,
    pub encoding_byte: u8,
    pub page_index: Vec<PageIndex>,
}
