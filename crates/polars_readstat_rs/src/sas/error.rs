use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid magic number in file header")]
    InvalidMagicNumber,

    #[error("Header too short: expected at least {expected} bytes, got {actual}")]
    HeaderTooShort { expected: usize, actual: usize },

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("Decompression failed: {0}")]
    DecompressionError(String),

    #[error("Invalid RLE command: 0x{0:02X}")]
    InvalidRleCommand(u8),

    #[error("Invalid RDC command: 0x{0:02X}")]
    InvalidRdcCommand(u8),

    #[error("Invalid page type: {0}")]
    InvalidPageType(u16),

    #[error("Buffer access out of bounds: offset={offset}, length={length}")]
    BufferOutOfBounds { offset: usize, length: usize },

    #[error("Cannot read page")]
    CannotReadPage,

    #[error("Column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },

    #[error("End of file reached")]
    EndOfFile,

    #[error("Invalid subheader signature")]
    InvalidSubheaderSignature,

    #[error("Missing required metadata")]
    MissingMetadata,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Arrow conversion error: {0}")]
    ConversionError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
