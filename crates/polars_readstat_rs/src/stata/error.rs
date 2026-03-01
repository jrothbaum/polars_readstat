use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid magic number in file header")]
    InvalidMagicNumber,

    #[error("Unsupported format/version: {0}")]
    UnsupportedFormat(String),

    #[error("Header too short: expected at least {expected} bytes, got {actual}")]
    HeaderTooShort { expected: usize, actual: usize },

    #[error("Invalid type code: {0}")]
    InvalidTypeCode(u16),

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

    #[error("Unsupported type for Stata writer: {0}")]
    UnsupportedType(String),

    #[error("Invalid Stata variable name: {0}")]
    InvalidName(String),

    #[error("String value is too long for Stata: {0}")]
    StringTooLong(String),

    #[error("Numeric value is out of range for Stata")]
    NumericOutOfRange,
}

pub type Result<T> = std::result::Result<T, Error>;
