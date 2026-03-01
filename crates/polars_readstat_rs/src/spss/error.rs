use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;
