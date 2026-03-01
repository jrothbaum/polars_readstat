// Decompressor module
mod rdc;
mod rle;

pub use rdc::RdcDecompressor;
pub use rle::RleDecompressor;

use crate::error::Result;
use crate::types::Compression;

pub enum Decompressor {
    None,
    Rle(RleDecompressor),
    Rdc(RdcDecompressor),
}

impl Decompressor {
    pub fn new(compression: Compression) -> Self {
        match compression {
            Compression::None => Self::None,
            Compression::Rle => Self::Rle(RleDecompressor::new()),
            Compression::Rdc => Self::Rdc(RdcDecompressor::new()),
        }
    }

    pub fn decompress(&mut self, input: &[u8], expected_output_size: usize) -> Result<Vec<u8>> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Rle(d) => d.decompress(input, expected_output_size),
            Self::Rdc(d) => d.decompress(input, expected_output_size),
        }
    }

    /// Decompress into a pre-allocated buffer, avoiding allocation.
    /// The buffer must be at least `expected_output_size` bytes.
    pub fn decompress_into(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::None => {
                output[..input.len()].copy_from_slice(input);
                for b in &mut output[input.len()..] {
                    *b = 0;
                }
                Ok(())
            }
            Self::Rle(d) => d.decompress_into(input, output),
            Self::Rdc(d) => d.decompress_into(input, output),
        }
    }
}
