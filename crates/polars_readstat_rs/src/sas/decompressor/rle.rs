// RLE (SASYZCRL) decompression
use crate::error::{Error, Result};

// RLE command constants (from ReadStat)
const COPY64: u8 = 0x00;
const COPY64_PLUS_4096: u8 = 0x01;
const COPY96: u8 = 0x02;
const INSERT_BYTE18: u8 = 0x04;
const INSERT_AT17: u8 = 0x05;
const INSERT_BLANK17: u8 = 0x06;
const INSERT_ZERO17: u8 = 0x07;
const COPY1: u8 = 0x08;
const COPY17: u8 = 0x09;
const COPY33: u8 = 0x0A;
const COPY49: u8 = 0x0B;
const INSERT_BYTE3: u8 = 0x0C;
const INSERT_AT2: u8 = 0x0D;
const INSERT_BLANK2: u8 = 0x0E;
const INSERT_ZERO2: u8 = 0x0F;

// Special characters
const C_NULL: u8 = 0x00;
const C_SPACE: u8 = 0x20;
const C_AT: u8 = 0x40;

pub struct RleDecompressor {}

impl RleDecompressor {
    pub fn new() -> Self {
        Self {}
    }

    /// Decompress into a pre-allocated buffer, without allocating.
    pub fn decompress_into(&mut self, input: &[u8], output: &mut [u8]) -> Result<()> {
        // Zero-fill first: unwritten tail bytes (and any stale content from a
        // previously reused buffer) must read back as zero, matching the
        // zero-padding behavior of the allocating `decompress` path.
        output.fill(C_NULL);
        self.decompress_to_slice(input, output)
    }

    pub fn decompress(&mut self, input: &[u8], expected_output_size: usize) -> Result<Vec<u8>> {
        let mut output = vec![C_NULL; expected_output_size];
        self.decompress_to_slice(input, &mut output)?;
        Ok(output)
    }

    /// Core RLE decode, writing into an already zero-filled `output` slice via
    /// a write cursor `pos` (rather than growing a `Vec`).
    fn decompress_to_slice(&mut self, input: &[u8], output: &mut [u8]) -> Result<()> {
        let expected_output_size = output.len();
        let mut pos = 0usize;
        let mut src_pos = 0;

        while src_pos < input.len() && pos < expected_output_size {
            if src_pos >= input.len() {
                break;
            }

            let control_byte = input[src_pos];
            src_pos += 1;

            let command = (control_byte >> 4) & 0x0F;
            let end_of_first_byte = control_byte & 0x0F;

            match command {
                COPY64_PLUS_4096 => {
                    // Copy 64 + (end_of_first_byte * 256) + next_byte + 4096 bytes
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = 64 + (end_of_first_byte as usize * 256) + next_byte + 4096;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                COPY96 => {
                    // Copy end_of_first_byte + 96 bytes
                    let count = end_of_first_byte as usize + 96;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                COPY64 => {
                    // Copy (end_of_first_byte << 8) + next_byte + 64 bytes
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 64;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                INSERT_BYTE18 => {
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;
                    let byte_to_insert = input[src_pos];
                    src_pos += 1;
                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 18; // was << 4
                    Self::safe_fill(output, &mut pos, count, byte_to_insert, expected_output_size);
                }
                INSERT_AT17 => {
                    // Insert (end_of_first_byte << 8) + next_byte + 17 '@' characters
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 17;
                    Self::safe_fill(output, &mut pos, count, C_AT, expected_output_size);
                }
                INSERT_BLANK17 => {
                    // Insert (end_of_first_byte << 8) + next_byte + 17 spaces
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 17;
                    Self::safe_fill(output, &mut pos, count, C_SPACE, expected_output_size);
                }
                INSERT_ZERO17 => {
                    // Insert (end_of_first_byte << 8) + next_byte + 17 nulls
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 17;
                    Self::safe_fill(output, &mut pos, count, C_NULL, expected_output_size);
                }
                COPY1 => {
                    // Copy end_of_first_byte + 1 bytes
                    let count = end_of_first_byte as usize + 1;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                COPY17 => {
                    // Copy end_of_first_byte + 17 bytes
                    let count = end_of_first_byte as usize + 17;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                COPY33 => {
                    // Copy end_of_first_byte + 33 bytes
                    let count = end_of_first_byte as usize + 33;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                COPY49 => {
                    // Copy end_of_first_byte + 49 bytes
                    let count = end_of_first_byte as usize + 49;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        output,
                        &mut pos,
                        count,
                        expected_output_size,
                    )?;
                }
                INSERT_BYTE3 => {
                    // Insert end_of_first_byte + 3 copies of next byte
                    if src_pos >= input.len() {
                        break;
                    }
                    let byte_to_insert = input[src_pos];
                    src_pos += 1;

                    let count = end_of_first_byte as usize + 3;
                    Self::safe_fill(output, &mut pos, count, byte_to_insert, expected_output_size);
                }
                INSERT_AT2 => {
                    // Insert end_of_first_byte + 2 '@' characters
                    let count = end_of_first_byte as usize + 2;
                    Self::safe_fill(output, &mut pos, count, C_AT, expected_output_size);
                }
                INSERT_BLANK2 => {
                    // Insert end_of_first_byte + 2 spaces
                    let count = end_of_first_byte as usize + 2;
                    Self::safe_fill(output, &mut pos, count, C_SPACE, expected_output_size);
                }
                INSERT_ZERO2 => {
                    // Insert end_of_first_byte + 2 nulls
                    let count = end_of_first_byte as usize + 2;
                    Self::safe_fill(output, &mut pos, count, C_NULL, expected_output_size);
                }
                _ => {
                    return Err(Error::InvalidRleCommand(command));
                }
            }
        }

        // `output` was pre-zeroed and copy_bytes/safe_fill already clamp to
        // expected_output_size, so no end-padding or truncation is needed.
        Ok(())
    }

    fn copy_bytes(
        &self,
        input: &[u8],
        src_pos: &mut usize,
        output: &mut [u8],
        pos: &mut usize,
        count: usize,
        max_output_size: usize,
    ) -> Result<()> {
        let remaining = input.len() - *src_pos;
        let space_left = max_output_size.saturating_sub(*pos);
        let to_copy = count.min(remaining).min(space_left);

        if to_copy > 0 {
            output[*pos..*pos + to_copy].copy_from_slice(&input[*src_pos..*src_pos + to_copy]);
            *pos += to_copy;
            *src_pos += to_copy;
        }

        Ok(())
    }

    /// Fill `count` bytes at the write cursor with `value`, clamped to max_output_size.
    fn safe_fill(output: &mut [u8], pos: &mut usize, count: usize, value: u8, max_output_size: usize) {
        let actual_count = count.min(max_output_size.saturating_sub(*pos));
        output[*pos..*pos + actual_count].fill(value);
        *pos += actual_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_copy_simple() {
        let mut decompressor = RleDecompressor::new();

        // COPY1 with count=5: copy 5 bytes
        // command=0x08 (COPY1) in high nibble, end=0x04 in low nibble -> count=5
        let input = vec![(0x08 << 4) | 0x04, b'h', b'e', b'l', b'l', b'o'];
        let output = decompressor.decompress(&input, 5).unwrap();
        assert_eq!(output, b"hello");
    }

    #[test]
    fn test_rle_insert_space() {
        let mut decompressor = RleDecompressor::new();

        // INSERT_BLANK2 with count=5: insert 5 spaces
        // command=0x0E (INSERT_BLANK2) in high nibble, end=0x03 in low nibble -> count=5
        let input = vec![(0x0E << 4) | 0x03];
        let output = decompressor.decompress(&input, 5).unwrap();
        assert_eq!(output, b"     ");
    }

    #[test]
    fn test_rle_insert_byte() {
        let mut decompressor = RleDecompressor::new();

        // INSERT_BYTE3 with count=5: insert 5 copies of 'A'
        // command=0x0C (INSERT_BYTE3) in high nibble, end=0x02 in low nibble -> count=5
        let input = vec![(0x0C << 4) | 0x02, b'A'];
        let output = decompressor.decompress(&input, 5).unwrap();
        assert_eq!(output, b"AAAAA");
    }
}
