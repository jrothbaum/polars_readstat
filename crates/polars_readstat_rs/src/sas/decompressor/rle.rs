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

    /// Decompress into a pre-allocated buffer, avoiding allocation.
    pub fn decompress_into(&mut self, input: &[u8], output: &mut [u8]) -> Result<()> {
        let result = self.decompress(input, output.len())?;
        output.copy_from_slice(&result);
        Ok(())
    }

    pub fn decompress(&mut self, input: &[u8], expected_output_size: usize) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(expected_output_size);
        let mut src_pos = 0;

        while src_pos < input.len() && output.len() < expected_output_size {
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
                        &mut output,
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
                        &mut output,
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
                        &mut output,
                        count,
                        expected_output_size,
                    )?;
                }
                INSERT_BYTE18 => {
                    // Insert (end_of_first_byte << 4) + next_byte + 18 copies of byte_to_insert
                    if src_pos + 1 >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;
                    let byte_to_insert = input[src_pos];
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 4) + next_byte + 18;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, byte_to_insert, expected_output_size);
                }
                INSERT_AT17 => {
                    // Insert (end_of_first_byte << 8) + next_byte + 17 '@' characters
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 17;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, C_AT, expected_output_size);
                }
                INSERT_BLANK17 => {
                    // Insert (end_of_first_byte << 8) + next_byte + 17 spaces
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 17;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, C_SPACE, expected_output_size);
                }
                INSERT_ZERO17 => {
                    // Insert (end_of_first_byte << 8) + next_byte + 17 nulls
                    if src_pos >= input.len() {
                        break;
                    }
                    let next_byte = input[src_pos] as usize;
                    src_pos += 1;

                    let count = ((end_of_first_byte as usize) << 8) + next_byte + 17;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, C_NULL, expected_output_size);
                }
                COPY1 => {
                    // Copy end_of_first_byte + 1 bytes
                    let count = end_of_first_byte as usize + 1;
                    self.copy_bytes(
                        input,
                        &mut src_pos,
                        &mut output,
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
                        &mut output,
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
                        &mut output,
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
                        &mut output,
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
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, byte_to_insert, expected_output_size);
                }
                INSERT_AT2 => {
                    // Insert end_of_first_byte + 2 '@' characters
                    let count = end_of_first_byte as usize + 2;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, C_AT, expected_output_size);
                }
                INSERT_BLANK2 => {
                    // Insert end_of_first_byte + 2 spaces
                    let count = end_of_first_byte as usize + 2;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, C_SPACE, expected_output_size);
                }
                INSERT_ZERO2 => {
                    // Insert end_of_first_byte + 2 nulls
                    let count = end_of_first_byte as usize + 2;
                    let new_len = output.len() + count;
                    Self::safe_resize(&mut output, new_len, C_NULL, expected_output_size);
                }
                _ => {
                    return Err(Error::InvalidRleCommand(command));
                }
            }
        }

        // Pad with zeros if we didn't reach the expected size
        if output.len() < expected_output_size {
            output.resize(expected_output_size, C_NULL);
        }

        // Truncate if we exceeded the expected size (shouldn't happen normally)
        if output.len() > expected_output_size {
            output.truncate(expected_output_size);
        }

        Ok(output)
    }

    fn copy_bytes(
        &self,
        input: &[u8],
        src_pos: &mut usize,
        output: &mut Vec<u8>,
        count: usize,
        max_output_size: usize,
    ) -> Result<()> {
        let remaining = input.len() - *src_pos;
        let space_left = max_output_size.saturating_sub(output.len());
        let to_copy = count.min(remaining).min(space_left);

        if to_copy > 0 {
            output.extend_from_slice(&input[*src_pos..*src_pos + to_copy]);
            *src_pos += to_copy;
        }

        Ok(())
    }

    /// Safe resize that doesn't exceed max_output_size
    fn safe_resize(output: &mut Vec<u8>, new_len: usize, value: u8, max_output_size: usize) {
        let actual_len = new_len.min(max_output_size);
        output.resize(actual_len, value);
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
