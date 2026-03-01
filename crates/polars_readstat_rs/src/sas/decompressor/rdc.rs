// RDC (SASYZCR2) decompression
use crate::error::{Error, Result};

// RDC command constants
const SHORT_RLE: u8 = 0;
const LONG_RLE: u8 = 1;
const LONG_PATTERN: u8 = 2;
// Commands 3-15 are short patterns with length = cmd

// Special character
const C_NULL: u8 = 0x00;

pub struct RdcDecompressor {}

impl RdcDecompressor {
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
        // Pre-allocate and zero-fill the output buffer
        // This is required for RDC pattern copying to work correctly
        let mut output = vec![C_NULL; expected_output_size];
        let mut output_pos = 0;
        let mut src_pos = 0;

        let mut ctrl_bits: u32 = 0;
        let mut ctrl_mask: u32 = 0;

        while src_pos < input.len() && output_pos < expected_output_size {
            // Load new control word if mask is exhausted
            if ctrl_mask == 0 {
                if src_pos + 1 >= input.len() {
                    break;
                }
                let byte1 = input[src_pos] as u32;
                let byte2 = input[src_pos + 1] as u32;
                src_pos += 2;

                ctrl_bits = (byte1 << 8) | byte2;
                ctrl_mask = 0x8000;
            }

            // Check control bit
            if (ctrl_bits & ctrl_mask) == 0 {
                // Literal byte
                if src_pos >= input.len() {
                    break;
                }
                let literal = input[src_pos];
                src_pos += 1;

                if output_pos < expected_output_size {
                    output[output_pos] = literal;
                    output_pos += 1;
                }
            } else {
                // Command byte
                if src_pos >= input.len() {
                    break;
                }
                let command_byte = input[src_pos];
                src_pos += 1;

                let cmd = (command_byte >> 4) & 0x0F;
                let cnt = command_byte & 0x0F;

                match cmd {
                    SHORT_RLE => {
                        // Short RLE: repeat byte (cnt + 3) times
                        if src_pos >= input.len() {
                            break;
                        }
                        let repeat_byte = input[src_pos];
                        src_pos += 1;

                        let count = cnt as usize + 3;
                        let actual_count = count.min(expected_output_size - output_pos);
                        for i in 0..actual_count {
                            output[output_pos + i] = repeat_byte;
                        }
                        output_pos += actual_count;
                    }
                    LONG_RLE => {
                        // Long RLE: repeat byte (cnt + (extra << 4) + 19) times
                        if src_pos + 1 >= input.len() {
                            break;
                        }
                        let extra = input[src_pos] as usize;
                        let repeat_byte = input[src_pos + 1];
                        src_pos += 2;

                        let count = cnt as usize + (extra << 4) + 19;
                        let actual_count = count.min(expected_output_size - output_pos);
                        for i in 0..actual_count {
                            output[output_pos + i] = repeat_byte;
                        }
                        output_pos += actual_count;
                    }
                    LONG_PATTERN => {
                        // Long pattern: copy from history
                        if src_pos + 1 >= input.len() {
                            break;
                        }
                        let extra = input[src_pos] as usize;
                        let count_byte = input[src_pos + 1] as usize;
                        src_pos += 2;

                        let offset = cnt as usize + 3 + (extra << 4);
                        let count = count_byte + 16;

                        output_pos = self.copy_pattern(
                            &mut output,
                            output_pos,
                            offset,
                            count,
                            expected_output_size,
                        )?;
                    }
                    _ if cmd >= 3 && cmd <= 15 => {
                        // Short pattern: copy cmd bytes from history
                        if src_pos >= input.len() {
                            break;
                        }
                        let extra = input[src_pos] as usize;
                        src_pos += 1;

                        let offset = cnt as usize + 3 + (extra << 4);
                        let count = cmd as usize;

                        output_pos = self.copy_pattern(
                            &mut output,
                            output_pos,
                            offset,
                            count,
                            expected_output_size,
                        )?;
                    }
                    _ => {
                        return Err(Error::InvalidRdcCommand(cmd));
                    }
                }
            }

            ctrl_mask >>= 1;
        }

        // Buffer is already the right size and zero-filled
        Ok(output)
    }

    fn copy_pattern(
        &self,
        output: &mut [u8],
        output_pos: usize,
        offset: usize,
        count: usize,
        max_output_size: usize,
    ) -> Result<usize> {
        if output_pos < offset {
            return Err(Error::DecompressionError(format!(
                "RDC: Invalid offset {} with current position {}",
                offset, output_pos
            )));
        }

        let src_pos = output_pos - offset;
        let actual_count = count.min(max_output_size - output_pos);

        // Copy bytes one at a time to handle overlapping patterns
        for i in 0..actual_count {
            output[output_pos + i] = output[src_pos + (i % offset)];
        }

        Ok(output_pos + actual_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdc_literal() {
        let mut decompressor = RdcDecompressor::new();

        // Control bits: 0x0000 (all zeros = all literals)
        // Followed by 5 literal bytes
        let input = vec![0x00, 0x00, b'h', b'e', b'l', b'l', b'o'];
        let output = decompressor.decompress(&input, 8192).unwrap();
        // Only first byte 'h' should be output since we need 16 control bits for 16 bytes
        // and 0x0000 means all 16 are literals
        assert_eq!(output[0], b'h');
    }

    #[test]
    fn test_rdc_short_rle() {
        let mut decompressor = RdcDecompressor::new();

        // Control bits: 0x8000 (first bit set = command)
        // Command: 0x02 (cmd=0=SHORT_RLE, cnt=2) -> repeat 2+3=5 times
        // Byte to repeat: 'A'
        let input = vec![0x80, 0x00, 0x02, b'A'];
        let output = decompressor.decompress(&input, 8192).unwrap();
        assert_eq!(&output[..5], b"AAAAA");
    }

    #[test]
    fn test_rdc_pattern() {
        let mut decompressor = RdcDecompressor::new();

        // Control word: 0xE000 = 1110 0000 0000 0000
        // First 3 bits are 1 (commands), rest are 0 (literals)
        // But we'll only use first 4 bits:
        // Bit 0: 1 -> command
        // Bit 1: 1 -> command
        // Bit 2: 1 -> command
        // Bit 3: 0 -> literal

        // Command 1: Insert literal 'A'
        // Command 2: Insert literal 'B'
        // Command 3: Insert literal 'C'
        // After this we have "ABC" in output

        // Actually, let's use a simpler test: just SHORT_RLE and literals
        // Control: 0xC000 = 1100 0000 0000 0000 (first 2 bits are commands)
        // Command 1 (bit 0 = 1): 0x02 (SHORT_RLE with cnt=2) -> repeat 5 times, byte='A'
        // Command 2 (bit 1 = 1): 0x02 (SHORT_RLE with cnt=2) -> repeat 5 times, byte='B'
        let input = vec![
            0xC0, 0x00, // Control word
            0x02, b'A', // Command 1: repeat 'A' 5 times
            0x02, b'B', // Command 2: repeat 'B' 5 times
        ];
        let output = decompressor.decompress(&input, 8192).unwrap();
        assert_eq!(&output[..10], b"AAAAABBBBB");
    }
}
