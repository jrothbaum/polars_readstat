//! Small text-decoding helpers shared across the sas/stata/spss readers.

/// Trim trailing space and NUL padding from a fixed-width string field.
pub(crate) fn trim_trailing_pad(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == 0) {
        end -= 1;
    }
    &bytes[..end]
}

/// SAS's C-string convention for fixed-width character columns: trim trailing
/// pad, then also stop at the first embedded NUL (a mid-string NUL
/// terminates the value, matching ReadStat's behavior).
pub(crate) fn trim_padded_c_string(bytes: &[u8]) -> &[u8] {
    let trimmed = trim_trailing_pad(bytes);
    match trimmed.iter().position(|&b| b == 0) {
        Some(nul) => &trimmed[..nul],
        None => trimmed,
    }
}
