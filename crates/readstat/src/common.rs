use std::error::Error;
use std::ffi::CStr;

pub fn build_offsets(
    row_count: u32,
    stream_rows: u32,
) -> Result<Vec<u32>, Box<dyn Error + Send + Sync>> {
    // Get number of chunks
    let chunks = if stream_rows < row_count {
        if row_count % stream_rows == 0 {
            row_count / stream_rows
        } else {
            (row_count / stream_rows) + 1
        }
    } else {
        1
    };

    // Allocate and populate a vector for the offsets
    let mut offsets: Vec<u32> = Vec::with_capacity(chunks as usize);

    for c in 0..=chunks {
        if c == 0 {
            offsets.push(0);
        } else if c == chunks {
            offsets.push(row_count);
        } else {
            offsets.push(c * stream_rows);
        }
    }

    Ok(offsets)
}

// String out from C pointer
pub unsafe fn ptr_to_string(x: *const i8) -> String {
    if x.is_null() {
        String::new()
    } else {
        // From Rust documentation - https://doc.rust-lang.org/std/ffi/struct.CStr.html
        let cstr = CStr::from_ptr(x);
        // Get copy-on-write Cow<'_, str>, then guarantee a freshly-owned String allocation
        String::from_utf8_lossy(cstr.to_bytes()).to_string()
    }
}
