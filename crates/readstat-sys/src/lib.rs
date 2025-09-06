#![allow(clippy::useless_transmute)]
#![allow(dead_code)]
#![allow(deref_nullptr)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
use std::ffi::CStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use memmap2::Mmap;
use std::fs::File;


// Internal structure for reference-counted mmap
struct SharedMmapInner {
    mmap: Mmap,
    ref_count: AtomicUsize,
}

pub struct SharedMmap {
    inner: Arc<SharedMmapInner>,
}

impl SharedMmap {
    pub fn new(file_path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let file = File::open(file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        Ok(SharedMmap {
            inner: Arc::new(SharedMmapInner {
                mmap,
                ref_count: AtomicUsize::new(1),
            })
        })
    }
    
    pub fn create_parser(&self) -> Result<*mut readstat_parser_t, Box<dyn std::error::Error + Send + Sync>> {
        let parser = unsafe { readstat_parser_init() };
        if parser.is_null() {
            return Err("Failed to create parser".into());
        }
        
        let result = unsafe { shared_mmap_io_init(parser, self as *const _ as *mut shared_mmap_t) };
        if result != readstat_error_e_READSTAT_OK {
            unsafe { readstat_parser_free(parser) };
            return Err("Failed to initialize mmap IO".into());
        }
        
        Ok(parser)
    }

    // public method for debugging
    pub fn memory_address(&self) -> *const u8 {
        self.inner.mmap.as_ptr()
    }
}

impl Clone for SharedMmap {
    fn clone(&self) -> Self {
        self.inner.ref_count.fetch_add(1, Ordering::SeqCst);
        SharedMmap {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for SharedMmap {
    fn drop(&mut self) {
        self.inner.ref_count.fetch_sub(1, Ordering::SeqCst);
    }
}

unsafe impl Send for SharedMmap {}
unsafe impl Sync for SharedMmap {}

// C FFI functions that Rust implements
#[no_mangle]
pub extern "C" fn create_shared_mmap(file_path: *const libc::c_char) -> *mut shared_mmap_t {
    let path_str = unsafe { CStr::from_ptr(file_path) };
    let path = match path_str.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    
    match SharedMmap::new(path) {
        Ok(mmap) => Box::into_raw(Box::new(mmap)) as *mut shared_mmap_t,
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn retain_shared_mmap(mmap: *mut shared_mmap_t) {
    if !mmap.is_null() {
        let shared_mmap = unsafe { &*(mmap as *const SharedMmap) };
        shared_mmap.inner.ref_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[no_mangle]
pub extern "C" fn release_shared_mmap(mmap: *mut shared_mmap_t) {
    if !mmap.is_null() {
        let shared_mmap = unsafe { &*(mmap as *const SharedMmap) };
        if shared_mmap.inner.ref_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            unsafe { let _ = Box::from_raw(mmap as *mut SharedMmap); };
        }
    }
}

#[no_mangle]
pub extern "C" fn get_mmap_size(mmap: *mut shared_mmap_t) -> libc::size_t {
    if mmap.is_null() {
        return 0;
    }
    let shared_mmap = unsafe { &*(mmap as *const SharedMmap) };
    shared_mmap.inner.mmap.len()
}

#[no_mangle]
pub extern "C" fn get_mmap_ptr(mmap: *mut shared_mmap_t) -> *const libc::c_char {
    if mmap.is_null() {
        return std::ptr::null();
    }
    let shared_mmap = unsafe { &*(mmap as *const SharedMmap) };
    shared_mmap.inner.mmap.as_ptr() as *const libc::c_char
}