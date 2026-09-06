//! Abstraction over where reader bytes come from.
//!
//! Every parser in this crate is generic over `R: Read + Seek`. The only place
//! the crate hard-codes "a local file" is in the various reader entry points
//! (`*Reader::open`, and the free `read_*`/`*_batch_iter` functions), which
//! each call `File::open(path)` directly — once per metadata pass and once
//! per parallel worker. [`ReadSource`] generalizes that single operation
//! ("give me a fresh, independently-positioned handle to the underlying
//! bytes") so those call sites can be backed by something other than a local
//! path, e.g. an in-memory buffer or a caller-supplied remote object.

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A `Read + Seek` handle that can be sent across threads and lives for the
/// program's duration. Blanket-implemented for anything that already
/// satisfies the bounds (`File`, `Cursor<Arc<[u8]>>`, ...).
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

/// Something that can hand out independent, freshly-positioned readers on
/// demand. Each call to [`open_reader`](ReadSource::open_reader) must return
/// a handle whose position is unaffected by any other handle previously
/// returned by the same source — this is what lets parallel workers each
/// call it and seek to their own byte range without interfering with one
/// another, exactly as repeated `File::open(path)` calls do today.
pub trait ReadSource: Send + Sync + 'static {
    fn open_reader(&self) -> std::io::Result<Box<dyn ReadSeek>>;

    /// Total byte length of the underlying data, without necessarily
    /// requiring the caller to open a reader first. Some call sites (SAS7BDAT
    /// and XPT parallel-read planning) need this and previously got it via
    /// `fs::metadata(path).len()`, which has no equivalent for a non-file
    /// source. The default implementation opens a reader and seeks to the
    /// end; concrete sources override it with something cheaper.
    fn len(&self) -> std::io::Result<u64> {
        let mut r = self.open_reader()?;
        r.seek(SeekFrom::End(0))
    }
}

/// A source backed by a local file path — opens a fresh [`std::fs::File`]
/// handle on every call, matching the crate's previous behavior exactly.
#[derive(Debug, Clone)]
pub struct LocalFileSource(PathBuf);

impl LocalFileSource {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(path.as_ref().to_path_buf())
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl ReadSource for LocalFileSource {
    fn open_reader(&self) -> std::io::Result<Box<dyn ReadSeek>> {
        Ok(Box::new(std::fs::File::open(&self.0)?))
    }

    fn len(&self) -> std::io::Result<u64> {
        Ok(std::fs::metadata(&self.0)?.len())
    }
}

/// A source backed by an in-memory buffer. The buffer is shared (`Arc`)
/// across every handle returned, so opening a reader is a cheap refcount
/// bump rather than a copy.
#[derive(Debug, Clone)]
pub struct InMemorySource(Arc<[u8]>);

impl InMemorySource {
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }
}

impl ReadSource for InMemorySource {
    fn open_reader(&self) -> std::io::Result<Box<dyn ReadSeek>> {
        Ok(Box::new(Cursor::new(Arc::clone(&self.0))))
    }

    fn len(&self) -> std::io::Result<u64> {
        Ok(self.0.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, SeekFrom};

    #[test]
    fn in_memory_source_readers_are_independent() {
        let source = InMemorySource::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7]);
        let mut a = source.open_reader().unwrap();
        let mut b = source.open_reader().unwrap();

        a.seek(SeekFrom::Start(6)).unwrap();
        let mut buf = [0u8; 1];
        // b was opened independently and must still read from offset 0,
        // unaffected by a's seek.
        b.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0);

        let mut buf = [0u8; 1];
        a.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 6);
    }

    #[test]
    fn in_memory_source_len_matches_file_len() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let bytes = std::fs::read(&path).unwrap();

        let file_source = LocalFileSource::new(&path);
        let mem_source = InMemorySource::new(bytes.clone());

        assert_eq!(file_source.len().unwrap(), bytes.len() as u64);
        assert_eq!(mem_source.len().unwrap(), bytes.len() as u64);
    }

    #[test]
    fn in_memory_source_matches_file_bytes() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let bytes = std::fs::read(&path).unwrap();

        let file_source = LocalFileSource::new(&path);
        let mem_source = InMemorySource::new(bytes.clone());

        let mut from_file = Vec::new();
        file_source
            .open_reader()
            .unwrap()
            .read_to_end(&mut from_file)
            .unwrap();

        let mut from_mem = Vec::new();
        mem_source
            .open_reader()
            .unwrap()
            .read_to_end(&mut from_mem)
            .unwrap();

        assert_eq!(from_file, from_mem);
        assert_eq!(from_file, bytes);
    }
}
