//! Abstraction over where writer bytes go — the write-side mirror of
//! [`crate::source::ReadSource`].
//!
//! Every format writer in this crate (`StataWriter::write_df`,
//! `SpssWriter::write_df`, `XptWriter::write_df`, `write_por`) is already
//! generic over `W: Write` internally; each one only hard-codes "a local
//! file" at a single `File::create(path)` call site. [`WriteTarget`]
//! generalizes that one operation so those call sites can target a cloud
//! object instead, without touching any of the format-specific serialization
//! logic.
//!
//! Unlike reads, none of the writers reachable from Python need `Seek` — the
//! DataFrame being written is always fully materialized first, so the row
//! count (and therefore every header field) is known before the first byte
//! is written. That means a cloud destination can stream straight into a
//! multipart upload ([`crate::cloud_destination::BlockingMultipartWriter`])
//! with no local temp file and no full in-memory buffering of the output.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Where a format writer's bytes should be sent.
#[derive(Debug, Clone)]
pub enum WriteTarget {
    Local(PathBuf),
    #[cfg(feature = "cloud")]
    Cloud(crate::cloud_destination::CloudWriteTarget),
}

impl WriteTarget {
    pub fn local(path: impl AsRef<Path>) -> Self {
        WriteTarget::Local(path.as_ref().to_path_buf())
    }

    /// A cloud object addressed by URL — see
    /// [`crate::cloud_source::ObjectStoreSource::from_url`] for the accepted
    /// schemes and `options` vocabulary (identical here: same
    /// `object_store::parse_url_opts` dispatch, same per-provider config
    /// keys Polars' `storage_options` uses).
    #[cfg(feature = "cloud")]
    pub fn from_url<I, K, V>(url: &str, options: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        Ok(WriteTarget::Cloud(
            crate::cloud_destination::CloudWriteTarget::from_url(url, options)?,
        ))
    }

    /// Open a fresh writer for this target. Callers must write all bytes,
    /// then call [`DestinationWriter::finish`] — dropping the writer without
    /// calling `finish` leaves a cloud upload aborted/incomplete.
    pub fn create_writer(&self) -> io::Result<DestinationWriter> {
        match self {
            WriteTarget::Local(path) => {
                let file = File::create(path)?;
                Ok(DestinationWriter::Local(BufWriter::with_capacity(
                    8 * 1024 * 1024,
                    file,
                )))
            }
            #[cfg(feature = "cloud")]
            WriteTarget::Cloud(target) => Ok(DestinationWriter::Cloud(target.start()?)),
        }
    }
}

/// A `Write` handle over a [`WriteTarget`]. Behaves exactly like the
/// `BufWriter<File>` every writer used to construct inline when the target
/// is local; behaves like a chunked multipart upload when it's cloud-backed.
pub enum DestinationWriter {
    Local(BufWriter<File>),
    #[cfg(feature = "cloud")]
    Cloud(crate::cloud_destination::BlockingMultipartWriter),
}

impl Write for DestinationWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            DestinationWriter::Local(w) => w.write(buf),
            #[cfg(feature = "cloud")]
            DestinationWriter::Cloud(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            DestinationWriter::Local(w) => w.flush(),
            #[cfg(feature = "cloud")]
            DestinationWriter::Cloud(w) => w.flush(),
        }
    }
}

impl DestinationWriter {
    /// Finalize the write. For a local file this is just a flush (the file
    /// closes on drop, same as before); for a cloud target this completes
    /// the multipart upload, making the object visible.
    pub fn finish(self) -> io::Result<()> {
        match self {
            DestinationWriter::Local(mut w) => w.flush(),
            #[cfg(feature = "cloud")]
            DestinationWriter::Cloud(w) => w.finish(),
        }
    }
}
