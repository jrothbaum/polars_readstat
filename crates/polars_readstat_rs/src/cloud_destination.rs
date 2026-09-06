//! [`WriteTarget`](crate::destination::WriteTarget) backed by
//! [`object_store`] — the write-side counterpart of
//! [`crate::cloud_source::ObjectStoreSource`]. Uses the same
//! `object_store::parse_url_opts` scheme dispatch and provider config-key
//! vocabulary (`AmazonS3ConfigKey`/`GoogleConfigKey`/`AzureConfigKey`) as the
//! read side, so a `storage_options` dict works identically for both.
//!
//! Uploads happen via `ObjectStore::put_multipart` in fixed-size chunks: a
//! [`BlockingMultipartWriter`] buffers writes in memory only up to
//! `CHUNK_SIZE` before sending a part, so writing arbitrarily large output
//! never holds more than one chunk's worth of bytes at a time (on top of
//! whatever the format writer itself already buffers) — no local temp file,
//! no doubling the DataFrame's serialized size in memory.

use crate::cloud_source::{parse_cloud_url, shared_runtime};
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use std::io::{Error as IoError, Result as IoResult, Write};
use std::sync::Arc;

/// Most providers require multipart parts to be at least 5 MiB (except the
/// last one); this is comfortably above that while still bounding memory use
/// to a small, fixed amount per in-flight part.
const CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// A cloud object addressed by URL, ready to open a
/// [`BlockingMultipartWriter`] against.
#[derive(Debug, Clone)]
pub struct CloudWriteTarget {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
}

impl CloudWriteTarget {
    pub fn from_url<I, K, V>(url: &str, options: I) -> IoResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, path) = parse_cloud_url(url, options)?;
        Ok(Self {
            store: Arc::from(store),
            path,
        })
    }

    pub fn start(&self) -> IoResult<BlockingMultipartWriter> {
        let upload = shared_runtime()
            .block_on(self.store.put_multipart(&self.path))
            .map_err(IoError::other)?;
        Ok(BlockingMultipartWriter {
            upload: Some(upload),
            buffer: Vec::with_capacity(CHUNK_SIZE),
        })
    }

    /// Stream an already-written local file to this destination in fixed-size
    /// chunks and complete the upload. Used by writers that need `Seek` to
    /// finalize their output (e.g. Stata's streaming header patch) and so
    /// can't write directly to a cloud destination: they write to a local
    /// temp file first, then hand it to this method to upload — reading the
    /// temp file back in chunks rather than loading it into memory at once.
    pub fn upload_file(&self, path: &std::path::Path) -> IoResult<()> {
        let mut file = std::fs::File::open(path)?;
        let mut writer = self.start()?;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            let n = std::io::Read::read(&mut file, &mut buf)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n])?;
        }
        writer.finish()
    }
}

/// A synchronous [`Write`] adapter over `object_store`'s multipart upload
/// API. Each `write()` call buffers bytes and, once a full `CHUNK_SIZE` is
/// accumulated, blocks the calling thread to upload that chunk — bridging
/// this crate's synchronous writer code to `object_store`'s async API the
/// same way [`crate::cloud_source::ObjectStoreSource`]'s reader does.
///
/// Must be finalized with [`Self::finish`] to complete the upload; if
/// dropped without finishing (e.g. an earlier write returned an error), the
/// upload is aborted on a best-effort basis so no partial object — or, for
/// providers that can't clean up automatically, stray uploaded parts — is
/// left behind.
pub struct BlockingMultipartWriter {
    upload: Option<Box<dyn MultipartUpload>>,
    buffer: Vec<u8>,
}

impl BlockingMultipartWriter {
    fn upload_full_chunks(&mut self) -> IoResult<()> {
        while self.buffer.len() >= CHUNK_SIZE {
            let chunk: Vec<u8> = self.buffer.drain(..CHUNK_SIZE).collect();
            let upload = self
                .upload
                .as_mut()
                .expect("write called after finish/abort");
            shared_runtime()
                .block_on(upload.put_part(PutPayload::from(chunk)))
                .map_err(IoError::other)?;
        }
        Ok(())
    }

    /// Flush any remaining buffered bytes as a final (possibly short) part,
    /// then complete the upload, making the object visible.
    pub fn finish(mut self) -> IoResult<()> {
        if !self.buffer.is_empty() {
            let chunk = std::mem::take(&mut self.buffer);
            let upload = self.upload.as_mut().expect("finish called twice");
            shared_runtime()
                .block_on(upload.put_part(PutPayload::from(chunk)))
                .map_err(IoError::other)?;
        }
        let mut upload = self.upload.take().expect("finish called twice");
        shared_runtime()
            .block_on(upload.complete())
            .map(|_| ())
            .map_err(IoError::other)
    }
}

impl Write for BlockingMultipartWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.buffer.extend_from_slice(buf);
        self.upload_full_chunks()?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        // Real completion happens in `finish`; providers generally can't
        // acknowledge a part as durable mid-upload any more than a
        // BufWriter's flush guarantees an fsync, so this is a no-op.
        Ok(())
    }
}

impl Drop for BlockingMultipartWriter {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take() {
            let _ = shared_runtime().block_on(upload.abort());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::io::ErrorKind;

    fn target_with_memory_store() -> (Arc<InMemory>, CloudWriteTarget) {
        let store = Arc::new(InMemory::new());
        let target = CloudWriteTarget {
            store: store.clone(),
            path: ObjectPath::from("written.bin"),
        };
        (store, target)
    }

    #[test]
    fn small_write_roundtrips() {
        let (store, target) = target_with_memory_store();
        let mut w = target.start().unwrap();
        w.write_all(b"hello object store").unwrap();
        w.finish().unwrap();

        let got = shared_runtime()
            .block_on(store.get(&ObjectPath::from("written.bin")))
            .unwrap();
        let bytes = shared_runtime().block_on(got.bytes()).unwrap();
        assert_eq!(&bytes[..], b"hello object store");
    }

    #[test]
    fn write_spanning_multiple_chunks_roundtrips() {
        let (store, target) = target_with_memory_store();
        let mut w = target.start().unwrap();
        // Larger than CHUNK_SIZE so this exercises the mid-stream upload
        // path, not just the final-chunk-on-finish path.
        let payload: Vec<u8> = (0..(CHUNK_SIZE * 2 + 1234))
            .map(|i| (i % 251) as u8)
            .collect();
        w.write_all(&payload).unwrap();
        w.finish().unwrap();

        let got = shared_runtime()
            .block_on(store.get(&ObjectPath::from("written.bin")))
            .unwrap();
        let bytes = shared_runtime().block_on(got.bytes()).unwrap();
        assert_eq!(&bytes[..], &payload[..]);
    }

    #[test]
    fn dropping_without_finish_leaves_no_object() {
        let (store, target) = target_with_memory_store();
        {
            let mut w = target.start().unwrap();
            w.write_all(b"never finished").unwrap();
            // dropped here without calling finish() -- should abort
        }
        let result = shared_runtime().block_on(store.get(&ObjectPath::from("written.bin")));
        assert!(result.is_err(), "aborted upload should not be visible");
    }

    #[test]
    fn from_url_rejects_malformed_url() {
        let err = CloudWriteTarget::from_url("not a url at all", std::iter::empty::<(&str, &str)>())
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    // Each format writer only ever hard-codes "a local file" at a single
    // `File::create`-equivalent call site, now routed through `WriteTarget`.
    // These tests prove that swapping in a cloud destination doesn't change
    // a single byte of the format writer's actual output: writing the same
    // DataFrame to a local temp file and to an in-memory `ObjectStore` must
    // produce byte-identical results.
    mod format_writer_parity {
        use super::*;
        use crate::destination::WriteTarget;
        use crate::sas::xpt_writer::XptWriter;
        use crate::spss::por::{write_por, write_por_to_destination, PorWriteOptions};
        use crate::spss::writer::SpssWriter;
        use crate::stata::writer::{StataWriteColumn, StataWriteSchema, StataWriter};
        use polars::prelude::*;
        use std::time::{SystemTime, UNIX_EPOCH};

        fn temp_path(prefix: &str, ext: &str) -> std::path::PathBuf {
            let mut path = std::env::temp_dir();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let pid = std::process::id();
            path.push(format!("{prefix}_{pid}_{nanos}.{ext}"));
            path
        }

        fn sample_df() -> DataFrame {
            df![
                "id" => [1i32, 2, 3],
                "name" => ["alpha", "beta", "gamma"],
                "value" => [1.5f64, 2.5, 3.5],
            ]
            .unwrap()
        }

        fn cloud_target(store: &Arc<InMemory>, key: &str) -> WriteTarget {
            WriteTarget::Cloud(CloudWriteTarget {
                store: store.clone(),
                path: ObjectPath::from(key),
            })
        }

        fn get_bytes(store: &Arc<InMemory>, key: &str) -> Vec<u8> {
            let got = shared_runtime()
                .block_on(store.get(&ObjectPath::from(key)))
                .unwrap();
            shared_runtime().block_on(got.bytes()).unwrap().to_vec()
        }

        #[test]
        fn stata_write_df_matches_local_file() {
            let df = sample_df();
            let local_path = temp_path("cloud_parity_stata", "dta");
            StataWriter::new(&local_path).write_df(&df).unwrap();
            let local_bytes = std::fs::read(&local_path).unwrap();
            std::fs::remove_file(&local_path).ok();

            let store = Arc::new(InMemory::new());
            let target = cloud_target(&store, "out.dta");
            StataWriter::with_destination(target).write_df(&df).unwrap();
            let cloud_bytes = get_bytes(&store, "out.dta");

            assert_eq!(local_bytes, cloud_bytes);
        }

        #[test]
        fn spss_write_df_matches_local_file() {
            let df = sample_df();
            let local_path = temp_path("cloud_parity_spss", "sav");
            SpssWriter::new(&local_path).write_df(&df).unwrap();
            let local_bytes = std::fs::read(&local_path).unwrap();
            std::fs::remove_file(&local_path).ok();

            let store = Arc::new(InMemory::new());
            let target = cloud_target(&store, "out.sav");
            SpssWriter::with_destination(target).write_df(&df).unwrap();
            let cloud_bytes = get_bytes(&store, "out.sav");

            assert_eq!(local_bytes, cloud_bytes);
        }

        #[test]
        fn xpt_write_df_matches_local_file() {
            let df = sample_df();
            let local_path = temp_path("cloud_parity_xpt", "xpt");
            XptWriter::new(local_path.clone()).write_df(&df).unwrap();
            let local_bytes = std::fs::read(&local_path).unwrap();
            std::fs::remove_file(&local_path).ok();

            let store = Arc::new(InMemory::new());
            let target = cloud_target(&store, "out.xpt");
            XptWriter::with_destination(target).write_df(&df).unwrap();
            let cloud_bytes = get_bytes(&store, "out.xpt");

            assert_eq!(local_bytes, cloud_bytes);
        }

        #[test]
        fn por_write_matches_local_file() {
            let df = sample_df();
            let local_path = temp_path("cloud_parity_por", "por");
            write_por(&df, &local_path, PorWriteOptions::default()).unwrap();
            let local_bytes = std::fs::read(&local_path).unwrap();
            std::fs::remove_file(&local_path).ok();

            let store = Arc::new(InMemory::new());
            let target = cloud_target(&store, "out.por");
            write_por_to_destination(&df, target, PorWriteOptions::default()).unwrap();
            let cloud_bytes = get_bytes(&store, "out.por");

            assert_eq!(local_bytes, cloud_bytes);
        }

        /// `write_batches_streaming` (the `sink_stata` path) needs `Seek` to
        /// patch its header once the final row count is known — something a
        /// cloud multipart upload can't do. For a cloud destination it
        /// should buffer through a local temp file and upload that,
        /// producing output identical to a local streaming write, and must
        /// not leave the temp file behind afterward.
        #[test]
        fn stata_streaming_write_to_cloud_matches_local_and_cleans_up_temp() {
            let schema = StataWriteSchema {
                columns: vec![
                    StataWriteColumn {
                        name: "id".to_string(),
                        dtype: DataType::Int32,
                        string_width_bytes: None,
                    },
                    StataWriteColumn {
                        name: "name".to_string(),
                        dtype: DataType::String,
                        string_width_bytes: Some(5),
                    },
                ],
                row_count: None,
                value_labels: None,
                variable_labels: None,
                variable_formats: None,
            };
            let batches = || {
                vec![
                    df!["id" => [1i32, 2], "name" => ["alpha", "beta"]].unwrap(),
                    df!["id" => [3i32], "name" => ["gamma"]].unwrap(),
                ]
            };

            let local_path = temp_path("cloud_parity_stata_stream", "dta");
            StataWriter::new(&local_path)
                .write_batches_streaming(batches(), schema.clone())
                .unwrap();
            let local_bytes = std::fs::read(&local_path).unwrap();
            std::fs::remove_file(&local_path).ok();

            let temp_dir_before: std::collections::HashSet<_> = std::fs::read_dir(std::env::temp_dir())
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .collect();

            let store = Arc::new(InMemory::new());
            let target = cloud_target(&store, "out.dta");
            StataWriter::with_destination(target)
                .write_batches_streaming(batches(), schema)
                .unwrap();
            let cloud_bytes = get_bytes(&store, "out.dta");

            assert_eq!(local_bytes, cloud_bytes);

            let temp_dir_after: std::collections::HashSet<_> = std::fs::read_dir(std::env::temp_dir())
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .collect();
            let leftover: Vec<_> = temp_dir_after
                .difference(&temp_dir_before)
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.starts_with("polars_readstat_stata_stream"))
                        .unwrap_or(false)
                })
                .collect();
            assert!(leftover.is_empty(), "temp file not cleaned up: {leftover:?}");
        }
    }
}
