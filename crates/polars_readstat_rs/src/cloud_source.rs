//! [`ReadSource`] backed by [`object_store`] — the same crate Polars itself
//! uses for `s3://`/`gs://`/`az://` (and friends) scan paths, already
//! present in this crate's dependency graph transitively via
//! `polars-error`. `ObjectStoreSource` works with any
//! `object_store::ObjectStore` backend (S3, GCS, Azure, a local
//! MinIO/localstack instance for testing); [`ObjectStoreSource::from_url`]
//! is the entry point for all of them.
//!
//! `from_url` is a thin wrapper over [`object_store::parse_url_opts`] — the
//! same universal scheme-dispatching function Polars' own `CloudOptions`
//! machinery is built on. It recognizes `s3://`/`s3a://` (AWS/S3-compatible,
//! e.g. MinIO via an endpoint override), `gs://` (GCS), and
//! `az://`/`abfs://`/`abfss://`/`adl://`/`azure://` (Azure, including the
//! `https://<account>.blob.core.windows.net/...` form) in one call, and the
//! `options` map is parsed per-scheme by that provider's own config-key enum
//! (`AmazonS3ConfigKey`/`GoogleConfigKey`/`AzureConfigKey`) — the exact same
//! types Polars' `storage_options` parses into, so the accepted key names
//! (`aws_access_key_id`, `aws_region`, `aws_endpoint_url`; `service_account`,
//! `service_account_key`; `account_name`, `account_key`, `sas_token`,
//! `tenant_id`, ...) match what `pl.scan_parquet(path, storage_options=...)`
//! documents for each provider.
//!
//! Gated behind the `cloud` feature — it pulls in `object_store`'s cloud
//! networking stack (reqwest, AWS/GCP/Azure request signing, etc.), which
//! the six local/in-memory format readers have no need for. Named to match
//! polars-io's own umbrella feature for the same thing.

use crate::source::{ReadSeek, ReadSource};
use object_store::buffered::BufReader as ObjectStoreBufReader;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::runtime::Runtime;
use url::Url;

/// One background runtime, shared by every `ObjectStoreSource` in the
/// process. This crate's parallel readers are plain OS threads
/// (`std::thread::spawn`), not async tasks, so each one just blocks on this
/// runtime while its chunk downloads — a multi-threaded runtime lets those
/// blocking calls from multiple reader threads actually proceed concurrently
/// rather than queuing behind one another.
pub(crate) fn shared_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("failed to start the background tokio runtime for object_store access")
    })
}

/// A [`ReadSource`] backed by any [`object_store::ObjectStore`]. Each
/// `open_reader()` call gets its own independent, chunked, range-fetching
/// reader ([`object_store::buffered::BufReader`]) so parallel workers can
/// each seek to and read their own byte range without interfering with one
/// another — exactly like `LocalFileSource`/`InMemorySource`, just backed by
/// ranged GET requests instead of local I/O.
#[derive(Debug, Clone)]
pub struct ObjectStoreSource {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
}

impl ObjectStoreSource {
    /// Build a source from any `ObjectStore` and object path. Issues one
    /// `HEAD`-equivalent request up front so `len()` — and every reader's
    /// internal chunk-fetch logic — never needs to ask again.
    pub fn new(store: Arc<dyn ObjectStore>, path: impl Into<ObjectPath>) -> IoResult<Self> {
        let path = path.into();
        let meta = shared_runtime()
            .block_on(store.head(&path))
            .map_err(IoError::other)?;
        Ok(Self { store, meta })
    }

    /// A cloud object addressed by URL — `s3://bucket/key.sas7bdat`,
    /// `gs://bucket/key.sas7bdat`, `az://container/key.sas7bdat`, etc. (see
    /// the module docs for the full scheme list) — configured the same way
    /// Polars' `storage_options` dict is: `options` is parsed by whichever
    /// provider's config-key enum matches the URL's scheme. Credentials/
    /// region not present in `options` fall back to the environment the
    /// same way each provider's own CLI/SDK does, so `options` can be empty
    /// when the environment (or an instance/task role) already provides
    /// everything needed.
    pub fn from_url<I, K, V>(url: &str, options: I) -> IoResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, path) = parse_cloud_url(url, options)?;
        Self::new(Arc::from(store), path)
    }
}

/// Resolve a `s3://`/`gs://`/`az://`(etc.) URL plus `storage_options`-style
/// config into an `ObjectStore` and the object path within it. Shared by
/// [`ObjectStoreSource::from_url`] (reads) and
/// [`crate::cloud_destination::CloudWriteTarget::from_url`] (writes) — both
/// just wrap the resulting `(store, path)` in whichever type they need.
pub(crate) fn parse_cloud_url<I, K, V>(
    url: &str,
    options: I,
) -> IoResult<(Box<dyn ObjectStore>, ObjectPath)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let parsed = Url::parse(url)
        .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("invalid URL {url}: {e}")))?;
    let options: Vec<(String, String)> = options
        .into_iter()
        .map(|(k, v)| (k.as_ref().to_string(), v.into()))
        .collect();
    object_store::parse_url_opts(&parsed, options).map_err(IoError::other)
}

impl ReadSource for ObjectStoreSource {
    fn open_reader(&self) -> IoResult<Box<dyn ReadSeek>> {
        let inner = ObjectStoreBufReader::new(self.store.clone(), &self.meta);
        Ok(Box::new(BlockingObjectReader { inner }))
    }

    fn len(&self) -> IoResult<u64> {
        // Already known from the `head` call in `new` — no I/O needed.
        Ok(self.meta.size)
    }
}

/// Adapts `object_store`'s async, chunked, range-fetching `BufReader` to
/// `std::io::{Read, Seek}` by blocking on the shared background runtime.
struct BlockingObjectReader {
    inner: ObjectStoreBufReader,
}

impl Read for BlockingObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        shared_runtime().block_on(self.inner.read(buf))
    }
}

impl Seek for BlockingObjectReader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        shared_runtime().block_on(self.inner.seek(pos))
    }
}

// `BlockingObjectReader` is only ever handed out as `Box<dyn ReadSeek>`
// (which requires `Send`); `ObjectStoreBufReader` itself is `Send`, so this
// is automatic — no unsafe impl needed.

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    #[test]
    fn object_store_parse_url_opts_splits_bucket_and_key() {
        let (store, path) = object_store::parse_url_opts(
            &Url::parse("s3://my-bucket/path/to/file.sas7bdat").unwrap(),
            std::iter::empty::<(&str, &str)>(),
        )
        .unwrap();
        assert_eq!(path.as_ref(), "path/to/file.sas7bdat");
        drop(store);
    }

    #[test]
    fn from_url_rejects_malformed_url() {
        let err = ObjectStoreSource::from_url("not a url at all", std::iter::empty::<(&str, &str)>())
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn from_url_ignores_unknown_storage_option_key() {
        // `object_store::parse_url_opts` (via its `builder_opts!` macro)
        // silently drops any option key that doesn't parse as that
        // provider's config-key enum, the same forward-compatible behavior
        // Polars' own `storage_options` has (unknown keys don't error) — it
        // does NOT validate key names up front. So an unrecognized key
        // alongside otherwise-valid, fast-failing config still reaches the
        // (unreachable) endpoint rather than failing at parse time.
        let result = ObjectStoreSource::from_url(
            "s3://bucket/key.sas7bdat",
            [
                ("not_a_real_option", "value"),
                ("aws_access_key_id", "id"),
                ("aws_secret_access_key", "secret"),
                ("aws_region", "us-east-1"),
                ("aws_endpoint_url", "http://127.0.0.1:19999"),
                ("aws_allow_http", "true"),
                ("aws_connect_timeout", "1s"),
                ("aws_timeout", "1s"),
            ],
        );
        // Reaches the (fast, local, unreachable) endpoint instead of
        // erroring immediately on the bad key — proves the bad key was
        // ignored rather than rejected.
        if let Err(e) = result {
            assert_ne!(e.kind(), ErrorKind::InvalidInput, "unknown key should be ignored, not rejected");
        }
    }

    /// One test per scheme Polars documents for `storage_options`
    /// (https://docs.pola.rs/user-guide/io/cloud-storage/) — proves each
    /// scheme is recognized and its provider-specific config keys accepted,
    /// without touching the network (these fail at config-parsing or at the
    /// unreachable local port, never at "unsupported scheme"/"unknown key").
    #[test]
    fn from_url_accepts_polars_style_storage_options_for_every_scheme() {
        // Every case points at a nonexistent local port with a short
        // timeout, so a real failed connection attempt (proving scheme +
        // config-key recognition succeeded) doesn't fall back to a slow
        // real-network / retry-backoff path like the earlier AWS IMDS case
        // did when no endpoint override was given.
        let cases: &[(&str, &[(&str, &str)])] = &[
            (
                "s3://bucket/key.sas7bdat",
                &[
                    ("aws_access_key_id", "id"),
                    ("aws_secret_access_key", "secret"),
                    ("aws_region", "us-east-1"),
                    ("aws_endpoint_url", "http://127.0.0.1:19999"),
                    ("aws_allow_http", "true"),
                    ("aws_connect_timeout", "1s"),
                    ("aws_timeout", "1s"),
                ],
            ),
            (
                "gs://bucket/key.sas7bdat",
                &[
                    ("service_account_key", "{}"),
                    ("google_base_url", "http://127.0.0.1:19999"),
                    ("google_skip_signature", "true"),
                    ("connect_timeout", "1s"),
                    ("timeout", "1s"),
                ],
            ),
            (
                "az://container/key.sas7bdat",
                &[
                    ("account_name", "acct"),
                    ("account_key", "a2V5"),
                    ("azure_endpoint", "http://127.0.0.1:19999"),
                    ("azure_allow_http", "true"),
                    ("connect_timeout", "1s"),
                    ("timeout", "1s"),
                ],
            ),
        ];
        for (url, options) in cases {
            let result = ObjectStoreSource::from_url(url, options.iter().copied());
            // Either it got far enough to attempt a connection (Ok, or an
            // Err that isn't InvalidInput), or building itself failed for a
            // reason unrelated to scheme/key recognition (e.g. malformed
            // fake credentials) — what must NOT happen is an
            // ErrorKind::InvalidInput, which is reserved for "couldn't even
            // parse the URL" in `from_url`.
            if let Err(e) = result {
                assert_ne!(e.kind(), ErrorKind::InvalidInput, "scheme/key rejected for {url}");
            }
        }
    }

    /// Exercises the adapter (open_reader/seek/read/len) against an
    /// in-process `object_store::memory::InMemory` backend — no network, no
    /// credentials. `InMemory` implements the same `ObjectStore` trait every
    /// cloud provider does, so this proves the sync/async bridge and the
    /// `ObjectStoreBufReader` wiring are correct; the cloud-specific pieces
    /// (`from_url`) are just `*Builder` configuration on top.
    fn source_with_bytes(bytes: &[u8]) -> ObjectStoreSource {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("test-object.bin");
        shared_runtime()
            .block_on(store.put(&path, PutPayload::from(bytes.to_vec())))
            .expect("put into in-memory object store");
        ObjectStoreSource::new(store, path).expect("head in-memory object")
    }

    #[test]
    fn len_matches_object_size() {
        let bytes = b"hello object store";
        let source = source_with_bytes(bytes);
        assert_eq!(source.len().unwrap(), bytes.len() as u64);
    }

    #[test]
    fn read_reproduces_original_bytes() {
        let bytes = b"the quick brown fox jumps over the lazy dog".to_vec();
        let source = source_with_bytes(&bytes);

        let mut out = Vec::new();
        source
            .open_reader()
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, bytes);
    }

    #[test]
    fn independent_readers_seek_independently() {
        let source = source_with_bytes(b"0123456789");

        let mut a = source.open_reader().unwrap();
        let mut b = source.open_reader().unwrap();

        a.seek(SeekFrom::Start(5)).unwrap();
        let mut buf = [0u8; 1];
        // b was opened independently and must still read from offset 0,
        // unaffected by a's seek — the same invariant LocalFileSource and
        // InMemorySource are held to.
        b.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], b'0');

        let mut buf = [0u8; 1];
        a.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], b'5');
    }

    /// Proves `ObjectStoreSource` works through a real format reader end to
    /// end, including the parallel path (multiple workers each calling
    /// `open_reader()` concurrently against the shared runtime) — not just
    /// raw byte I/O like the tests above.
    #[test]
    fn sas7bdat_reads_identically_through_object_store_source() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/sas/data/data_pandas/test1.sas7bdat");
        if !path.exists() {
            return;
        }
        let bytes = std::fs::read(&path).expect("read fixture bytes");
        let source = source_with_bytes(&bytes);

        let reader = crate::sas::reader::Sas7bdatReader::open_source(Arc::new(source))
            .expect("open_source");
        let expected = crate::sas::reader::Sas7bdatReader::open(&path).expect("open by path");
        let limit = usize::min(200_000, expected.metadata().row_count);

        let df_parallel = reader
            .read()
            .with_limit(limit)
            .finish()
            .expect("parallel read via ObjectStoreSource");
        let df_sequential = reader
            .read()
            .sequential()
            .with_limit(limit)
            .finish()
            .expect("sequential read via ObjectStoreSource");
        let df_path = expected
            .read()
            .with_limit(limit)
            .finish()
            .expect("read by path");

        assert!(
            df_parallel.equals_missing(&df_sequential),
            "parallel and sequential ObjectStoreSource reads diverged"
        );
        assert!(
            df_parallel.equals_missing(&df_path),
            "ObjectStoreSource read diverged from path-backed read"
        );
    }
}
