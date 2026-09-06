"""No real S3/MinIO is available in this environment, so these tests can't
prove an actual S3 *read* or *write* succeeds end to end. What they do
prove: the `storage_options`/`s3://` plumbing genuinely reaches Rust's
`ObjectStoreSource` (reads) / `ObjectStoreDestination` (writes) — a real
HTTP attempt against the given endpoint with the given credentials/config,
not a Python-side `TypeError` on an unexpected keyword or a "missing file
extension" error — the same failure mode you'd see hitting a real,
unreachable, or misconfigured bucket.

`aws_connect_timeout`/`aws_timeout` keep these fast and deterministic
instead of falling back through object_store's default 10-retry backoff
(which would make each test take 10+ seconds against a guaranteed-closed
local port).
"""
from __future__ import annotations

import pytest
import polars as pl

import polars_readstat as prs

FAST_TIMEOUT_OPTIONS = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "aws_region": "us-east-1",
    "aws_endpoint_url": "http://127.0.0.1:19999",  # nothing listens here
    "aws_allow_http": "true",
    "aws_connect_timeout": "1s",
    "aws_timeout": "1s",
}

# Same shape as FAST_TIMEOUT_OPTIONS above, but for GCS/Azure — mirrors the
# key vocabulary and endpoint-override cases already covered on the Rust
# side in cloud_source.rs's `from_url_accepts_polars_style_storage_options_for_every_scheme`.
GCS_FAST_TIMEOUT_OPTIONS = {
    "service_account_key": "{}",
    "google_base_url": "http://127.0.0.1:19999",
    "google_skip_signature": "true",
    "connect_timeout": "1s",
    "timeout": "1s",
}

AZURE_FAST_TIMEOUT_OPTIONS = {
    "account_name": "acct",
    "account_key": "a2V5",
    "azure_endpoint": "http://127.0.0.1:19999",
    "azure_allow_http": "true",
    "connect_timeout": "1s",
    "timeout": "1s",
}


def test_scan_readstat_s3_url_reaches_network_layer():
    with pytest.raises(Exception) as exc_info:
        prs.scan_readstat(
            "s3://fake-bucket/fake.sas7bdat",
            storage_options=FAST_TIMEOUT_OPTIONS,
        ).collect()
    # A Python-side arg-binding failure would be a TypeError; a routing
    # failure (s3:// not recognized as such) would be a format/extension
    # error. Neither of those is what we want here — we want proof it tried
    # to talk to (a nonexistent) S3.
    assert "http" in str(exc_info.value).lower() or "connect" in str(exc_info.value).lower()


def test_read_sas7bcat_s3_url_reaches_network_layer():
    with pytest.raises(Exception) as exc_info:
        prs.read_sas7bcat("s3://fake-bucket/fake.sas7bcat", storage_options=FAST_TIMEOUT_OPTIONS)
    assert "http" in str(exc_info.value).lower() or "connect" in str(exc_info.value).lower()


def test_scan_readstat_por_s3_url_reaches_network_layer():
    with pytest.raises(Exception) as exc_info:
        prs.scan_readstat(
            "s3://fake-bucket/fake.por",
            storage_options=FAST_TIMEOUT_OPTIONS,
        ).collect()
    assert "http" in str(exc_info.value).lower() or "connect" in str(exc_info.value).lower()


def test_unrecognized_storage_option_key_is_ignored_not_rejected():
    """`object_store::parse_url_opts` silently drops config keys it doesn't
    recognize (the same forward-compatible behavior Polars' own
    `storage_options` has) rather than erroring on them — so an unknown key
    alongside otherwise-valid, fast-failing config still reaches the
    (unreachable) endpoint instead of failing at parse time."""
    options = {**FAST_TIMEOUT_OPTIONS, "this_is_not_a_real_option": "value"}
    with pytest.raises(Exception) as exc_info:
        prs.scan_readstat(
            "s3://fake-bucket/fake.sas7bdat",
            storage_options=options,
        ).collect()
    assert "http" in str(exc_info.value).lower() or "connect" in str(exc_info.value).lower()


@pytest.mark.parametrize("ext", ["dta", "sav", "xpt", "por"])
def test_write_readstat_s3_url_reaches_network_layer(ext):
    """Same proof as the read-side tests above, but for writes: a
    `storage_options` + `s3://` write attempt must reach `object_store`'s
    real (multipart-upload) network layer — not fail with a Python-side
    TypeError or a local-path error — even though there's nothing at the
    other end to actually receive it."""
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    with pytest.raises(Exception) as exc_info:
        prs.write_readstat(df, f"s3://fake-bucket/out.{ext}", storage_options=FAST_TIMEOUT_OPTIONS)
    assert "http" in str(exc_info.value).lower() or "connect" in str(exc_info.value).lower()


@pytest.mark.parametrize("ext", ["dta", "sav", "xpt", "por"])
def test_write_readstat_gs_url_reaches_network_layer(ext):
    """Same proof as `test_write_readstat_s3_url_reaches_network_layer`, but
    for Google Cloud Storage — same `write_readstat`/`storage_options`
    plumbing, just a `gs://` URL and GCS's config-key vocabulary.

    Unlike S3/Azure, a fake `service_account_key` fails GCS credential
    *parsing* locally (no network round trip) rather than failing an actual
    HTTP request — object_store validates the service-account JSON before
    ever using `google_base_url`. That's still proof the URL/scheme/config
    keys were recognized and routed into GCS-specific machinery (a routing
    or arg-binding failure would look completely different), so this
    asserts on the GCS-specific error instead of "http"/"connect" — and it's
    the fast, deterministic failure mode, unlike falling back to a real (and
    slow) GCE metadata-server token request when no key is given at all."""
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    with pytest.raises(Exception) as exc_info:
        prs.write_readstat(df, f"gs://fake-bucket/out.{ext}", storage_options=GCS_FAST_TIMEOUT_OPTIONS)
    assert "gcs" in str(exc_info.value).lower()


@pytest.mark.parametrize("ext", ["dta", "sav", "xpt", "por"])
def test_write_readstat_az_url_reaches_network_layer(ext):
    """Same proof as `test_write_readstat_s3_url_reaches_network_layer`, but
    for Azure Blob Storage — same `write_readstat`/`storage_options`
    plumbing, just an `az://` URL and Azure's config-key vocabulary."""
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    with pytest.raises(Exception) as exc_info:
        prs.write_readstat(df, f"az://fake-container/out.{ext}", storage_options=AZURE_FAST_TIMEOUT_OPTIONS)
    assert "http" in str(exc_info.value).lower() or "connect" in str(exc_info.value).lower()


def test_non_s3_path_ignores_storage_options():
    """storage_options is only consulted for s3:// paths — passing it
    alongside a local path should have zero effect."""
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / "crates/polars_readstat_rs/tests/sas/data/data_pandas/test1.sas7bdat"
    )
    if not path.exists():
        pytest.skip(f"fixture not found: {path}")

    df_plain = prs.scan_readstat(str(path)).collect()
    df_with_unused_options = prs.scan_readstat(
        str(path), storage_options={"aws_access_key_id": "irrelevant"}
    ).collect()
    assert df_plain.equals(df_with_unused_options)
