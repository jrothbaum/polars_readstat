"""Real-S3 end-to-end tests. Skipped unless PRS_TEST_S3_BUCKET is set.

Credentials come from the standard AWS_* env vars (see README.md).
"""
from __future__ import annotations

import os
import subprocess
import uuid

import polars as pl
import pytest

import polars_readstat as prs

BUCKET = os.environ.get("PRS_TEST_S3_BUCKET")
REGION = os.environ.get("AWS_REGION", "us-east-2")

pytestmark = pytest.mark.skipif(
    not BUCKET, reason="set PRS_TEST_S3_BUCKET to run real-S3 tests"
)

# object_store's parse_url_opts does not read AWS_* itself, so pass them explicitly.
OPTS = {"aws_region": REGION}
for _env, _key in (
    ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
    ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
    ("AWS_SESSION_TOKEN", "aws_session_token"),
):
    if os.environ.get(_env):
        OPTS[_key] = os.environ[_env]


@pytest.fixture(scope="module")
def prefix():
    p = f"run-{uuid.uuid4().hex[:8]}"
    yield p
    profile = os.environ.get("AWS_PROFILE_NAME")
    cmd = ["aws", "s3", "rm", f"s3://{BUCKET}/{p}/", "--recursive"]
    if profile and not os.environ.get("AWS_ACCESS_KEY_ID"):
        cmd += ["--profile", profile]
    subprocess.run(cmd, check=False, capture_output=True)


def _frame(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "i": pl.Series(range(n), dtype=pl.Int64),
            "x": [i * 0.5 for i in range(n)],
            "s": [f"row-{i}" for i in range(n)],
        }
    )


@pytest.mark.parametrize("ext", ["dta", "sav", "xpt"])
def test_write_then_read_roundtrip(prefix, ext):
    df = _frame(1_000)
    url = f"s3://{BUCKET}/{prefix}/rt.{ext}"
    prs.write_readstat(df, url, storage_options=OPTS)
    out = prs.scan_readstat(url, storage_options=OPTS).collect()
    assert out.shape == df.shape
    assert out["i"].to_list() == df["i"].to_list()


def test_large_write_triggers_multipart(prefix):
    df = _frame(2_000_000)  # tens of MB -> multipart upload
    url = f"s3://{BUCKET}/{prefix}/big.dta"
    prs.write_readstat(df, url, storage_options=OPTS)
    out = prs.scan_readstat(url, storage_options=OPTS).select(pl.len()).collect()
    assert out.item() == df.height


def test_s3_read_matches_local_read(prefix, tmp_path):
    df = _frame(5_000)
    local = tmp_path / "x.dta"
    prs.write_readstat(df, str(local))
    url = f"s3://{BUCKET}/{prefix}/x.dta"
    prs.write_readstat(df, url, storage_options=OPTS)
    a = prs.scan_readstat(str(local)).collect()
    b = prs.scan_readstat(url, storage_options=OPTS).collect()
    assert a.equals(b)


def test_missing_key_errors_clearly():
    with pytest.raises(Exception) as e:
        prs.scan_readstat(
            f"s3://{BUCKET}/does-not-exist.dta", storage_options=OPTS
        ).collect()
    assert "404" in str(e.value) or "not found" in str(e.value).lower()


def test_bad_credentials_error():
    bad = {**OPTS, "aws_access_key_id": "AKIAINVALID", "aws_secret_access_key": "nope"}
    with pytest.raises(Exception):
        prs.scan_readstat(f"s3://{BUCKET}/x.dta", storage_options=bad).collect()
