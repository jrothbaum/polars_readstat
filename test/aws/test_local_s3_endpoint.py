"""S3 behaviour against a local S3-compatible server (moto) — no AWS account needed.

Opt-in: skipped unless `moto` and `boto3` are installed
(`uv pip install "moto[server]"`). Runs the installed polars_readstat, so it works
against a PyPI wheel or a local build. Unlike test_aws_s3.py it needs no bucket or
credentials; credentials are dummies and are passed only through AWS_* environment
variables, so it also covers the "no storage_options, no region" resolution path.

    uv run pytest test/aws/test_local_s3_endpoint.py -v
"""
from __future__ import annotations

import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import polars as pl
import pytest

import polars_readstat as prs

pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")

DATA = Path(__file__).resolve().parents[2] / "crates/polars_readstat_rs/tests"
SAS = DATA / "sas/data"
BUCKET = "local-bkt"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def endpoint():
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(url, timeout=1)
                break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.fail("moto server did not start")
        boto3.client(
            "s3", endpoint_url=url, region_name="us-east-1",
            aws_access_key_id="testing", aws_secret_access_key="testing",
        ).create_bucket(Bucket=BUCKET)
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture(autouse=True)
def dummy_env(monkeypatch, tmp_path):
    """Credentials only via AWS_*; an empty HOME so ~/.aws files can't interfere."""
    for k in ("AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("HOME", str(tmp_path))


@pytest.fixture
def opts(endpoint):
    # Only the endpoint: credentials come from the environment and the region is
    # defaulted because a custom endpoint is set.
    return {"aws_endpoint_url": endpoint, "aws_allow_http": "true",
            "aws_connect_timeout": "2s", "aws_timeout": "30s"}


def _put(endpoint, key: str, path: Path) -> str:
    boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1",
        aws_access_key_id="testing", aws_secret_access_key="testing",
    ).upload_file(str(path), BUCKET, key)
    return f"s3://{BUCKET}/{key}"


def _frame(n: int) -> pl.DataFrame:
    return pl.DataFrame({"i": pl.Series(range(n), dtype=pl.Int64),
                         "x": [i * 0.5 for i in range(n)],
                         "s": [f"row-{i}" for i in range(n)]})


@pytest.mark.parametrize("name", ["data/file1.sas7bdat", "xpt/sample.xpt"])
def test_read_sas_files_matches_local(endpoint, opts, name):
    path = SAS / name
    url = _put(endpoint, f"in/{path.name}", path)
    assert prs.scan_readstat(url, storage_options=opts).collect().equals(
        prs.scan_readstat(str(path)).collect())


@pytest.mark.parametrize("name", ["stata/data/stata1_115.dta", "spss/data/sample.sav",
                                  "spss/data/sample.zsav", "spss/data/sample.por"])
def test_read_matches_local(endpoint, opts, name):
    path = DATA / name
    url = _put(endpoint, f"in/{path.name}", path)
    assert prs.scan_readstat(url, storage_options=opts).collect().equals(
        prs.scan_readstat(str(path)).collect())


@pytest.mark.parametrize("ext", ["dta", "sav", "zsav", "xpt", "por"])
def test_write_then_read_back(endpoint, opts, ext):
    df = _frame(2_000)
    url = f"s3://{BUCKET}/out/rt.{ext}"
    prs.write_readstat(df, url, storage_options=opts)
    back = prs.scan_readstat(url, storage_options=opts).collect()
    assert back.shape == df.shape
    # .por stores variable names in upper case
    assert [c.lower() for c in back.columns] == df.columns
    assert back[back.columns[0]].to_list() == df["i"].to_list()


def test_large_write_uses_multipart_and_reads_back(endpoint, opts):
    df = _frame(1_500_000)  # several tens of MB as .dta
    url = f"s3://{BUCKET}/out/big.dta"
    prs.write_readstat(df, url, storage_options=opts)
    assert prs.scan_readstat(url, storage_options=opts).select(pl.len()).collect().item() == df.height


def test_sas7bcat_catalog_as_s3_url(endpoint, opts):
    dat = SAS / "haven/tagged-na.sas7bdat"
    cat = SAS / "haven/tagged-na.sas7bcat"
    dat_url = _put(endpoint, "cat/tagged-na.sas7bdat", dat)
    cat_url = _put(endpoint, "cat/tagged-na.sas7bcat", cat)
    assert prs.read_sas7bcat(cat_url, storage_options=opts) == prs.read_sas7bcat(cat)
    got = prs.scan_readstat(dat_url, catalog=cat_url, storage_options=opts).collect()
    assert got.equals(prs.scan_readstat(str(dat), catalog=str(cat)).collect())


def test_missing_object_errors_quickly(endpoint, opts):
    t = time.perf_counter()
    with pytest.raises(Exception):
        prs.scan_readstat(f"s3://{BUCKET}/does-not-exist.dta", storage_options=opts).collect()
    assert time.perf_counter() - t < 20
