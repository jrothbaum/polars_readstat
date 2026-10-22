"""Run inside a venv containing ONLY polars + polars-readstat (stdlib otherwise).

Usage: python clean_venv_s3_check.py <bucket> <prefix> <region> <local_sample_dir>

For each format: read the local sample, read the same file from S3 (uploaded
by the runner with the AWS CLI), compare; then write the frame to S3 and read
it back. Credentials come from the standard AWS_* environment variables.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import polars as pl
import polars_readstat as prs

bucket, prefix, region, sample_dir = sys.argv[1:5]
sample_dir = Path(sample_dir)
import os

# Explicit credentials from the standard env vars (object_store's
# parse_url_opts does not read the environment itself).
_ENV_KEYS = {
    "AWS_ACCESS_KEY_ID": "aws_access_key_id",
    "AWS_SECRET_ACCESS_KEY": "aws_secret_access_key",
    "AWS_SESSION_TOKEN": "aws_session_token",
}
opts = {"aws_region": region}
opts.update({v: os.environ[k] for k, v in _ENV_KEYS.items() if os.environ.get(k)})

# Formats readstat can write. sas7bdat has no binary writer (read-only).
READ_FORMATS = ["sas7bdat", "dta", "sav", "zsav", "xpt", "por"]
WRITE_FORMATS = {"dta", "sav", "zsav", "xpt", "por"}

results: list[tuple[str, str, str]] = []  # (format, step, outcome)


def record(fmt: str, step: str, fn) -> object:
    try:
        out = fn()
        results.append((fmt, step, "PASS"))
        return out
    except Exception as e:  # report every failure, keep going
        results.append((fmt, step, f"FAIL: {type(e).__name__}: {str(e)[:150]}"))
        return None


print(f"polars {pl.__version__}  polars_readstat {getattr(prs, '__version__', '?')}")

# Informational probe: does S3 work with ONLY the AWS_* env vars (no explicit
# credentials in storage_options), the way pl.scan_parquet does? Short timeouts
# so a miss fails fast instead of walking the EC2-metadata retry chain.
_dta = next(sample_dir.glob("*.dta"))
record(
    "env-only",
    "read s3 (no explicit creds)",
    lambda: prs.scan_readstat(
        f"s3://{bucket}/{prefix}/in/{_dta.name}",
        storage_options={"aws_region": region, "aws_connect_timeout": "2s", "aws_timeout": "3s"},
    ).collect(),
)

for ext in READ_FORMATS:
    sample = next(sample_dir.glob(f"*.{ext}"))
    local = record(ext, "read local", lambda: prs.scan_readstat(str(sample)).collect())
    if local is None:
        continue

    remote = record(
        ext,
        "read s3",
        lambda: prs.scan_readstat(
            f"s3://{bucket}/{prefix}/in/{sample.name}", storage_options=opts
        ).collect(),
    )
    if remote is not None:
        same = remote.equals(local)
        results.append((ext, "s3 == local", "PASS" if same else "FAIL: frames differ"))

    if ext not in WRITE_FORMATS:
        results.append((ext, "write s3", "SKIP (no binary writer)"))
        continue

    url = f"s3://{bucket}/{prefix}/out/out.{ext}"
    wrote = record(
        ext, "write s3", lambda: prs.write_readstat(local, url, storage_options=opts)
    )
    if wrote is None and results[-1][2] != "PASS":
        continue
    back = record(
        ext,
        "read back",
        lambda: prs.scan_readstat(url, storage_options=opts).collect(),
    )
    if back is not None:
        ok = back.shape == local.shape and back.columns == local.columns
        results.append((ext, "round-trip shape", "PASS" if ok else f"FAIL: {back.shape} vs {local.shape}"))

# sas7bcat: standalone read from S3, then a cloud catalog used via catalog=.
cat_dir = sample_dir / "catalog"
if cat_dir.exists():
    dat = cat_dir / "tagged-na.sas7bdat"
    cat = cat_dir / "tagged-na.sas7bcat"
    base = f"s3://{bucket}/{prefix}/in/catalog"
    local_cat = record("sas7bcat", "read local", lambda: prs.read_sas7bcat(str(cat)))
    s3_cat = record(
        "sas7bcat", "read s3", lambda: prs.read_sas7bcat(f"{base}/{cat.name}", storage_options=opts)
    )
    if local_cat is not None and s3_cat is not None:
        results.append(("sas7bcat", "s3 == local", "PASS" if s3_cat == local_cat else "FAIL: catalogs differ"))

    # catalog= given as a cloud URL must resolve to the same catalog as a local path
    lc = record("sas7bcat", "catalog= local path", lambda: prs.ScanReadstat(str(dat), catalog=str(cat)).catalog)
    sc = record(
        "sas7bcat", "catalog= s3:// url",
        lambda: prs.ScanReadstat(
            f"{base}/{dat.name}", catalog=f"{base}/{cat.name}", storage_options=opts
        ).catalog,
    )
    if lc is not None and sc is not None:
        results.append(("sas7bcat", "catalog s3 == local", "PASS" if sc == lc else "FAIL: catalogs differ"))
    got = record(
        "sas7bcat", "scan s3 data+catalog",
        lambda: prs.scan_readstat(
            f"{base}/{dat.name}", catalog=f"{base}/{cat.name}", storage_options=opts
        ).collect(),
    )
    want = record("sas7bcat", "scan local data+catalog", lambda: prs.scan_readstat(str(dat), catalog=str(cat)).collect())
    if want is not None and got is not None:
        results.append(("sas7bcat", "frames s3 == local", "PASS" if got.equals(want) else "FAIL: frames differ"))

width = max(len(r[1]) for r in results)
print()
for fmt, step, outcome in results:
    print(f"{fmt:9} {step:<{width}}  {outcome}")
failed = [r for r in results if r[2].startswith("FAIL")]
print(f"\n{len(results) - len(failed)} ok/skip, {len(failed)} failed")
sys.exit(1 if failed else 0)
