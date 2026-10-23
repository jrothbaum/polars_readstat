"""One timed read in its own process (so peak RSS is per-measurement).

Usage: python bench_s3_read.py <local|s3> <full|select> <path_or_url> [region]
Set PRS_ENDPOINT=http://host:port to read from an S3-compatible server instead of AWS.
Needs only polars + polars-readstat. S3 credentials come from AWS_* env vars,
passed explicitly in storage_options (the library does not read the env itself).
"""
from __future__ import annotations

import os
import resource
import sys
import time

import polars as pl
import polars_readstat as prs

where, mode, target = sys.argv[1:4]
region = sys.argv[4] if len(sys.argv) > 4 else "us-east-2"

opts = None
if where == "s3" and os.environ.get("PRS_ENDPOINT"):
    # an S3-compatible server (e.g. a local nginx or moto): dummy credentials, plain HTTP
    opts = {"aws_endpoint_url": os.environ["PRS_ENDPOINT"], "aws_allow_http": "true",
            "aws_access_key_id": "x", "aws_secret_access_key": "x", "aws_region": "us-east-1"}
elif where == "s3":
    opts = {"aws_region": region}
    for env, key in (("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
                     ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
                     ("AWS_SESSION_TOKEN", "aws_session_token")):
        if os.environ.get(env):
            opts[key] = os.environ[env]


def rx_bytes() -> int:
    """Total received bytes across non-loopback interfaces (rough: includes other traffic)."""
    total = 0
    with open("/proc/net/dev") as f:
        for line in list(f)[2:]:
            name, data = line.split(":", 1)
            if name.strip() != "lo":
                total += int(data.split()[0])
    return total


lf = prs.scan_readstat(target, storage_options=opts)
if mode == "select":
    cols = lf.collect_schema().names()[:3]
    lf = lf.select(cols)

rx0, t0 = rx_bytes(), time.perf_counter()
df = lf.collect()
wall = time.perf_counter() - t0
rx = rx_bytes() - rx0
ru = resource.getrusage(resource.RUSAGE_SELF)

print(
    f"{where:5} {mode:6} rows={df.height:>9,} cols={df.width:>4} "
    f"wall={wall:6.2f}s cpu={ru.ru_utime + ru.ru_stime:6.2f}s "
    f"peakRSS={ru.ru_maxrss / 1024:7.0f}MB rx={rx / 1e6:7.1f}MB "
    f"(df={df.estimated_size() / 1e6:.0f}MB)"
)
