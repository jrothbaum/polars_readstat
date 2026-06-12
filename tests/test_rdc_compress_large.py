"""Regression test for RDC-compressed SAS files with millions of rows.

Before the fix, reading compress_test_16.sas7bdat with parallel workers
raised: RuntimeError: Decompression failed: RDC: Invalid offset 4098 with
current position 0
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_FILE = (
    REPO_ROOT
    / "crates/polars_readstat_rs/tests/sas/data/too_big/compress_test_16.sas7bdat"
)


@pytest.fixture(scope="module")
def rdc_file() -> Path:
    if not TEST_FILE.exists():
        pytest.skip(f"Large RDC test file not found: {TEST_FILE}")
    return TEST_FILE


def test_rdc_compressed_read_matches_pyreadstat(package_module, rdc_file: Path):
    pyreadstat = pytest.importorskip("pyreadstat")

    df_ours = package_module.read_readstat(str(rdc_file))
    df_pyreadstat, _ = pyreadstat.read_sas7bdat(str(rdc_file))

    assert df_ours.height == len(df_pyreadstat), (
        f"Row count mismatch: polars_readstat={df_ours.height}, "
        f"pyreadstat={len(df_pyreadstat)}"
    )
    assert df_ours.width == len(df_pyreadstat.columns), (
        f"Column count mismatch: polars_readstat={df_ours.width}, "
        f"pyreadstat={len(df_pyreadstat.columns)}"
    )
