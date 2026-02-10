from __future__ import annotations

from pathlib import Path

import pytest

import polars_readstat as prs

REPO_ROOT = Path(__file__).resolve().parents[1]
RS_TESTS_ROOT = (REPO_ROOT / "../polars_readstat_rs/tests").resolve()


@pytest.fixture(scope="session")
def package_module():
    return prs


@pytest.fixture(scope="session")
def rs_tests_root() -> Path:
    if not RS_TESTS_ROOT.exists():
        pytest.skip(f"Rust fixture directory not found: {RS_TESTS_ROOT}")
    return RS_TESTS_ROOT


@pytest.fixture(scope="session")
def sas_fixture(rs_tests_root: Path) -> Path:
    path = rs_tests_root / "sas/data/data_pandas/test1.sas7bdat"
    if not path.exists():
        pytest.skip(f"Missing SAS fixture: {path}")
    return path


@pytest.fixture(scope="session")
def stata_fixture(rs_tests_root: Path) -> Path:
    path = rs_tests_root / "stata/data/stata15.dta"
    if not path.exists():
        pytest.skip(f"Missing Stata fixture: {path}")
    return path


@pytest.fixture(scope="session")
def spss_fixture(rs_tests_root: Path) -> Path:
    path = rs_tests_root / "spss/data/sample.sav"
    if not path.exists():
        pytest.skip(f"Missing SPSS fixture: {path}")
    return path
