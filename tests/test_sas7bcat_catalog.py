from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

pyreadstat = pytest.importorskip("pyreadstat")

REPO_ROOT = Path(__file__).resolve().parents[1]
SAS_FORMAT_DIR = (
    REPO_ROOT / "crates/polars_readstat_rs/tests/sas/data/too_big/sas_format"
)
DAT_PATH = SAS_FORMAT_DIR / "crash_hospital.sas7bdat"
CAT_PATH = SAS_FORMAT_DIR / "formats.sas7bcat"


@pytest.fixture(scope="module")
def catalog(package_module):
    if not CAT_PATH.exists():
        pytest.skip(f"Missing catalog fixture: {CAT_PATH}")
    return package_module.read_sas7bcat(CAT_PATH)


@pytest.fixture(scope="module")
def pyreadstat_meta():
    if not DAT_PATH.exists() or not CAT_PATH.exists():
        pytest.skip("Missing SAS format fixtures")
    _, meta = pyreadstat.read_sas7bdat(
        str(DAT_PATH),
        catalog_file=str(CAT_PATH),
        formats_as_category=False,
        row_limit=0,
        metadataonly=True,
    )
    return meta


@pytest.fixture(scope="module")
def pyreadstat_df():
    if not DAT_PATH.exists() or not CAT_PATH.exists():
        pytest.skip("Missing SAS format fixtures")
    df, _ = pyreadstat.read_sas7bdat(
        str(DAT_PATH),
        catalog_file=str(CAT_PATH),
        formats_as_category=False,
    )
    return df


# ---------------------------------------------------------------------------
# read_sas7bcat
# ---------------------------------------------------------------------------


def test_read_sas7bcat_format_count(catalog):
    assert len(catalog) == 10


def test_read_sas7bcat_format_names(catalog):
    expected = {
        "AGEFMT", "ALCDRUGSFMT", "DEATHORINJUREDFMT", "DISTRACTFMT",
        "FATIGUEFMT", "RESTUSEFMT", "SEXFMT", "VEHEVENTFMT",
        "VEHEVENTSHORT", "YESNO",
    }
    assert set(catalog.keys()) == expected


def test_read_sas7bcat_sexfmt_entries(catalog):
    assert catalog["SEXFMT"] == {1.0: "Male", 2.0: "Female"}


def test_read_sas7bcat_yesno_entries(catalog):
    assert catalog["YESNO"] == {0.0: "No", 1.0: "Yes"}


def test_read_sas7bcat_veheventfmt_entries(catalog):
    assert catalog["VEHEVENTFMT"] == {
        0.0: "None",
        1.0: "Non-collision",
        2.0: "Collision with MV in transport",
        3.0: "Collision with non-fixed object",
        4.0: "Collision with fixed object",
    }


def test_read_sas7bcat_matches_pyreadstat(catalog, pyreadstat_meta):
    """Each column's value labels from pyreadstat should match our catalog lookup."""
    for col, py_labels in pyreadstat_meta.variable_value_labels.items():
        fmt = pyreadstat_meta.variable_value_labels.get(col)
        if fmt is None:
            continue
        # pyreadstat keys are already floats; compare directly
        for code, label in py_labels.items():
            fmt_name = pyreadstat_meta.original_variable_types.get(col, "")
            # Find which catalog entry covers this column by checking all formats
            found = False
            for cat_name, entries in catalog.items():
                if code in entries and entries[code] == label:
                    found = True
                    break
            assert found, (
                f"pyreadstat label ({code!r} -> {label!r}) for column {col!r} "
                f"not found in catalog"
            )


# ---------------------------------------------------------------------------
# catalog_labels property on ScanReadstat
# ---------------------------------------------------------------------------


def test_catalog_labels_columns(package_module, catalog):
    if not DAT_PATH.exists():
        pytest.skip(f"Missing fixture: {DAT_PATH}")
    r = package_module.ScanReadstat(str(DAT_PATH), catalog=catalog)
    col_labels = r.catalog_labels
    assert col_labels is not None
    assert "SEX" in col_labels
    assert "speedlimitGE55" in col_labels
    assert "VEHEVENT" in col_labels
    assert col_labels["SEX"] == {1.0: "Male", 2.0: "Female"}
    assert col_labels["speedlimitGE55"] == {0.0: "No", 1.0: "Yes"}


def test_catalog_labels_none_without_catalog(package_module):
    if not DAT_PATH.exists():
        pytest.skip(f"Missing fixture: {DAT_PATH}")
    r = package_module.ScanReadstat(str(DAT_PATH))
    assert r.catalog_labels is None


# ---------------------------------------------------------------------------
# scan_readstat with catalog
# ---------------------------------------------------------------------------


def test_scan_with_catalog_dtypes(package_module, catalog):
    if not DAT_PATH.exists():
        pytest.skip(f"Missing fixture: {DAT_PATH}")
    lf = package_module.scan_readstat(
        str(DAT_PATH),
        value_labels_as_strings=True,
        catalog=catalog,
    )
    schema = lf.collect_schema()
    for col in ("SEX", "speedlimitGE55", "VEHEVENT", "FATIGUE", "DISTRACT"):
        assert schema[col] == pl.String, f"{col} should be String after catalog application"


def test_scan_with_catalog_no_labels_when_disabled(package_module, catalog):
    """Without value_labels_as_strings, catalog should not change dtypes."""
    if not DAT_PATH.exists():
        pytest.skip(f"Missing fixture: {DAT_PATH}")
    lf = package_module.scan_readstat(
        str(DAT_PATH),
        value_labels_as_strings=False,
        catalog=catalog,
    )
    schema = lf.collect_schema()
    assert schema["SEX"] == pl.Float64


def test_scan_with_catalog_from_path(package_module):
    """Passing a file path as catalog should produce the same result as a pre-loaded dict."""
    if not DAT_PATH.exists() or not CAT_PATH.exists():
        pytest.skip("Missing SAS format fixtures")
    df_path = package_module.scan_readstat(
        str(DAT_PATH),
        value_labels_as_strings=True,
        catalog=str(CAT_PATH),
        preserve_order=True,
    ).select(["SEX", "speedlimitGE55"]).head(100).collect()

    cat = package_module.read_sas7bcat(CAT_PATH)
    df_dict = package_module.scan_readstat(
        str(DAT_PATH),
        value_labels_as_strings=True,
        catalog=cat,
        preserve_order=True,
    ).select(["SEX", "speedlimitGE55"]).head(100).collect()

    assert df_path.equals(df_dict)


def test_scan_with_catalog_matches_pyreadstat(package_module, pyreadstat_df):
    """Labeled values in catalog-covered columns should match pyreadstat."""
    if not DAT_PATH.exists() or not CAT_PATH.exists():
        pytest.skip("Missing SAS format fixtures")

    labeled_cols = ["SEX", "speedlimitGE55", "VEHEVENT", "FATIGUE", "DISTRACT"]

    df_ours = package_module.scan_readstat(
        str(DAT_PATH),
        value_labels_as_strings=True,
        catalog=str(CAT_PATH),
        preserve_order=True,
    ).select(labeled_cols).collect()

    import math

    def _eq(a, b) -> bool:
        # None (Polars null) and float NaN (pandas missing) are both "missing"
        a_missing = a is None or (isinstance(a, float) and math.isnan(a))
        b_missing = b is None or (isinstance(b, float) and math.isnan(b))
        if a_missing and b_missing:
            return True
        return a == b

    for col in labeled_cols:
        prs_vals = df_ours[col].to_list()
        py_vals = pyreadstat_df[col].tolist()
        mismatches = [
            (i, prs_vals[i], py_vals[i])
            for i in range(len(prs_vals))
            if not _eq(prs_vals[i], py_vals[i])
        ]
        assert not mismatches, (
            f"Column {col!r}: first mismatch at row {mismatches[0][0]}: "
            f"ours={mismatches[0][1]!r}, pyreadstat={mismatches[0][2]!r}"
        )
