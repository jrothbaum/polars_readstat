"""Run inside a specific `polars` version's environment (via `uv run --with polars==X.Y.Z`)
to check that polars_readstat can load small SAS/Stata/SPSS fixtures and that the
returned DataFrames behave normally under that polars version.

Not a pytest test: invoked as a subprocess by polars_compat_check.py, one polars
version per process. Exits non-zero (with a traceback on stderr) on any failure;
prints a single "PROBE_OK ..." line on success.
"""
import json
import sys


def check_file(prs, pl, path: str, *, needs_date: bool = False) -> dict:
    df = prs.scan_readstat(path).collect()
    if df.height == 0:
        raise AssertionError(f"{path}: no rows")

    dtypes = df.dtypes
    has_string = any(dt == pl.String for dt in dtypes)
    has_numeric = any(dt.is_numeric() for dt in dtypes)
    if not has_string:
        raise AssertionError(f"{path}: expected at least one String column, got {dtypes}")
    if not has_numeric:
        raise AssertionError(f"{path}: expected at least one numeric column, got {dtypes}")
    if needs_date:
        has_temporal = any(dt.is_temporal() for dt in dtypes)
        if not has_temporal:
            raise AssertionError(f"{path}: expected at least one temporal column, got {dtypes}")

    # Exercise the returned columns like real usage would: a numeric reduction
    # and a string comparison/filter, both of which touch polars' own dtype
    # and expression machinery rather than just the Arrow import boundary.
    numeric_col = next(name for name, dt in zip(df.columns, dtypes) if dt.is_numeric())
    string_col = next(name for name, dt in zip(df.columns, dtypes) if dt == pl.String)
    _ = df.select(pl.col(numeric_col).sum())
    _ = df.filter(pl.col(string_col).is_not_null())

    return {"path": path, "shape": df.shape, "dtypes": [str(dt) for dt in dtypes]}


def main() -> None:
    sas_path, stata_path, spss_path = sys.argv[1:4]

    import polars as pl
    import polars_readstat as prs

    results = {
        "polars_version": pl.__version__,
        "sas": check_file(prs, pl, sas_path, needs_date=True),
        "stata": check_file(prs, pl, stata_path, needs_date=True),
        "spss": check_file(prs, pl, spss_path, needs_date=True),
    }
    print("PROBE_OK " + json.dumps(results))


if __name__ == "__main__":
    main()
