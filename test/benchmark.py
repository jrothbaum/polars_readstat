import os
os.environ["_RJEM_MALLOC_CONF"]="muzzy_decay_ms:0"
os.environ["MALLOC_TRIM_THRESHOLD_"]="0"


import sys
from pathlib import Path
import pandas as pd
import polars as pl
import polars.selectors as cs
import pyreadstat as prs
from polars_readstat import scan_readstat, ScanReadstat


TOO_BIG = Path("/home/jrothbaum/Coding/claude_code/polars_readstat/crates")

FIXTURE = {
    "sas7bdat": dict(
        path=str(TOO_BIG / "polars_readstat_rs/tests/sas/data/too_big/psam_p17.sas7bdat"),
        f_pandas=pd.read_sas,
        f_pyreadstat=prs.read_sas7bdat,
        pandas_col_kwarg=None,  # pd.read_sas has no column selection
        threads=None,           # default (all cores)
        subset_columns=["SERIALNO", "STATE", "PINCP"],
        filter_col="PINCP", filter_val=5000,
    ),
    "stata": dict(
        path=str(TOO_BIG / "polars_readstat_rs/tests/stata/data/too_big/usa_00009.dta"),
        f_pandas=pd.read_stata,
        f_pyreadstat=prs.read_dta,
        pandas_col_kwarg="columns",
        threads=None,
        subset_columns=["index", "YEAR", "SAMPLE", "BIRTHYR"],
        filter_col="BIRTHYR", filter_val=1980,
    ),
    "spss": dict(
        path=str(TOO_BIG / "polars_readstat_rs/tests/spss/data/too_big/anes_timeseries_cdf_spss_20260205.sav"),
        f_pandas=pd.read_spss,
        f_pyreadstat=prs.read_sav,
        pandas_col_kwarg="usecols",
        threads=4,
        subset_columns=["VCF0004", "VCF0006", "VCF0006a"],
        filter_col="VCF0004", filter_val=2020,
    ),
    "zsav": dict(
        path=str(TOO_BIG / "polars_readstat_rs/tests/spss/data/too_big/psam_p17.zsav"),
        f_pandas=pd.read_spss,
        f_pyreadstat=prs.read_sav,
        pandas_col_kwarg="usecols",
        threads=None,
        subset_columns=["SERIALNO", "STATE", "PINCP"],
        filter_col="PINCP", filter_val=5000,
    ),
    "xpt": dict(
        path=str(TOO_BIG / "polars_readstat_rs/tests/sas/data/too_big/numeric_1000000_2.xpt"),
        f_pandas=pd.read_sas,
        f_pyreadstat=prs.read_xport,
        pandas_col_kwarg=None,  # pd.read_sas has no column selection
        threads=None,
        subset_columns=["VAR1"],
        filter_col="VAR1", filter_val=500000,
    ),
}


if __name__ == "__main__":
    def read_multithreaded_test(fmt: str,
                                subset_columns: bool = False,
                                filter_rows: bool = False):
        import time

        fix = FIXTURE[fmt]
        path = fix["path"]
        f_pandas = fix["f_pandas"]
        f_pyreadstat = fix["f_pyreadstat"]
        pandas_col_kwarg = fix["pandas_col_kwarg"]
        threads = fix["threads"] or pl.thread_pool_size()
        columns = fix["subset_columns"] if subset_columns else None
        filter_col = fix["filter_col"]
        filter_val = fix["filter_val"]

        engines = [
            "polars_readstat",
            "pandas",
            # "pyreadstat",
        ]

        d_timings = {}
        for enginei in engines:
            print(f"Running {enginei}")
            start = time.time()

            if enginei.startswith("polars"):
                df = scan_readstat(path, threads=threads)
                if subset_columns:
                    df = df.select(columns)
                if filter_rows:
                    df = df.filter(pl.col(filter_col) >= filter_val)
                df = df.collect()
            elif enginei == "pandas":
                if subset_columns and pandas_col_kwarg:
                    df = f_pandas(path, **{pandas_col_kwarg: columns})
                else:
                    df = f_pandas(path)
                    if subset_columns:
                        df = df[columns]
                if filter_rows:
                    df = df[df[filter_col] >= filter_val]
            elif enginei == "pyreadstat":
                (df, _) = prs.read_file_multiprocessing(
                    f_pyreadstat, path,
                    num_processes=threads,
                    usecols=columns,
                )
                if filter_rows:
                    df = df[df[filter_col] >= filter_val]

            elapsed = time.time() - start
            d_timings[enginei] = elapsed

            try:
                del df
            except Exception:
                pass

        for enginei in engines:
            print(f"     {enginei:<16}{d_timings[enginei]:.2f}")

    def test(fmt: str):
        print(f"\n=== {fmt} ===")
        read_multithreaded_test(fmt, subset_columns=False, filter_rows=False)
        read_multithreaded_test(fmt, subset_columns=True,  filter_rows=False)
        read_multithreaded_test(fmt, subset_columns=False, filter_rows=True)
        read_multithreaded_test(fmt, subset_columns=True,  filter_rows=True)

    test("sas7bdat")
    # test("stata")
    # test("spss")
    # test("zsav")
    # test("xpt")
