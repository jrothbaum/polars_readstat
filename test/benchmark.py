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






if __name__ == "__main__":
    def read_multithreaded_test(subset_columns:bool=False,
                                filter_rows:bool=False,
                                stata:bool=False,
                                spss:bool=False):
        import time

        if stata:
            path = "/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/stata/data/too_big/usa_00009.dta"
            f_pandas = pd.read_stata
            f_pyreadstat = prs.read_dta
            threads = pl.thread_pool_size()
        elif spss:
            path = "/home/jrothbaum/Coding/claude_code/polars_readstat_rs/tests/spss/data/too_big/anes_timeseries_cdf_spss_20260205.sav"
            f_pandas = pd.read_spss
            f_pyreadstat = prs.read_sav

            threads = 4
        else:
            path = "/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/sas/data/too_big/psam_p17.sas7bdat"
            f_pandas = pd.read_sas
            f_pyreadstat = prs.read_sas7bdat
            threads = pl.thread_pool_size()
        columns = None
        if subset_columns:
            if stata:
                columns = ["index",
                        "YEAR",
                        "SAMPLE",
                        "BIRTHYR"]
            elif spss:
                columns = ['VCF0004', 'VCF0006', 'VCF0006a']
            else:
                columns = ["SERIALNO",
                        "STATE",
                        "PINCP"]
            
        
        
        if stata:
            engines = ["polars_readstat",
                       "pandas",
                    #    "pyreadstat"
                       ]
        elif spss:
            engines = ["polars_readstat",
                       "pandas",
                    #    "pyreadstat"
                       ]

        else:
            engines = ["polars_readstat",
                       # "polars_cpp",
                       "pandas",
                    #    "pyreadstat"
                       ]
        d_timings = {}
        for enginei in engines:
            print(f"Running {enginei}")
            start = time.time()

            if enginei.startswith("polars"):
                polars_engine = enginei[7:]
                df = scan_readstat(path,
                                #    engine=polars_engine,
                                   threads=threads)
                if subset_columns:
                    df = df.select(columns) 
                if filter_rows:
                    if stata:
                        df = df.filter(pl.col("BIRTHYR") >= 1980)
                    elif spss:
                        df = df.filter(pl.col("VCF0004") >= 2020)
                    else:
                        df = df.filter(pl.col("PINCP") >= 5000)
                
                df = df.collect()
            elif enginei == "pandas":
                df = f_pandas(path)
                if subset_columns:
                    df = df[columns]
                if filter_rows:
                    if stata:
                        df = df[df['BIRTHYR'] >= 1980]
                    elif spss:
                        df = df[df['VCF0004'] >= 2020]
                    else:
                        df = df[df['PINCP'] >= 5000]
                # dfp = pl.from_pandas(dfp)
            elif enginei == "pyreadstat":
                (df,_) = prs.read_file_multiprocessing(f_pyreadstat,
                                                        path,
                                                        num_processes=threads,
                                                        usecols=columns)
                if stata:
                    df = df[df['BIRTHYR'] >= 1980]
                elif spss:
                    df = df[df['VCF0004'] >= 2020]
                else:
                    df = df[df['PINCP'] >= 5000]
            #     # dfp = pl.from_pandas(dfp)
            elapsed = time.time() - start
            d_timings[enginei] = elapsed
            
            try:
                del df
            except:
                pass
        
        for enginei in engines:
            print(f"     {enginei:<16}{d_timings[enginei]:.2f}")

    def test(stata:bool=False,spss:bool=False):

        read_multithreaded_test(subset_columns=False,
                                filter_rows=False,
                                stata=stata,
                                spss=spss)
        read_multithreaded_test(subset_columns=True, 
                                filter_rows=False,
                                stata=stata,
                                spss=spss)
        read_multithreaded_test(subset_columns=False,
                                filter_rows=True,
                                stata=stata,
                                spss=spss)
        read_multithreaded_test(subset_columns=True,
                                filter_rows=True,
                                stata=stata,
                                spss=spss)

    test(stata=False,
         spss=False)

