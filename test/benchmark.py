import sys
from pathlib import Path
import pandas as pd
import polars as pl
import pyreadstat as prs

# Load the test library
prs_path = f"{Path(__file__).parent.parent}/polars_readstat/polars_readstat/"
sys.path.insert(0, str(prs_path))

from polars_readstat import scan_readstat






if __name__ == "__main__":
    def read_test(path:str) -> None:
        print(path)
        df = scan_readstat(path)
        print(df.collect_schema())
        print(df.collect())
        df = df.head(2)
        #   print(df.collect())
        
        
        print(df.collect_schema())

        df = df.filter(pl.col("mylabl") > 1.5)

        
        df = df.select("mychar",
                        "mynum",
                        "myord")        
        df = df.collect()
        print(df)
        print("\n\n\n")

    def read_multithreaded_test_dta(subset_columns:bool=False,
                                    filter_rows:bool=False,
                                    use_pyreadstat:bool=False):
        import time

        path = "/home/jrothbaum/Downloads/usa_00008.dta"

        columns = None
        if subset_columns:
            columns = ["index",
                        "YEAR",
                        "SAMPLE",
                        "BIRTHYR"]
            
        
        start_pl = time.time()
        df = scan_readstat(path)
        if subset_columns:
            df = df.select(columns)
        if filter_rows:
            df = df.filter(pl.col("BIRTHYR") >= 1980)
            
        df = df.collect()
        elapsed_pl = time.time() - start_pl
        # print(df.schema)
        # print(df)
        del df
        if use_pyreadstat:
            import pyreadstat as prs
            start_pd = time.time()
            (dfp,_) = prs.read_file_multiprocessing(prs.read_dta,
                                                    path,
                                                    num_processes=pl.thread_pool_size(),
                                                    usecols=columns)
            if filter_rows:
                dfp = dfp[dfp['BIRTHYR'] >= 1980]
            dfp = pl.from_pandas(dfp)
            elapsed_pd = time.time() - start_pd
        else:
            import pandas as pd

            start_pd = time.time()
            dfp = pd.read_stata(path)
            if subset_columns:
                dfp = dfp[columns]
            if filter_rows:
                dfp = dfp[dfp['BIRTHYR'] >= 1980]
            dfp = pl.from_pandas(dfp)
            elapsed_pd = time.time() - start_pd
        # print(dfp)
        del dfp
        
        
        print(f"Subset: {subset_columns}, Filter: {filter_rows}")
        print(f"    Polars: {elapsed_pl}")
        if use_pyreadstat:
            print(f"    Pyreadstat: {elapsed_pd}")
        else:
            print(f"    Pandas: {elapsed_pd}")
        #   print(f"    Are identical: {dfp.equals(df)}")
    # read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.sas7bdat")

    # read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.dta")

    def dta_test(use_pyreadstat:bool=False):
        print("\n\n\nStata")
        read_multithreaded_test_dta(subset_columns=False,
                                    filter_rows=False,
                                    use_pyreadstat=use_pyreadstat)
        read_multithreaded_test_dta(subset_columns=True,
                                    filter_rows=False,
                                    use_pyreadstat=use_pyreadstat)
        read_multithreaded_test_dta(subset_columns=False,
                                    filter_rows=True,
                                    use_pyreadstat=use_pyreadstat)
        read_multithreaded_test_dta(subset_columns=True,
                                    filter_rows=True,
                                    use_pyreadstat=use_pyreadstat)
        

    def read_multithreaded_test_sas7bdat(subset_columns:bool=False,
                                    filter_rows:bool=False,
                                    use_pyreadstat:bool=False):
        import time

        path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat"

        columns = None
        if subset_columns:
            columns = ["SERIALNO",
                       "STATE",
                       "PINCP"]
            
        
        start_pl = time.time()
        df = scan_readstat(path)
        if subset_columns:
            df = df.select(columns)
        if filter_rows:
            df = df.filter(pl.col("PINCP") >= 5000)
        
        df = df.collect()
        elapsed_pl = time.time() - start_pl
        #   print(df.schema)
        print(df.shape)
        #   print(df.glimpse())
        df_describe = df.describe()
        del df

        if use_pyreadstat:
            import pyreadstat as prs
            start_pd = time.time()
            (dfp,_) = prs.read_file_multiprocessing(prs.read_sas7bdat,
                                                    path,
                                                    num_processes=pl.thread_pool_size(),
                                                    usecols=columns)
            if filter_rows:
                dfp = dfp[dfp['PINCP'] >= 5000]
            dfp = pl.from_pandas(dfp)
            elapsed_pd = time.time() - start_pd
        else:
            import pandas as pd

            start_pd = time.time()
            dfp = pd.read_sas(path)
            if subset_columns:
                dfp = dfp[columns]
            if filter_rows:
                dfp = dfp[dfp['PINCP'] >= 5000]
            dfp = pl.from_pandas(dfp)
            elapsed_pd = time.time() - start_pd
            # print(dfp)
        
        #   print(dfp.schema)
        print(dfp.shape)
        dfp = dfp.with_columns(pl.col(pl.Binary).cast(pl.String))
        #   print(dfp.glimpse())
        dfp_describe = dfp.describe()
        del dfp
        
        
        print(f"Subset: {subset_columns}, Filter: {filter_rows}")
        print(f"    Polars: {elapsed_pl}")
        if use_pyreadstat:
            print(f"    Pyreadstat: {elapsed_pd}")
        else:
            print(f"    Pandas: {elapsed_pd}")
        print(f"    Are identical: {dfp_describe.equals(df_describe)}")

    def sas7bdat_test(use_pyreadstat:bool=False):

        path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat"
        
        print("\n\n\nSAS")
        read_multithreaded_test_sas7bdat(subset_columns=False,
                                         filter_rows=False,
                                         use_pyreadstat=use_pyreadstat)
        read_multithreaded_test_sas7bdat(subset_columns=True,
                                         filter_rows=False,
                                         use_pyreadstat=use_pyreadstat)
        read_multithreaded_test_sas7bdat(subset_columns=False,
                                         filter_rows=True,
                                         use_pyreadstat=use_pyreadstat)
        read_multithreaded_test_sas7bdat(subset_columns=True,
                                         filter_rows=True,
                                         use_pyreadstat=use_pyreadstat)

    sas7bdat_test(use_pyreadstat=False)
    # dta_test(use_pyreadstat=False)

    # read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat_rs/tests/data/sample.sav")
    # read_test("/home/jrothbaum/Downloads/pyreadstat-master/test_data/ínternátionál/sample.sas7bdat")

    # for filei in [
    #                 "/home/jrothbaum/Downloads/pyreadstat-master/test_data/missing_data/missing_test.dta",
    #                 "/home/jrothbaum/Downloads/pyreadstat-master/test_data/multiple_response/simple_alltypes.sav",
    #                 "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/hebrews.sav",
    #                 "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/ordered_category.sav",
    #               "/home/jrothbaum/Downloads/haven-main/tests/testthat/stata/datetime-d.dta"
    #               ]:
        
    #     df = scan_readstat(filei)
    #     # df = df.head(2)
    #     print(df.collect())
