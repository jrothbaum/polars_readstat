
from typing import Any, Iterator
from polars.io.plugins import register_io_source
import polars as pl


from polars_readstat_rs import read_readstat 

def scan_readstat(path:str) -> pl.LazyFrame:
    
    def schema() -> pl.Schema:
        src = read_readstat(path,
                            0, 
                            0,
                            1)
        return src.schema()
    
    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None=1_000_000,
    ) -> Iterator[pl.DataFrame]:
        
        src = read_readstat(path,
                            batch_size,
                            n_rows,
                            threads=pl.thread_pool_size())
        
        schema = src.schema()

        if with_columns is not None:
            src.set_with_columns(with_columns)
        
        while (out := src.next()) is not None:
            if predicate is not None:
                out = out.filter(predicate)


            #   Cast to int8 and int16 when needed.  This is not ideal...
            with_intcast = []
            cols_int8_cast = src.cast_int8()
            if len(cols_int8_cast):
                with_intcast.append(pl.col(cols_int8_cast).cast(pl.Int8))
            cols_int16_cast = src.cast_int16()
            if len(cols_int16_cast):
                with_intcast.append(pl.col(cols_int16_cast).cast(pl.Int16))
            if len(with_intcast):
                out = out.with_columns(with_intcast)
            yield out



    return register_io_source(io_source=source_generator, schema=schema())



if __name__ == "__main__":
    def read_test(path:str) -> None:
        print(path)
        df = scan_readstat(path)
        print(df.collect_schema())
        print(df.collect())
        df = df.head(2)
        print(df.collect())
        
        df = df.select("mychar",
                    "mynum",
                    "myord")
        
        print(df.collect_schema())
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
        
        
        
        print(f"Subset: {subset_columns}, Filter: {filter_rows}")
        print(f"    Polars: {elapsed_pl}")
        if use_pyreadstat:
            print(f"    Pyreadstat: {elapsed_pd}")
        else:
            print(f"    Pandas: {elapsed_pd}")
        print(f"    Are identical: {dfp.equals(df)}")
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

        path = "/home/jrothbaum/Downloads/psam_p17.sas7bdat"

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
        # print(df.schema)
        # print(df)
        
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
        
        
        
        print(f"Subset: {subset_columns}, Filter: {filter_rows}")
        print(f"    Polars: {elapsed_pl}")
        if use_pyreadstat:
            print(f"    Pyreadstat: {elapsed_pd}")
        else:
            print(f"    Pandas: {elapsed_pd}")
        print(f"    Are identical: {dfp.equals(df)}")

    def sas7bdat_test(use_pyreadstat:bool=False):

        path = "/home/jrothbaum/Downloads/psam_p06.sas7bdat"
        
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

    # sas7bdat_test(use_pyreadstat=False)
    # dta_test(use_pyreadstat=False)
    read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat_rs/tests/data/sample.sav")