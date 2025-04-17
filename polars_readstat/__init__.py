
from typing import Any, Iterator
from polars.io.plugins import register_io_source
import polars as pl


from polars_readstat import read_readstat 

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
        

        if with_columns is not None:
            src.set_with_columns(with_columns)
        # else:
        #     src.set_with_columns(list(schema().keys()))


        # Set the predicate.
        # predicate_set = True
        # if predicate is not None:
        #     try:
        #         src.try_set_predicate(predicate)
        #     except pl.exceptions.ComputeError:
        #         predicate_set = False

        while (out := src.next()) is not None:
            # If the source could not apply the predicate
            # (because it wasn't able to deserialize it), we do it here.
            # if not predicate_set and predicate is not None:
            if predicate is not None:
                out = out.filter(predicate)

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
                                    filter_rows:bool=False):
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
        # print(df)
        
        import pandas as pd

        start_pd = time.time()
        dfp = pd.read_stata(path)
        if subset_columns:
            dfp = dfp[columns]
        if filter_rows:
            dfp = dfp[dfp['BIRTHYR'] >= 1980]
        elapsed_pd = time.time() - start_pd
        # print(dfp)
        
        dfp = pl.from_pandas(dfp)
        
        print(f"Subset: {subset_columns}, Filter: {filter_rows}")
        print(f"    Polars: {elapsed_pl}")
        print(f"    Pandas: {elapsed_pd}")
        print(f"    Are identical: {dfp.equals(df)}")
    # read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.sas7bdat")

    # read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.dta")

    def dta_test():
        print("\n\n\nStata")
        read_multithreaded_test_dta(subset_columns=False,
                                    filter_rows=False)
        read_multithreaded_test_dta(subset_columns=True,
                                    filter_rows=False)
        read_multithreaded_test_dta(subset_columns=False,
                                    filter_rows=True)
        read_multithreaded_test_dta(subset_columns=True,
                                    filter_rows=True)
        

    def read_multithreaded_test_sas7bdat(subset_columns:bool=False,
                                    filter_rows:bool=False):
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
        # print(df)
        
        import pandas as pd

        start_pd = time.time()
        dfp = pd.read_sas(path)
        if subset_columns:
            dfp = dfp[columns]
        if filter_rows:
            dfp = dfp[dfp['PINCP'] >= 5000]
        elapsed_pd = time.time() - start_pd
        # print(dfp)
        
        dfp = pl.from_pandas(dfp)
        
        print(f"Subset: {subset_columns}, Filter: {filter_rows}")
        print(f"    Polars: {elapsed_pl}")
        print(f"    Pandas: {elapsed_pd}")
        print(f"    Are identical: {dfp.equals(df)}")

    def sas7bdat_test():

        path = "/home/jrothbaum/Downloads/psam_p06.sas7bdat"
        
        print("\n\n\nSAS")
        read_multithreaded_test_sas7bdat(subset_columns=False,
                                         filter_rows=False)
        read_multithreaded_test_sas7bdat(subset_columns=True,
                                         filter_rows=False)
        read_multithreaded_test_sas7bdat(subset_columns=False,
                                         filter_rows=True)
        read_multithreaded_test_sas7bdat(subset_columns=True,
                                         filter_rows=True)

    # sas7bdat_test()
    dta_test()