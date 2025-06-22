import sys
import os
from time import perf_counter
from pathlib import Path
import glob
from enum import Enum    

prs_path = f"{Path(__file__).parent.parent}/polars_readstat/polars_readstat/"
sys.path.insert(0, str(prs_path))

from polars_readstat import scan_readstat

import polars as pl
import pandas as pd
import pyreadstat as prs

class Reader(Enum):
    CPP = 0
    READSTAT = 1
    PANDAS = 2
    PYREADSTAT = 3

def read_file(path:str,
              reader:Reader,
              d_time:dict[Reader,float]) -> pl.DataFrame:
    start = perf_counter()
    if reader == Reader.CPP:
        df = scan_readstat(path).collect()
    elif reader == Reader.READSTAT:
        df = scan_readstat(path,
                           engine="readstat").collect()
    elif reader == Reader.PANDAS:
        df = pl.from_pandas(pd.read_sas(path))
        #   df = df.with_columns(pl.col(pl.Binary).cast(pl.String))
    elif reader == Reader.PYREADSTAT:
        df = pl.from_pandas(prs.read_sas7bdat(path)[0])

    elapsed = perf_counter() - start

    d_time[reader] = d_time[reader] + elapsed
    
    return df

if __name__ == "__main__":
    test_path = f"{Path(__file__).parent.parent}/crates/cpp-sas7bdat/vendor/test"
    
    sas_files = glob.glob(f'{test_path}/**/*.sas7bdat', recursive=True)

    files_to_skip = ["/home/jrothbaum/Coding/polars_readstat/crates/cpp-sas7bdat/vendor/test/data_pandas/zero_variables.sas7bdat"]
    
    compare_to = Reader.PYREADSTAT
    total_time = {Reader.CPP:0.0,
                  Reader.READSTAT:0.0,
                  Reader.PANDAS:0.0,
                  Reader.PYREADSTAT:0.0}
    n_matches  = {Reader.CPP:0,
                  Reader.READSTAT:0,
                  Reader.PANDAS:0,
                  Reader.PYREADSTAT:0
                  }
    del n_matches[compare_to]

    n_files = 0
    for filei in sas_files:
        if filei not in files_to_skip:
            n_files = n_files + 1
            print(filei)
            
            dfs = {}
            for reader in Reader:
                #   print(reader)
                dfs[reader] = read_file(path=filei,
                                        reader=reader,
                                        d_time=total_time)

            for reader in dfs.keys():
                if reader != compare_to:
                    df1 = dfs[compare_to]
                    df2 = dfs[reader]
                    # df_schema = pl.concat([df1.lazy(),df2.lazy()],
                    #                       how="diagonal_relaxed")
                    # schema = df_schema.collect_schema()

                    # try:
                    #     for coli in df1.select(pl.col(pl.Binary)).columns:
                    #         try:
                    #             df1 = df1.with_columns(pl.col(coli).cast(pl.String))
                    #         except:
                    #             print(f"{coli} cannot be cast to string")
                    #             print(df1.select(coli).glimpse())
                    #             print(df2.select(coli).glimpse())
                    #     for coli in df2.select(pl.col(pl.Binary)).columns:
                    #         try:
                    #             df2 = df2.with_columns(pl.col(coli).cast(pl.String))
                    #         except:
                    #             print(f"{coli} cannot be cast to string")
                    #             print(df1.select(coli).glimpse())
                    #             print(df2.select(coli).glimpse())
                    #     df1 = df1.match_to_schema(schema,
                    #                             integer_cast="upcast",
                    #                             float_cast="upcast")
                    #     df2 = df2.match_to_schema(schema,
                    #                             integer_cast="upcast",
                    #                             float_cast="upcast")
                        
                    #     df1 = df1.lazy().collect()
                    #     df2 = df2.lazy().collect()
                    #     equal = df1.equals(df2)
                    # except Exception as e:
                    #     print(f"Cast failed: {e}")
                    #     equal = False


                    equal = df1.equals(df2)
                    print(f"    {reader}: {equal}")

                    if equal:
                        n_matches[reader] = n_matches[reader] + 1
                    else:
                        if df1.shape != df2.shape:
                            print(f"     Shape mismatch ({compare_to}, {reader})")
                            print(df1.shape)
                            print(df2.shape)
                            print(df1)
                            print(df2)
                        if df1.schema != df2.schema:
                            print("     Schema mismatch")
                        

                        # l_seen = []
                        # for vari, typei in df1.schema.items():
                        #     l_seen.append(vari)
                            
                        #     if vari in df2.schema:
                        #         type2 = df2.schema[vari]
                        #         print(f"{vari}: {typei}, {type2}")
                        #     else:
                        #         print(f"{vari} not in {compare_to}")
                        # for vari, typei in df2.schema.items():
                        #     if vari not in l_seen:
                        #         print(f"{vari} not in {reader}")
                        
                    # if not equal:
                    #     print(dfs[reader])
                    #     print(dfs[Reader.PYREADSTAT])


    for keyi, valuei in total_time.items():
        keyi_colon = str(keyi) + ":"
        print(f"{keyi_colon: <20}{valuei:.2f} seconds")
        if keyi in n_matches.keys():
            print(f"     {'matches:':<15}{n_matches[keyi]/n_files*100:.2f}% match")

    # path_test_root = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic"

    # paths = [
    #             f"{path_test_root}/sample.sas7bdat",
    #             f"{path_test_root}/sample.dta",
    #             f"{path_test_root}/sample.sav"
    #          ]
    
    # for pathi in paths:
    #     print(f"\n\n\n{pathi.split('.')[1]}")
    #     start_polars = perf_counter()
    #     df = scan_readstat(pathi)
    #     df = df.collect()
    #     elapsed_polars = perf_counter() - start_polars

    #     start_pandas = perf_counter()
    #     if pathi.endswith(".dta"):
    #         dfp = pd.read_stata(pathi)
    #     elif pathi.endswith(".sas7bdat"):
    #         dfp = pd.read_sas(pathi)
    #     elif pathi.endswith(".sav"):
    #         dfp = pd.read_spss(pathi)
        
    #     elapsed_pandas = perf_counter() - start_pandas

    #     print(df)
    #     print(dfp)

    #     print(f"polars:     {elapsed_polars}")
    #     print(f"pandas:     {elapsed_pandas}")
        
