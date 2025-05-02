from time import perf_counter
from polars_readstat import scan_readstat

import pandas as pd

if __name__ == "__main__":
    path_test_root = "/home/jrothbaum/Downloads/pyreadstat_container/pyreadstat-master/test_data/basic"

    paths = [
                f"{path_test_root}/sample.sas7bdat",
                f"{path_test_root}/sample.dta",
                f"{path_test_root}/sample.sav"
             ]
    
    for pathi in paths:
        print(f"\n\n\n{pathi.split('.')[1]}")
        start_polars = perf_counter()
        df = scan_readstat(pathi)
        df = df.collect()
        elapsed_polars = perf_counter() - start_polars

        start_pandas = perf_counter()
        if pathi.endswith(".dta"):
            dfp = pd.read_stata(pathi)
        elif pathi.endswith(".sas7bdat"):
            dfp = pd.read_sas(pathi)
        elif pathi.endswith(".sav"):
            dfp = pd.read_spss(pathi)
        
        elapsed_pandas = perf_counter() - start_pandas

        print(df)
        print(dfp)

        print(f"polars:     {elapsed_polars}")
        print(f"pandas:     {elapsed_pandas}")
        
