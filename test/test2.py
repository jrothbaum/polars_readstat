import sys
import os
from time import perf_counter
from pathlib import Path
import glob
from enum import Enum    
import polars as pl
prs_path = f"/home/jrothbaum/Coding/polars_readstat/target/wheels/polars_readstat-0.4.2-cp39-abi3-manylinux_2_39_x86_64.whl_FILES"
sys.path.insert(0, str(prs_path))

from polars_readstat import scan_readstat



path_error_files = "/home/jrothbaum/Downloads"

path_compressed = f"{path_error_files}/test.sas7bdat"

df = scan_readstat(path_compressed)
df = df.head(10)
df = df.collect()
print(df)


# df_correct = scan_readstat(path_compressed,
#                            engine="readstat")
# df_correct = df_correct.collect()
# print(df_correct)