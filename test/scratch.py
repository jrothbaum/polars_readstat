import polars as pl

from polars_readstat import write_readstat, scan_readstat

df = pl.DataFrame(
    {
        "strl":["".join(["a"*4000]),"".join(["b"*4000])],
        "strl2":["".join(["C"*4000]),"".join(["b"*4000])]
    }
)

path = "C:/Users/jonro/Downloads/random_types"
df = pl.scan_parquet(f"{path}.parquet")
print(df.collect())
# df = df.with_columns(pl.col(pl.UInt8).cast(pl.Int16),
#                      pl.col(pl.UInt16).cast(pl.Int32))
write_readstat(df,f"{path}.dta")
# path = "C:/Users/jonro/AppData/Local/Temp/ST_1fc8_000001.dta"
df = scan_readstat(f"{path}.dta")
print(df.collect())