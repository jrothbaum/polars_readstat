import polars_readstat
import polars_readstat.polars_readstat_bindings as b

from polars_readstat import scan_readstat
lf = scan_readstat("/home/jrothbaum/Coding/claude_code/polars_readstat_rs/tests/spss/data/simple_alltypes.sav")
print("lazy schema:", lf.schema)
df = lf.collect()
print("collected schema:", df.schema)

print(df)

