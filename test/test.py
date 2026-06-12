import polars_readstat
import polars_readstat.polars_readstat_bindings as b

from polars_readstat import scan_readstat
lf = scan_readstat("/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/sas/data/too_big/compress_test_16.sas7bdat",
                   batch_size=10_000)
df = lf.collect()

print(df)

