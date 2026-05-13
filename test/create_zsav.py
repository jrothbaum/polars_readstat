import pyreadstat as prs

(df,_) = prs.read_sas7bdat("/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/sas/data/too_big/psam_p17.sas7bdat")
prs.write_sav(df,"/home/jrothbaum/Coding/claude_code/polars_readstat/crates/polars_readstat_rs/tests/spss/data/too_big/psam_p17.zsav",compress=True,)