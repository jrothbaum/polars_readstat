import polars as pl
import pyreadstat
import pandas as pd
from polars_readstat import ScanReadstat, write_readstat

# ── Step 1: create input SAV with declared A1024, only short data ────────
pdf = pd.DataFrame({"Q6": ["short answer"] * 5})  # max 12 chars
pyreadstat.write_sav(
    pdf, "input.sav",
    variable_format={"Q6": "A1024"},     # declare width 1024
)

# ── Step 2a: pyreadstat roundtrip with variable_format passthrough ──────
df_pr, meta_pr = pyreadstat.read_sav("input.sav")
print("pyreadstat read — original_variable_types:", meta_pr.original_variable_types)
# {'Q6': 'A1024'}

pyreadstat.write_sav(
    df_pr, "out_pyreadstat.sav",
    variable_format=meta_pr.original_variable_types,
)
_, meta_pr2 = pyreadstat.read_sav("out_pyreadstat.sav")
print("pyreadstat roundtrip — original_variable_types:", meta_pr2.original_variable_types)
# {'Q6': 'A1024'}  ✓ preserved

# ── Step 2b: polars_readstat roundtrip with metadata= passthrough ──────
r = ScanReadstat("input.sav")
df_pl = r.df.collect()
q6_meta_in = next(v for v in r.metadata["variables"] if v["name"] == "Q6")
print("polars_readstat read — Q6 string_len / format:",
      q6_meta_in.get("string_len"), "/", q6_meta_in.get("format_class"))
# 1024 / 'A'

write_readstat(df_pl, "out_polars.sav", metadata=r.metadata)

r2 = ScanReadstat("out_polars.sav")
q6_meta_out = next(v for v in r2.metadata["variables"] if v["name"] == "Q6")
print("polars_readstat roundtrip — Q6 string_len:", q6_meta_out.get("string_len"))
print("polars_readstat roundtrip — Q6 format_width:", q6_meta_out.get("format_width"))
