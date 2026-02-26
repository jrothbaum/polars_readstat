"""Explore all three informative-null modes on the SAS test file."""
import polars as pl
import polars_readstat as prs

PATH = "../polars_readstat_rs/tests/sas/data/info_nulls_test_data.sas7bdat"

# ── baseline ─────────────────────────────────────────────────────────────────
base = prs.scan_readstat(PATH)
print("=== Baseline ===")
print("Columns:", base.columns)
print(base.head(5))

# ── separate_column (default) ────────────────────────────────────────────────
sep = prs.scan_readstat(PATH, informative_nulls=prs.InformativeNullOpts())
print("\n=== separate_column (default) ===")
print("Columns:", sep.columns)
print(sep.head(5))

ind_cols = [c for c in sep.columns if c.endswith("_null")]
print(f"\nRows where any indicator fires (first 10):")
mask = pl.any_horizontal([sep[c].is_not_null() for c in ind_cols])
print(sep.filter(mask).head(10))

# ── struct ────────────────────────────────────────────────────────────────────
struct_df = prs.scan_readstat(
    PATH, informative_nulls=prs.InformativeNullOpts(mode="struct")
)
print("\n=== struct ===")
print("Schema:", struct_df.schema)
print(struct_df.head(5).collect())

struct_cols = [c for c in struct_df.columns if struct_df[c].dtype == pl.Struct]
if struct_cols:
    print(f"\nUnnested first struct col ({struct_cols[0]!r}):")
    print(struct_df.select(pl.col(struct_cols[0]).struct.unnest()).head(10))

# ── merged_string ─────────────────────────────────────────────────────────────
merged = prs.scan_readstat(
    PATH, informative_nulls=prs.InformativeNullOpts(mode="merged_string")
)
print("\n=== merged_string ===")
print("Schema:", merged.schema)
print(merged.head(5).collect())

# Show rows where a tracked column holds a missing-code string instead of a number
tracked = [c for c in base.columns if base[c].dtype == pl.Float64]
if tracked:
    first = tracked[0]
    miss_rows = merged.filter(pl.col(first).str.starts_with("."))
    print(f"\nRows where {first!r} contains a missing code:")
    print(miss_rows.head(10))
