# Explore all three informative-null modes on the SAS test file.
import polars as pl
import polars.selectors as cs
import polars_readstat as prs

PATH = "../polars_readstat_rs/tests/sas/data/info_nulls_test_data.sas7bdat"

# ── baseline ─────────────────────────────────────────────────────────────────
base = prs.scan_readstat(PATH)
print("=== Baseline ===")
print("Columns:", base.collect_schema().names())
print(base.head(5).collect())

# ── separate_column (default) ────────────────────────────────────────────────
sep = prs.scan_readstat(PATH, informative_nulls=prs.InformativeNullOpts())
print("\n=== separate_column (default) ===")
print("Columns:", sep.collect_schema().names())
print(sep.head(5).collect())

print(f"\nRows where any indicator fires (first 10):")
print(sep.filter(pl.any_horizontal(cs.ends_with("_null").is_not_null())).head(10).collect())

# ── struct ────────────────────────────────────────────────────────────────────
struct_df = prs.scan_readstat(
    PATH, informative_nulls=prs.InformativeNullOpts(mode="struct")
)
print("\n=== struct ===")
print("Schema:", struct_df.collect_schema())
print(struct_df.head(5).collect())

struct_cols = [n for n, t in struct_df.collect_schema().items() if isinstance(t, pl.Struct)]
if struct_cols:
    print(f"\nUnnested struct col with nulls ({struct_cols[1]!r}):")
    print(struct_df.select(pl.col(struct_cols[1]).struct.unnest()).head(10).collect())

# ── merged_string ─────────────────────────────────────────────────────────────
merged = prs.scan_readstat(
    PATH, informative_nulls=prs.InformativeNullOpts(mode="merged_string")
)
print("\n=== merged_string ===")
print("Schema:", merged.collect_schema())
print(merged.head(5).collect())

# Show rows where a tracked column holds a missing-code string instead of a number
tracked = base.select(cs.by_dtype(pl.Float64)).collect_schema().names()
if tracked:
    first = tracked[1]
    print(f"\nRows where {first!r} contains a missing code:")
    print(merged.filter(pl.col(first).str.starts_with(".")).head(10).collect())
