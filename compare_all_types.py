"""Compare SAV bytes between pyreadstat and polars_readstat for all supported types."""
import datetime
import polars as pl
import pyreadstat
import pandas as pd
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "polars_readstat"))
from polars_readstat import write_readstat

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"

def hexblock(data, label, start=0, length=None):
    if length is None:
        length = len(data) - start
    print(f"  === {label}  (offset {start}–{start+length-1}) ===")
    chunk = data[start:start+length]
    for i in range(0, len(chunk), 16):
        row = chunk[i:i+16]
        hex_part  = " ".join(f"{b:02x}" for b in row)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in row)
        print(f"    {start+i:04x}  {hex_part:<47}  {ascii_part}")

def meaningful_diffs(ref, ours):
    """Return list of (offset, ref_byte, our_byte) excluding cosmetic header fields."""
    diffs = []
    for i, (a, b) in enumerate(zip(ref, ours)):
        if 4 <= i < 64:    # prod_name cosmetic
            continue
        if 92 <= i < 109:  # creation_date/time cosmetic
            continue
        if a != b:
            diffs.append((i, a, b))
    return diffs

def compare(ref_path, our_path, label, show_on_fail=True):
    ref  = open(ref_path,  "rb").read()
    ours = open(our_path,  "rb").read()
    diffs = meaningful_diffs(ref, ours)
    size_ok = len(ref) == len(ours)
    if size_ok and not diffs:
        print(f"  [{PASS}] {label}  ({len(ref)} bytes)")
        return True
    else:
        print(f"  [{FAIL}] {label}  (py={len(ref)}, rs={len(ours)})")
        if show_on_fail:
            # Show variable record + data
            hexblock(ref,  "pyreadstat var record", 176, min(64, len(ref)-176))
            hexblock(ours, "polars_rs  var record", 176, min(64, len(ours)-176))
            hexblock(ref,  "pyreadstat data tail", max(0,len(ref)-32))
            hexblock(ours, "polars_rs  data tail", max(0,len(ours)-32))
            limit = 10
            shown = 0
            for offset, a, b in diffs[:limit]:
                ctx_r = ref[max(0,offset-4):offset+8]
                ctx_o = ours[max(0,offset-4):offset+8]
                print(f"    0x{offset:04x} ({offset:4d}): ref={a:02x}  ours={b:02x}")
                print(f"      ref  ctx: {' '.join(f'{x:02x}' for x in ctx_r)}")
                print(f"      ours ctx: {' '.join(f'{x:02x}' for x in ctx_o)}")
                shown += 1
            if len(diffs) > limit:
                print(f"    … {len(diffs)-limit} more diffs")
        return False

print("=" * 60)
print("  polars_readstat vs pyreadstat — all type comparison")
print("=" * 60)

results = {}

# ── Helpers ──────────────────────────────────────────────────────────────────
def run(label, py_path, rs_path, pd_df, pl_df,
        py_kwargs=None, rs_kwargs=None):
    py_kwargs = py_kwargs or {}
    rs_kwargs = rs_kwargs or {}
    pyreadstat.write_sav(pd_df, py_path, **py_kwargs)
    write_readstat(pl_df, rs_path, **rs_kwargs)
    ok = compare(py_path, rs_path, label)
    results[label] = ok

# ── 1. Int32 ──────────────────────────────────────────────────────────────────
run(
    "Int32",
    "/tmp/cmp_int32_py.sav",
    "/tmp/cmp_int32_rs.sav",
    pd.DataFrame({"x": pd.array([1, 2, 3], dtype="int64")}),
    pl.DataFrame({"x": pl.Series([1, 2, 3], dtype=pl.Int32)}),
)

# ── 2. Int64 ──────────────────────────────────────────────────────────────────
run(
    "Int64",
    "/tmp/cmp_int64_py.sav",
    "/tmp/cmp_int64_rs.sav",
    pd.DataFrame({"x": pd.array([100, 200, 300], dtype="int64")}),
    pl.DataFrame({"x": pl.Series([100, 200, 300], dtype=pl.Int64)}),
)

# ── 3. Float64 ────────────────────────────────────────────────────────────────
run(
    "Float64",
    "/tmp/cmp_float64_py.sav",
    "/tmp/cmp_float64_rs.sav",
    pd.DataFrame({"x": [1.5, 2.25, 3.125]}),
    pl.DataFrame({"x": pl.Series([1.5, 2.25, 3.125], dtype=pl.Float64)}),
)

# ── 4. Float32 ────────────────────────────────────────────────────────────────
run(
    "Float32",
    "/tmp/cmp_float32_py.sav",
    "/tmp/cmp_float32_rs.sav",
    pd.DataFrame({"x": pd.array([1.5, 2.25, 3.125], dtype="float32")}),
    pl.DataFrame({"x": pl.Series([1.5, 2.25, 3.125], dtype=pl.Float32)}),
)

# ── 5. Boolean ────────────────────────────────────────────────────────────────
# pyreadstat has no native bool; use int64 0/1 with F8.0 format
run(
    "Boolean",
    "/tmp/cmp_bool_py.sav",
    "/tmp/cmp_bool_rs.sav",
    pd.DataFrame({"x": pd.array([1, 0, 1], dtype="int64")}),
    pl.DataFrame({"x": pl.Series([True, False, True])}),
)

# ── 6. String (short, ASCII) ──────────────────────────────────────────────────
run(
    "String (short)",
    "/tmp/cmp_str_py.sav",
    "/tmp/cmp_str_rs.sav",
    pd.DataFrame({"x": ["hello", "world", "foo"]}),
    pl.DataFrame({"x": pl.Series(["hello", "world", "foo"])}),
)

# ── 7. String (exactly 8 bytes) ───────────────────────────────────────────────
run(
    "String (8-byte)",
    "/tmp/cmp_str8_py.sav",
    "/tmp/cmp_str8_rs.sav",
    pd.DataFrame({"x": ["abcdefgh", "12345678", "ABCDEFGH"]}),
    pl.DataFrame({"x": pl.Series(["abcdefgh", "12345678", "ABCDEFGH"])}),
)

# ── 8. String (9 bytes, crosses segment boundary) ────────────────────────────
run(
    "String (9-byte)",
    "/tmp/cmp_str9_py.sav",
    "/tmp/cmp_str9_rs.sav",
    pd.DataFrame({"x": ["abcdefghi", "123456789", "ABCDEFGHI"]}),
    pl.DataFrame({"x": pl.Series(["abcdefghi", "123456789", "ABCDEFGHI"])}),
)

# ── 9. String (UTF-8, non-ASCII) ──────────────────────────────────────────────
run(
    "String (UTF-8)",
    "/tmp/cmp_utf8_py.sav",
    "/tmp/cmp_utf8_rs.sav",
    pd.DataFrame({"x": ["café", "naïve", "résumé"]}),
    pl.DataFrame({"x": pl.Series(["café", "naïve", "résumé"])}),
)

# ── 10. Date ──────────────────────────────────────────────────────────────────
date_val = datetime.date(2020, 3, 15)
run(
    "Date",
    "/tmp/cmp_date_py.sav",
    "/tmp/cmp_date_rs.sav",
    pd.DataFrame({"x": [date_val]}),
    pl.DataFrame({"x": pl.Series([date_val])}),
    py_kwargs={"variable_format": {"x": "DATE11"}},
)

# ── 11. Datetime (ms) ────────────────────────────────────────────────────────
dt_val = datetime.datetime(2020, 3, 15, 10, 30, 0)
run(
    "Datetime(ms)",
    "/tmp/cmp_dt_py.sav",
    "/tmp/cmp_dt_rs.sav",
    pd.DataFrame({"x": [dt_val]}),
    pl.DataFrame({"x": pl.Series([dt_val]).cast(pl.Datetime("ms"))}),
    py_kwargs={"variable_format": {"x": "DATETIME20"}},
)

# ── 12. Datetime (us) ────────────────────────────────────────────────────────
run(
    "Datetime(us)",
    "/tmp/cmp_dt_us_py.sav",
    "/tmp/cmp_dt_us_rs.sav",
    pd.DataFrame({"x": [dt_val]}),
    pl.DataFrame({"x": pl.Series([dt_val]).cast(pl.Datetime("us"))}),
    py_kwargs={"variable_format": {"x": "DATETIME20"}},
)

# ── 13. Datetime (ns) ────────────────────────────────────────────────────────
run(
    "Datetime(ns)",
    "/tmp/cmp_dt_ns_py.sav",
    "/tmp/cmp_dt_ns_rs.sav",
    pd.DataFrame({"x": [dt_val]}),
    pl.DataFrame({"x": pl.Series([dt_val]).cast(pl.Datetime("ns"))}),
    py_kwargs={"variable_format": {"x": "DATETIME20"}},
)

# ── 14. Time ──────────────────────────────────────────────────────────────────
time_secs = 10 * 3600 + 30 * 60   # 10:30:00
run(
    "Time",
    "/tmp/cmp_time_py.sav",
    "/tmp/cmp_time_rs.sav",
    pd.DataFrame({"x": [float(time_secs)]}),
    pl.DataFrame({"x": pl.Series([datetime.time(10, 30, 0)])}),
    py_kwargs={"variable_format": {"x": "TIME8"}},
)

# ── 15. Nullable Int32 (with null) ───────────────────────────────────────────
run(
    "Int32 with null",
    "/tmp/cmp_int32_null_py.sav",
    "/tmp/cmp_int32_null_rs.sav",
    pd.DataFrame({"x": pd.array([1, None, 3], dtype="Int64")}),
    pl.DataFrame({"x": pl.Series([1, None, 3], dtype=pl.Int32)}),
)

# ── 16. Float64 with null ────────────────────────────────────────────────────
run(
    "Float64 with null",
    "/tmp/cmp_float64_null_py.sav",
    "/tmp/cmp_float64_null_rs.sav",
    pd.DataFrame({"x": [1.5, None, 3.125]}),
    pl.DataFrame({"x": pl.Series([1.5, None, 3.125], dtype=pl.Float64)}),
)

# ── 17. String with null ─────────────────────────────────────────────────────
run(
    "String with null",
    "/tmp/cmp_str_null_py.sav",
    "/tmp/cmp_str_null_rs.sav",
    pd.DataFrame({"x": ["hello", None, "foo"]}),
    pl.DataFrame({"x": pl.Series(["hello", None, "foo"])}),
)

# ── 18. Multiple columns (mixed types) ───────────────────────────────────────
run(
    "Mixed columns",
    "/tmp/cmp_mixed_py.sav",
    "/tmp/cmp_mixed_rs.sav",
    pd.DataFrame({
        "n": pd.array([1, 2, 3], dtype="int64"),
        "f": [1.1, 2.2, 3.3],
        "s": ["a", "bb", "ccc"],
    }),
    pl.DataFrame({
        "n": pl.Series([1, 2, 3], dtype=pl.Int32),
        "f": pl.Series([1.1, 2.2, 3.3], dtype=pl.Float64),
        "s": pl.Series(["a", "bb", "ccc"]),
    }),
)

# ── Summary ───────────────────────────────────────────────────────────────────
print()
print("=" * 60)
passed = sum(1 for v in results.values() if v)
total  = len(results)
print(f"  Result: {passed}/{total} passed")
if passed < total:
    failed = [k for k, v in results.items() if not v]
    print(f"  Failed: {', '.join(failed)}")
print("=" * 60)
