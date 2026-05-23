"""Compare SAV bytes for various temporal columns written by pyreadstat vs polars_readstat."""
import datetime
import polars as pl
import pyreadstat
import pandas as pd
import sys, os, struct

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "polars_readstat"))
from polars_readstat import ScanReadstat, write_readstat

def hexblock(data, label, start=0, length=None):
    if length is None:
        length = len(data) - start
    print(f"=== {label}  (offset {start}–{start+length-1}, {length} bytes) ===")
    chunk = data[start:start+length]
    for i in range(0, len(chunk), 16):
        row = chunk[i:i+16]
        hex_part  = " ".join(f"{b:02x}" for b in row)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in row)
        print(f"  {start+i:04x}  {hex_part:<47}  {ascii_part}")
    print()

def first_diffs(ref, ours, limit=30, skip_header=False):
    """Print first N byte differences, optionally skipping the 60-byte prod_name at offset 4."""
    diffs = 0
    for i, (a, b) in enumerate(zip(ref, ours)):
        if skip_header and 4 <= i < 64:   # prod_name cosmetic
            continue
        if skip_header and 92 <= i < 109:  # creation_date cosmetic
            continue
        if a != b:
            ctx_r = ref[max(0,i-4):i+12]
            ctx_o = ours[max(0,i-4):i+12]
            print(f"  0x{i:04x} ({i:4d}): ref={a:02x}  ours={b:02x}")
            print(f"    ref  ctx: {' '.join(f'{x:02x}' for x in ctx_r)}")
            print(f"    ours ctx: {' '.join(f'{x:02x}' for x in ctx_o)}")
            diffs += 1
            if diffs >= limit:
                print("  … (stopping)")
                break
    if diffs == 0:
        print("  (no meaningful differences)")

def compare(ref_path, our_path, label, skip_header=True):
    ref  = open(ref_path,  "rb").read()
    ours = open(our_path,  "rb").read()
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    print(f"  pyreadstat  size: {len(ref)}")
    print(f"  polars_rs   size: {len(ours)}")
    # Variable record: always starts at byte 176 (header) and is 32 bytes
    hexblock(ref,  "pyreadstat variable record", 176, 32)
    hexblock(ours, "polars_rs  variable record", 176, 32)
    # Data: last 8 bytes (one double)
    hexblock(ref,  "pyreadstat data (last 8B)", len(ref)-8, 8)
    hexblock(ours, "polars_rs  data (last 8B)", len(ours)-8, 8)
    # Meaningful diffs
    print("--- Meaningful byte differences (skip prod_name+creation_date) ---")
    first_diffs(ref, ours, skip_header=skip_header)

dt = datetime.datetime(2020, 3, 15, 10, 30, 0)
date_val = datetime.date(2020, 3, 15)

# ── 1. DATETIME20 ────────────────────────────────────────────────────────────
PYRE = "/tmp/cmp_datetime_py.sav"
OURS = "/tmp/cmp_datetime_rs.sav"
df_pd = pd.DataFrame({"dt_col": [dt]})
pyreadstat.write_sav(df_pd, PYRE, variable_format={"dt_col": "DATETIME20"})
df_pl = pl.DataFrame({"dt_col": pl.Series([dt]).cast(pl.Datetime("ms"))})
write_readstat(df_pl, OURS)
compare(PYRE, OURS, "DATETIME20  (Datetime ms)")

# ── 2. DATE11 ────────────────────────────────────────────────────────────────
PYRE = "/tmp/cmp_date_py.sav"
OURS = "/tmp/cmp_date_rs.sav"
df_pd = pd.DataFrame({"d_col": [date_val]})
pyreadstat.write_sav(df_pd, PYRE, variable_format={"d_col": "DATE11"})
df_pl = pl.DataFrame({"d_col": pl.Series([date_val])})
write_readstat(df_pl, OURS)
compare(PYRE, OURS, "DATE11  (Date)")

# ── 3. TIME8 ─────────────────────────────────────────────────────────────────
PYRE = "/tmp/cmp_time_py.sav"
OURS = "/tmp/cmp_time_rs.sav"
# pyreadstat wants seconds-since-midnight as a float for TIME format
time_secs = 10 * 3600 + 30 * 60   # 10:30:00
df_pd = pd.DataFrame({"t_col": [float(time_secs)]})
pyreadstat.write_sav(df_pd, PYRE, variable_format={"t_col": "TIME8"})
df_pl = pl.DataFrame({"t_col": pl.Series([datetime.time(10, 30, 0)])})
write_readstat(df_pl, OURS)
compare(PYRE, OURS, "TIME8  (Time)")

# ── 4. ADATE (format 23) via pyreadstat ──────────────────────────────────────
PYRE = "/tmp/cmp_adate_py.sav"
OURS = "/tmp/cmp_adate_rs.sav"
df_pd = pd.DataFrame({"a_col": [date_val]})
pyreadstat.write_sav(df_pd, PYRE, variable_format={"a_col": "ADATE"})
df_pl = pl.DataFrame({"a_col": pl.Series([date_val])})
# write as ADATE via explicit format override
write_readstat(df_pl, OURS, variable_format={"a_col": {"format_type": 23, "width": 8, "decimals": 0}})
compare(PYRE, OURS, "ADATE  (format_type=23)")

# ── 5. Roundtrip: read datetime.sav, write, compare variable record + data ───
SRC = "crates/polars_readstat_rs/tests/spss/data/datetime.sav"
RT_OURS = "/tmp/cmp_roundtrip_rs.sav"
if os.path.exists(SRC):
    reader = ScanReadstat(SRC)
    df_rt = reader.df.collect()
    write_readstat(df_rt, RT_OURS, metadata=reader.metadata)
    ref_src  = open(SRC,    "rb").read()
    rt_bytes = open(RT_OURS,"rb").read()
    print(f"\n{'='*70}")
    print(f"  Roundtrip: datetime.sav → write back")
    print(f"{'='*70}")
    print(f"  original  size: {len(ref_src)}")
    print(f"  roundtrip size: {len(rt_bytes)}")
    print(f"  schema: {df_rt.schema}")
    # Show variable records
    hexblock(ref_src, "original  variable records", 176, 96)
    hexblock(rt_bytes,"roundtrip variable records", 176, 96)
    # Show data
    print("--- original  tail ---")
    hexblock(ref_src, "original  tail", max(0,len(ref_src)-32), 32)
    hexblock(rt_bytes,"roundtrip tail", max(0,len(rt_bytes)-32), 32)
