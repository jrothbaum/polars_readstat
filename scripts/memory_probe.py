#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import time
from pathlib import Path

import polars as pl

from polars_readstat import read_readstat, scan_readstat


def rss_kb() -> int | None:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
    except OSError:
        return None
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe RSS while repeatedly reading a ReadStat file."
    )
    parser.add_argument("path", type=Path, help="Path to .sas7bdat/.dta/.sav/.zsav")
    parser.add_argument("--iters", type=int, default=50)
    parser.add_argument("--mode", choices=("read", "scan"), default="read")
    parser.add_argument("--threads", type=int, default=0, help="0 means polars default")
    parser.add_argument("--batch-size", type=int, default=0, help="Only used in scan mode")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument(
        "--gc",
        action="store_true",
        help="Call gc.collect() after each iteration",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Explicitly del the DataFrame each iteration",
    )
    parser.add_argument(
        "--disable-string-cache",
        action="store_true",
        help="Disable Polars string cache for this run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.disable_string_cache:
        pl.disable_string_cache()

    threads = None if args.threads == 0 else args.threads
    batch_size = None if args.batch_size == 0 else args.batch_size

    for i in range(args.iters):
        if args.mode == "read":
            df = read_readstat(
                path=str(args.path),
                threads=threads,
                missing_string_as_null=False,
                value_labels_as_strings=False,
            )
        else:
            lf = scan_readstat(
                path=str(args.path),
                threads=threads,
                missing_string_as_null=False,
                value_labels_as_strings=False,
                batch_size=batch_size,
            )
            df = lf.collect()

        if args.drop:
            del df
        if args.gc:
            gc.collect()

        rss = rss_kb()
        rss_str = f"{rss} kB" if rss is not None else "unknown"
        print(f"{i:04d} rss={rss_str}")
        if args.sleep > 0:
            time.sleep(args.sleep)


if __name__ == "__main__":
    main()
