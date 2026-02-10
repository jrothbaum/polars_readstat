#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "polars-readstat==0.11.1",
#   "polars>=1.25.2",
#   "pyarrow>=19.0.1",
# ]
# ///

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import polars as pl
import polars_readstat as prs

READSTAT_EXTS = {".sas7bdat", ".dta", ".sav", ".zsav"}
INT_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect non-huge fixture read results for polars-readstat==0.11.1"
    )
    parser.add_argument(
        "--fixtures-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "polars_readstat_rs" / "tests",
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-bytes", type=int, default=100 * 1024 * 1024)
    return parser.parse_args()


def iter_fixture_files(root: Path, max_bytes: int) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in READSTAT_EXTS:
            continue
        if path.stat().st_size > max_bytes:
            continue
        files.append(path)
    files.sort()
    return files


def fingerprint_int_series(series: pl.Series) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(str(series.len()).encode("utf-8"))
    h.update(b"|")
    for v in series.to_list():
        if v is None:
            h.update(b"N;")
        else:
            h.update(str(int(v)).encode("utf-8"))
            h.update(b";")
    return h.hexdigest()


def int_value_fingerprints(df: pl.DataFrame) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, dtype in df.schema.items():
        if dtype in INT_DTYPES:
            out[name] = fingerprint_int_series(df[name])
    return out


def summarize_df(df: pl.DataFrame) -> dict:
    schema = {name: str(dtype) for name, dtype in df.schema.items()}
    null_counts = {name: int(v) for name, v in df.null_count().row(0, named=True).items()}
    return {
        "rows": int(df.height),
        "cols": int(df.width),
        "schema": schema,
        "null_counts": null_counts,
        "int_value_fingerprints": int_value_fingerprints(df),
    }


def collect_lf(lf: pl.LazyFrame) -> pl.DataFrame:
    for kwargs in ({"engine": "streaming"}, {"streaming": True}, {}):
        try:
            return lf.collect(**kwargs)
        except TypeError:
            continue
    return lf.collect()


def read_0111(path: Path) -> pl.DataFrame:
    out = prs.scan_readstat(str(path))
    if isinstance(out, pl.DataFrame):
        return out
    return collect_lf(out)


def try_loader(path: Path) -> dict:
    try:
        df = read_0111(path)
        return {"ok": True, **summarize_df(df)}
    except BaseException as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def main() -> int:
    args = parse_args()
    fixtures_root = args.fixtures_root.resolve()
    out_path = args.out.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = iter_fixture_files(fixtures_root, args.max_bytes)
    manifest = {
        "fixtures_root": str(fixtures_root),
        "max_bytes": int(args.max_bytes),
        "meta": {
            "count_files": len(files),
            "old_module": str(Path(prs.__file__).resolve()),
            "old_version": getattr(prs, "__version__", "0.11.1"),
            "int_value_fingerprints": True,
        },
        "files": {},
    }

    for i, path in enumerate(files, start=1):
        rel = str(path.relative_to(fixtures_root))
        manifest["files"][rel] = {
            "size_bytes": path.stat().st_size,
            "old0111": try_loader(path),
        }
        if i % 25 == 0 or i == len(files):
            print(f"processed {i}/{len(files)}")

    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
