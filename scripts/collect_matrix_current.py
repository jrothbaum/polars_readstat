#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import inspect
import json
from pathlib import Path

import pandas as pd
import polars as pl
import polars_readstat as prs
import pyreadstat

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
        description="Collect non-huge fixture read results for current polars_readstat, pyreadstat, and pandas"
    )
    parser.add_argument(
        "--fixtures-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "polars_readstat_rs" / "tests",
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-bytes", type=int, default=100 * 1024 * 1024)
    parser.add_argument(
        "--missing-string-as-null",
        action="store_true",
        help="Set missing_string_as_null=True for current polars_readstat reads",
    )
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


def read_current(path: Path, missing_string_as_null: bool) -> pl.DataFrame:
    sig = inspect.signature(prs.scan_readstat)
    kwargs = {}
    if "preserve_order" in sig.parameters:
        kwargs["preserve_order"] = True
    if "missing_string_as_null" in sig.parameters:
        kwargs["missing_string_as_null"] = missing_string_as_null

    out = prs.scan_readstat(str(path), **kwargs)
    if isinstance(out, pl.DataFrame):
        return out
    return collect_lf(out)


def read_pyreadstat(path: Path) -> pl.DataFrame:
    ext = path.suffix.lower()
    if ext == ".sas7bdat":
        pdf, _ = pyreadstat.read_sas7bdat(str(path), output_format="pandas")
    elif ext == ".dta":
        pdf, _ = pyreadstat.read_dta(str(path), output_format="pandas")
    elif ext in {".sav", ".zsav"}:
        pdf, _ = pyreadstat.read_sav(str(path), output_format="pandas")
    else:
        raise ValueError(ext)
    return pl.from_pandas(pdf)


def read_pandas(path: Path) -> pl.DataFrame:
    ext = path.suffix.lower()
    if ext == ".sas7bdat":
        pdf = pd.read_sas(str(path), format="sas7bdat")
    elif ext == ".dta":
        pdf = pd.read_stata(str(path))
    elif ext in {".sav", ".zsav"}:
        pdf = pd.read_spss(str(path))
    else:
        raise ValueError(ext)
    return pl.from_pandas(pdf)


def try_loader(fn, path: Path) -> dict:
    try:
        df = fn(path)
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
            "current_module": str(Path(prs.__file__).resolve()),
            "current_version": getattr(prs, "__version__", "unknown"),
            "pandas_version": getattr(pd, "__version__", "unknown"),
            "pyreadstat_version": getattr(pyreadstat, "__version__", "unknown"),
            "missing_string_as_null": bool(args.missing_string_as_null),
            "int_value_fingerprints": True,
        },
        "files": {},
    }

    current_loader = lambda p: read_current(p, args.missing_string_as_null)

    for i, path in enumerate(files, start=1):
        rel = str(path.relative_to(fixtures_root))
        manifest["files"][rel] = {
            "size_bytes": path.stat().st_size,
            "current": try_loader(current_loader, path),
            "pyreadstat": try_loader(read_pyreadstat, path),
            "pandas": try_loader(read_pandas, path),
        }
        if i % 25 == 0 or i == len(files):
            print(f"processed {i}/{len(files)}")

    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
