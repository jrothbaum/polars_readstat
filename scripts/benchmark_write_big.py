#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import polars as pl
import polars_readstat as prs


def _largest_file(dir_path: Path, suffixes: tuple[str, ...]) -> Path | None:
    candidates = [p for p in dir_path.rglob("*") if p.is_file() and p.suffix.lower() in suffixes]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_size)


def _pick_default_paths(fixtures_root: Path) -> dict[str, Path]:
    defaults: dict[str, Path] = {}

    sas_candidates = [
        fixtures_root / "sas/data/too_big/psam_p17.sas7bdat",
        fixtures_root / "sas/data/too_big/topical.sas7bdat",
    ]
    defaults["sas"] = next((p for p in sas_candidates if p.exists()), fixtures_root / "sas/data/data_pandas/test1.sas7bdat")

    stata_dir = fixtures_root / "stata/data"
    defaults["stata"] = _largest_file(stata_dir, (".dta",)) or (stata_dir / "sample.dta")

    spss_candidates = [
        fixtures_root / "spss/data/too_big/GSS2024.sav",
        fixtures_root / "spss/data/too_big/ess_data.sav",
        fixtures_root / "spss/data/sample_large.sav",
    ]
    defaults["spss"] = next((p for p in spss_candidates if p.exists()), fixtures_root / "spss/data/sample.sav")

    return defaults


def _write_once(df: pl.DataFrame, out_path: Path, fmt: str, threads: int | None) -> float:
    kwargs: dict[str, Any] = {"format": fmt}
    if fmt == "dta" and threads is not None:
        kwargs["threads"] = threads

    t0 = time.perf_counter()
    prs.write_readstat(df, str(out_path), **kwargs)
    return time.perf_counter() - t0


def _bench_case(
    source_path: Path,
    source_kind: str,
    target_fmt: str,
    output_dir: Path,
    repeat: int,
    rows: int | None,
    threads: int | None,
    keep_files: bool,
) -> dict[str, Any]:
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    t_load0 = time.perf_counter()
    df = prs.scan_readstat(str(source_path), preserve_order=True).collect(engine="streaming")
    if rows is not None:
        df = df.head(rows)
    load_s = time.perf_counter() - t_load0

    write_times: list[float] = []
    out_paths: list[Path] = []
    for i in range(repeat):
        out_path = output_dir / f"{source_kind}_write_{i + 1}.{target_fmt}"
        elapsed = _write_once(df, out_path, target_fmt, threads)
        write_times.append(elapsed)
        out_paths.append(out_path)

    bytes_out = sum(p.stat().st_size for p in out_paths if p.exists())

    if not keep_files:
        for p in out_paths:
            p.unlink(missing_ok=True)

    return {
        "source_kind": source_kind,
        "source_path": str(source_path),
        "target_format": target_fmt,
        "rows": int(df.height),
        "cols": int(df.width),
        "source_size_mb": round(source_path.stat().st_size / (1024 * 1024), 2),
        "output_size_mb_total": round(bytes_out / (1024 * 1024), 2),
        "load_seconds": round(load_s, 4),
        "write_seconds": [round(x, 4) for x in write_times],
        "write_seconds_avg": round(sum(write_times) / len(write_times), 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark big-file write performance for Python polars_readstat bindings")
    parser.add_argument(
        "--fixtures-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "polars_readstat_rs" / "tests",
        help="Root folder containing fixture files (default: ../polars_readstat_rs/tests)",
    )
    parser.add_argument("--sas-file", type=Path, default=None, help="Override SAS source file")
    parser.add_argument("--stata-file", type=Path, default=None, help="Override Stata source file")
    parser.add_argument("--spss-file", type=Path, default=None, help="Override SPSS source file")
    parser.add_argument("--repeat", type=int, default=1, help="Write repeats per format (default: 1)")
    parser.add_argument("--rows", type=int, default=None, help="Optional row cap for quicker dry-runs")
    parser.add_argument("--threads", type=int, default=None, help="Optional writer threads for Stata writes")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/polars_readstat_write_bench"),
        help="Output directory for temporary benchmark writes",
    )
    parser.add_argument("--keep-files", action="store_true", help="Keep written benchmark files")
    parser.add_argument("--json-out", type=Path, default=None, help="Optional JSON summary output path")
    args = parser.parse_args()

    if args.repeat < 1:
        raise ValueError("--repeat must be >= 1")

    defaults = _pick_default_paths(args.fixtures_root)
    sas_path = args.sas_file or defaults["sas"]
    stata_path = args.stata_file or defaults["stata"]
    spss_path = args.spss_file or defaults["spss"]

    args.output_dir.mkdir(parents=True, exist_ok=True)

    cases = [
        ("sas", sas_path, "dta"),
        ("stata", stata_path, "dta"),
        ("spss", spss_path, "sav"),
    ]

    results = []
    for source_kind, source_path, target_fmt in cases:
        result = _bench_case(
            source_path=source_path,
            source_kind=source_kind,
            target_fmt=target_fmt,
            output_dir=args.output_dir,
            repeat=args.repeat,
            rows=args.rows,
            threads=args.threads,
            keep_files=args.keep_files,
        )
        results.append(result)
        print(
            f"{source_kind:>5} -> {target_fmt}: "
            f"rows={result['rows']:,} cols={result['cols']} "
            f"load={result['load_seconds']:.3f}s "
            f"write_avg={result['write_seconds_avg']:.3f}s"
        )

    summary = {
        "fixtures_root": str(args.fixtures_root),
        "repeat": args.repeat,
        "rows_cap": args.rows,
        "results": results,
    }

    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
