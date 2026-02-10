#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars_readstat as prs

READSTAT_EXTS = {".sas7bdat", ".dta", ".sav", ".zsav"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export non-huge ReadStat fixtures to parquet using the currently imported polars_readstat"
    )
    parser.add_argument(
        "--fixtures-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "polars_readstat_rs" / "tests",
        help="Root containing fixture files (default: ../polars_readstat_rs/tests)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory for parquet files and manifest.json",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=100 * 1024 * 1024,
        help="Skip files larger than this many bytes (default: 100MB)",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="current",
        help="Label stored in the manifest",
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


def is_temporal(dtype_name: str) -> bool:
    return dtype_name.startswith("Date") or dtype_name.startswith("Datetime") or dtype_name.startswith("Time")


def export_one(path: Path, fixtures_root: Path, out_dir: Path) -> dict:
    rel = path.relative_to(fixtures_root)
    out_path = out_dir / rel.parent / f"{rel.name}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = prs.scan_readstat(str(path), preserve_order=True).collect(engine="streaming")
    df.write_parquet(str(out_path), compression="zstd")

    schema = {name: str(dtype) for name, dtype in df.schema.items()}
    null_counts = {name: int(v) for name, v in df.null_count().row(0, named=True).items()}
    temporal_columns = [name for name, dtype in schema.items() if is_temporal(dtype)]

    return {
        "source": str(rel),
        "parquet": str(out_path.relative_to(out_dir)),
        "rows": int(df.height),
        "cols": int(df.width),
        "schema": schema,
        "null_counts": null_counts,
        "temporal_columns": temporal_columns,
    }


def main() -> int:
    args = parse_args()
    fixtures_root = args.fixtures_root.resolve()
    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    files = iter_fixture_files(fixtures_root, args.max_bytes)
    manifest = {
        "label": args.label,
        "module_path": str(Path(prs.__file__).resolve()),
        "version": getattr(prs, "__version__", "unknown"),
        "fixtures_root": str(fixtures_root),
        "max_bytes": int(args.max_bytes),
        "files": [],
        "errors": [],
    }

    for i, path in enumerate(files, start=1):
        try:
            record = export_one(path, fixtures_root, out_dir)
            manifest["files"].append(record)
            print(f"[{i}/{len(files)}] ok {record['source']}")
        except Exception as exc:  # noqa: BLE001
            manifest["errors"].append({"source": str(path.relative_to(fixtures_root)), "error": str(exc)})
            print(f"[{i}/{len(files)}] err {path.relative_to(fixtures_root)}: {exc}")

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print("\nExport complete")
    print(f"- label: {args.label}")
    print(f"- imported module: {manifest['module_path']}")
    print(f"- files exported: {len(manifest['files'])}")
    print(f"- files errored: {len(manifest['errors'])}")
    print(f"- manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
