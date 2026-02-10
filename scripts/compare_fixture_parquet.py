#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare parquet snapshots from baseline and current runs"
    )
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--current-dir", type=Path, required=True)
    parser.add_argument("--show-examples", type=int, default=25)
    parser.add_argument("--report-json", type=Path, default=None)
    return parser.parse_args()


def load_manifest(dir_path: Path) -> dict:
    manifest_path = dir_path / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def is_temporal(dtype_name: str) -> bool:
    return dtype_name.startswith("Date") or dtype_name.startswith("Datetime") or dtype_name.startswith("Time")


def main() -> int:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    current_dir = args.current_dir.resolve()

    baseline = load_manifest(baseline_dir)
    current = load_manifest(current_dir)

    b_files = {f["source"]: f for f in baseline.get("files", [])}
    c_files = {f["source"]: f for f in current.get("files", [])}

    all_sources = sorted(set(b_files) | set(c_files))

    summary = {
        "baseline_label": baseline.get("label"),
        "current_label": current.get("label"),
        "baseline_version": baseline.get("version"),
        "current_version": current.get("version"),
        "baseline_module": baseline.get("module_path"),
        "current_module": current.get("module_path"),
        "baseline_errors": len(baseline.get("errors", [])),
        "current_errors": len(current.get("errors", [])),
        "file_missing_in_baseline": [],
        "file_missing_in_current": [],
        "shape_mismatches": [],
        "dtype_mismatches": [],
        "null_count_mismatches": [],
        "temporal_improvements": [],
        "temporal_regressions": [],
    }

    for source in all_sources:
        b = b_files.get(source)
        c = c_files.get(source)
        if b is None:
            summary["file_missing_in_baseline"].append(source)
            continue
        if c is None:
            summary["file_missing_in_current"].append(source)
            continue

        b_shape = (b["rows"], b["cols"])
        c_shape = (c["rows"], c["cols"])
        if b_shape != c_shape:
            summary["shape_mismatches"].append(
                {"source": source, "baseline": b_shape, "current": c_shape}
            )

        b_schema = b.get("schema", {})
        c_schema = c.get("schema", {})
        for col in sorted(set(b_schema) | set(c_schema)):
            b_dtype = b_schema.get(col)
            c_dtype = c_schema.get(col)
            if b_dtype == c_dtype:
                continue
            mismatch = {
                "source": source,
                "column": col,
                "baseline": b_dtype,
                "current": c_dtype,
            }
            summary["dtype_mismatches"].append(mismatch)

            b_temp = is_temporal(b_dtype or "")
            c_temp = is_temporal(c_dtype or "")
            if c_temp and not b_temp:
                summary["temporal_improvements"].append(mismatch)
            elif b_temp and not c_temp:
                summary["temporal_regressions"].append(mismatch)

        b_nulls = b.get("null_counts", {})
        c_nulls = c.get("null_counts", {})
        for col in sorted(set(b_nulls) | set(c_nulls)):
            b_null = b_nulls.get(col)
            c_null = c_nulls.get(col)
            if b_null != c_null:
                summary["null_count_mismatches"].append(
                    {
                        "source": source,
                        "column": col,
                        "baseline": b_null,
                        "current": c_null,
                    }
                )

    print("Snapshot comparison summary")
    print(f"- baseline: {summary['baseline_label']} ({summary['baseline_version']})")
    print(f"- current:  {summary['current_label']} ({summary['current_version']})")
    print(f"- baseline module: {summary['baseline_module']}")
    print(f"- current module:  {summary['current_module']}")
    print(f"- baseline export errors: {summary['baseline_errors']}")
    print(f"- current export errors: {summary['current_errors']}")
    print(f"- missing in baseline: {len(summary['file_missing_in_baseline'])}")
    print(f"- missing in current: {len(summary['file_missing_in_current'])}")
    print(f"- shape mismatches: {len(summary['shape_mismatches'])}")
    print(f"- dtype mismatches: {len(summary['dtype_mismatches'])}")
    print(f"- null count mismatches: {len(summary['null_count_mismatches'])}")
    print(f"- temporal improvements: {len(summary['temporal_improvements'])}")
    print(f"- temporal regressions: {len(summary['temporal_regressions'])}")

    n = args.show_examples
    if summary["temporal_regressions"]:
        print("\nTemporal regressions (baseline temporal, current non-temporal):")
        for row in summary["temporal_regressions"][:n]:
            print(f"- {row['source']} | {row['column']} | {row['baseline']} -> {row['current']}")

    if summary["temporal_improvements"]:
        print("\nTemporal improvements (current temporal, baseline non-temporal):")
        for row in summary["temporal_improvements"][:n]:
            print(f"- {row['source']} | {row['column']} | {row['baseline']} -> {row['current']}")

    if args.report_json is not None:
        report_path = args.report_json.resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nWrote report: {report_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
