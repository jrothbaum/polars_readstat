#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


INT_WIDTHS = {
    "Int8": ("signed", 8),
    "Int16": ("signed", 16),
    "Int32": ("signed", 32),
    "Int64": ("signed", 64),
    "UInt8": ("unsigned", 8),
    "UInt16": ("unsigned", 16),
    "UInt32": ("unsigned", 32),
    "UInt64": ("unsigned", 64),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize 4-way readstat comparison manifests")
    parser.add_argument("--current-manifest", type=Path, required=True)
    parser.add_argument("--old-manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--examples", type=int, default=20)
    return parser.parse_args()


def is_temporal(dtype_name: str | None) -> bool:
    if not dtype_name:
        return False
    return dtype_name.startswith("Date") or dtype_name.startswith("Datetime") or dtype_name.startswith("Time")


def is_int_narrower(a_dtype: str | None, b_dtype: str | None) -> bool:
    if a_dtype not in INT_WIDTHS or b_dtype not in INT_WIDTHS:
        return False
    a_sign, a_bits = INT_WIDTHS[a_dtype]
    b_sign, b_bits = INT_WIDTHS[b_dtype]
    return a_sign == b_sign and a_bits < b_bits


def get_engine(cur: dict, old: dict, rel: str, engine: str) -> dict | None:
    if engine == "old0111":
        return old.get("files", {}).get(rel, {}).get("old0111")
    return cur.get("files", {}).get(rel, {}).get(engine)


def get_int_fp(entry: dict, col: str) -> str | None:
    fps = entry.get("int_value_fingerprints")
    if not isinstance(fps, dict):
        return None
    val = fps.get(col)
    return val if isinstance(val, str) else None


def width_diff_verified_equal(a_entry: dict, b_entry: dict, col: str) -> bool | None:
    af = get_int_fp(a_entry, col)
    bf = get_int_fp(b_entry, col)
    if af is None or bf is None:
        return None
    return af == bf


def init_format_counts() -> dict:
    return {
        "both_read_ok": 0,
        "read_improvement": 0,
        "read_regression": 0,
        "both_read_fail": 0,
        "dtype_mismatch_columns": 0,
        "dtype_narrowing_improvement_columns": 0,
        "dtype_narrowing_regression_columns": 0,
        "dtype_narrowing_value_mismatch_columns": 0,
        "dtype_narrowing_unverified_columns": 0,
        "null_mismatch_columns": 0,
        "temporal_improvement_columns": 0,
        "temporal_regression_columns": 0,
    }


def pair_summary(cur: dict, old: dict, a: str, b: str, max_examples: int) -> dict:
    all_files = sorted(set(cur.get("files", {}).keys()) | set(old.get("files", {}).keys()))
    out = {
        "counts": {
            "both_read_ok": 0,
            "read_improvement": 0,
            "read_regression": 0,
            "both_read_fail": 0,
            "shape_mismatch_files": 0,
            "dtype_mismatch_columns": 0,
            "dtype_narrowing_improvement_columns": 0,
            "dtype_narrowing_regression_columns": 0,
            "dtype_narrowing_value_mismatch_columns": 0,
            "dtype_narrowing_unverified_columns": 0,
            "null_mismatch_columns": 0,
            "temporal_improvement_columns": 0,
            "temporal_regression_columns": 0,
        },
        "format_counts": {},
        "examples": {
            "read_improvement": [],
            "read_regression": [],
            "temporal_improvement": [],
            "temporal_regression": [],
            "dtype_narrowing_improvement": [],
            "dtype_narrowing_regression": [],
            "dtype_narrowing_value_mismatch": [],
            "dtype_narrowing_unverified": [],
        },
    }

    for rel in all_files:
        ea = get_engine(cur, old, rel, a)
        eb = get_engine(cur, old, rel, b)
        if ea is None or eb is None:
            continue

        ext = Path(rel).suffix.lower()
        out["format_counts"].setdefault(ext, init_format_counts())

        ok_a = bool(ea.get("ok"))
        ok_b = bool(eb.get("ok"))
        if ok_a and not ok_b:
            out["counts"]["read_improvement"] += 1
            out["format_counts"][ext]["read_improvement"] += 1
            if len(out["examples"]["read_improvement"]) < max_examples:
                out["examples"]["read_improvement"].append(
                    {"file": rel, "a_error": ea.get("error"), "b_error": eb.get("error")}
                )
            continue
        if not ok_a and ok_b:
            out["counts"]["read_regression"] += 1
            out["format_counts"][ext]["read_regression"] += 1
            if len(out["examples"]["read_regression"]) < max_examples:
                out["examples"]["read_regression"].append(
                    {"file": rel, "a_error": ea.get("error"), "b_error": eb.get("error")}
                )
            continue
        if not ok_a and not ok_b:
            out["counts"]["both_read_fail"] += 1
            out["format_counts"][ext]["both_read_fail"] += 1
            continue

        out["counts"]["both_read_ok"] += 1
        out["format_counts"][ext]["both_read_ok"] += 1

        if (ea.get("rows"), ea.get("cols")) != (eb.get("rows"), eb.get("cols")):
            out["counts"]["shape_mismatch_files"] += 1

        aschema = ea.get("schema", {})
        bschema = eb.get("schema", {})
        for col in sorted(set(aschema) & set(bschema)):
            ad = aschema.get(col)
            bd = bschema.get(col)
            if ad == bd:
                continue

            if is_int_narrower(ad, bd) or is_int_narrower(bd, ad):
                eq = width_diff_verified_equal(ea, eb, col)
                if eq is None:
                    out["counts"]["dtype_narrowing_unverified_columns"] += 1
                    out["format_counts"][ext]["dtype_narrowing_unverified_columns"] += 1
                    if len(out["examples"]["dtype_narrowing_unverified"]) < max_examples:
                        out["examples"]["dtype_narrowing_unverified"].append(
                            {"file": rel, "column": col, "a_dtype": ad, "b_dtype": bd}
                        )
                elif eq:
                    if is_int_narrower(ad, bd):
                        out["counts"]["dtype_narrowing_improvement_columns"] += 1
                        out["format_counts"][ext]["dtype_narrowing_improvement_columns"] += 1
                        if len(out["examples"]["dtype_narrowing_improvement"]) < max_examples:
                            out["examples"]["dtype_narrowing_improvement"].append(
                                {"file": rel, "column": col, "a_dtype": ad, "b_dtype": bd}
                            )
                    else:
                        out["counts"]["dtype_narrowing_regression_columns"] += 1
                        out["format_counts"][ext]["dtype_narrowing_regression_columns"] += 1
                        if len(out["examples"]["dtype_narrowing_regression"]) < max_examples:
                            out["examples"]["dtype_narrowing_regression"].append(
                                {"file": rel, "column": col, "a_dtype": ad, "b_dtype": bd}
                            )
                else:
                    out["counts"]["dtype_narrowing_value_mismatch_columns"] += 1
                    out["format_counts"][ext]["dtype_narrowing_value_mismatch_columns"] += 1
                    out["counts"]["dtype_mismatch_columns"] += 1
                    out["format_counts"][ext]["dtype_mismatch_columns"] += 1
                    if len(out["examples"]["dtype_narrowing_value_mismatch"]) < max_examples:
                        out["examples"]["dtype_narrowing_value_mismatch"].append(
                            {"file": rel, "column": col, "a_dtype": ad, "b_dtype": bd}
                        )
                continue

            out["counts"]["dtype_mismatch_columns"] += 1
            out["format_counts"][ext]["dtype_mismatch_columns"] += 1

            at = is_temporal(ad)
            bt = is_temporal(bd)
            if at and not bt:
                out["counts"]["temporal_improvement_columns"] += 1
                out["format_counts"][ext]["temporal_improvement_columns"] += 1
                if len(out["examples"]["temporal_improvement"]) < max_examples:
                    out["examples"]["temporal_improvement"].append(
                        {"file": rel, "column": col, "a_dtype": ad, "b_dtype": bd}
                    )
            elif bt and not at:
                out["counts"]["temporal_regression_columns"] += 1
                out["format_counts"][ext]["temporal_regression_columns"] += 1
                if len(out["examples"]["temporal_regression"]) < max_examples:
                    out["examples"]["temporal_regression"].append(
                        {"file": rel, "column": col, "a_dtype": ad, "b_dtype": bd}
                    )

        anulls = ea.get("null_counts", {})
        bnulls = eb.get("null_counts", {})
        for col in sorted(set(anulls) & set(bnulls)):
            if anulls.get(col) != bnulls.get(col):
                out["counts"]["null_mismatch_columns"] += 1
                out["format_counts"][ext]["null_mismatch_columns"] += 1

    return out


def current_vs_old_relative(cur: dict, old: dict, ref: str) -> dict:
    all_files = sorted(set(cur.get("files", {}).keys()) | set(old.get("files", {}).keys()))
    out = {
        "files_all_ok": 0,
        "dtype_closer_than_old": 0,
        "dtype_farther_than_old": 0,
        "dtype_same_as_old": 0,
        "null_closer_than_old": 0,
        "null_farther_than_old": 0,
        "null_same_as_old": 0,
    }

    for rel in all_files:
        ec = get_engine(cur, old, rel, "current")
        eo = get_engine(cur, old, rel, "old0111")
        er = get_engine(cur, old, rel, ref)
        if ec is None or eo is None or er is None:
            continue
        if not (ec.get("ok") and eo.get("ok") and er.get("ok")):
            continue

        out["files_all_ok"] += 1
        c_dtype = 0
        o_dtype = 0
        c_null = 0
        o_null = 0

        for col in set(ec.get("schema", {})) & set(er.get("schema", {})):
            cd = ec["schema"].get(col)
            rd = er["schema"].get(col)
            if cd == rd:
                continue
            if is_int_narrower(cd, rd) or is_int_narrower(rd, cd):
                eq = width_diff_verified_equal(ec, er, col)
                if eq:
                    continue
            c_dtype += 1

        for col in set(eo.get("schema", {})) & set(er.get("schema", {})):
            od = eo["schema"].get(col)
            rd = er["schema"].get(col)
            if od == rd:
                continue
            if is_int_narrower(od, rd) or is_int_narrower(rd, od):
                eq = width_diff_verified_equal(eo, er, col)
                if eq:
                    continue
            o_dtype += 1

        for col in set(ec.get("null_counts", {})) & set(er.get("null_counts", {})):
            if ec["null_counts"].get(col) != er["null_counts"].get(col):
                c_null += 1
        for col in set(eo.get("null_counts", {})) & set(er.get("null_counts", {})):
            if eo["null_counts"].get(col) != er["null_counts"].get(col):
                o_null += 1

        if c_dtype < o_dtype:
            out["dtype_closer_than_old"] += 1
        elif c_dtype > o_dtype:
            out["dtype_farther_than_old"] += 1
        else:
            out["dtype_same_as_old"] += 1

        if c_null < o_null:
            out["null_closer_than_old"] += 1
        elif c_null > o_null:
            out["null_farther_than_old"] += 1
        else:
            out["null_same_as_old"] += 1

    return out


def main() -> int:
    args = parse_args()
    cur = json.loads(args.current_manifest.read_text(encoding="utf-8"))
    old = json.loads(args.old_manifest.read_text(encoding="utf-8"))

    pairs = [
        ("current", "old0111"),
        ("current", "pyreadstat"),
        ("current", "pandas"),
        ("old0111", "pyreadstat"),
        ("old0111", "pandas"),
        ("pyreadstat", "pandas"),
    ]

    summary = {
        "file_count": len(set(cur.get("files", {}).keys()) | set(old.get("files", {}).keys())),
        "pairs": {},
        "relative_to_old": {
            "vs_pyreadstat": current_vs_old_relative(cur, old, "pyreadstat"),
            "vs_pandas": current_vs_old_relative(cur, old, "pandas"),
        },
    }

    for a, b in pairs:
        key = f"{a}__vs__{b}"
        summary["pairs"][key] = pair_summary(cur, old, a, b, args.examples)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Wrote summary: {args.out}")
    print(f"Files considered: {summary['file_count']}")
    for key in ["current__vs__old0111", "current__vs__pyreadstat", "current__vs__pandas"]:
        counts = summary["pairs"][key]["counts"]
        print(
            f"{key}: both_ok={counts['both_read_ok']} "
            f"read_improvement={counts['read_improvement']} "
            f"read_regression={counts['read_regression']} "
            f"dtype_cols={counts['dtype_mismatch_columns']} "
            f"narrow_imp={counts['dtype_narrowing_improvement_columns']} "
            f"narrow_reg={counts['dtype_narrowing_regression_columns']} "
            f"narrow_val_mismatch={counts['dtype_narrowing_value_mismatch_columns']} "
            f"narrow_unverified={counts['dtype_narrowing_unverified_columns']} "
            f"null_cols={counts['null_mismatch_columns']} "
            f"temp_imp={counts['temporal_improvement_columns']} "
            f"temp_reg={counts['temporal_regression_columns']}"
        )

    rel_py = summary["relative_to_old"]["vs_pyreadstat"]
    rel_pd = summary["relative_to_old"]["vs_pandas"]
    print(
        "current closer than old (files): "
        f"pyreadstat dtype={rel_py['dtype_closer_than_old']} null={rel_py['null_closer_than_old']} | "
        f"pandas dtype={rel_pd['dtype_closer_than_old']} null={rel_pd['null_closer_than_old']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
