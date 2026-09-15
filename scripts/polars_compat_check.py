#!/usr/bin/env python3
"""Check which released `polars` versions work with the current polars_readstat build.

Not a pytest test. For each non-yanked polars release on PyPI (from --min-version
up), spins up an ephemeral env via `uv run --with polars==X.Y.Z` on top of this
project's own environment (which has polars_readstat installed in editable mode)
and tries to read a small SAS, Stata, and SPSS fixture -- each with a mix of
string, numeric, and date columns -- through polars_readstat. Reports pass/fail
per version.

Usage:
    uv run scripts/polars_compat_check.py
    uv run scripts/polars_compat_check.py --min-version 1.30.0 --include-prereleases
    uv run scripts/polars_compat_check.py --versions 1.28.1,1.44.1
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

from packaging.version import InvalidVersion, Version

REPO_ROOT = Path(__file__).resolve().parent.parent
PROBE_SCRIPT = Path(__file__).resolve().parent / "_polars_compat_probe.py"

FIXTURES = {
    "sas": REPO_ROOT / "crates/polars_readstat_rs/tests/sas/data/data_reikoch/dates.sas7bdat",
    "stata": REPO_ROOT / "crates/polars_readstat_rs/tests/stata/data/stata6_117.dta",
    "spss": REPO_ROOT / "crates/polars_readstat_rs/tests/spss/data/simple_alltypes.sav",
}

PYPI_JSON_URL = "https://pypi.org/pypi/polars/json"


def fetch_versions(min_version: Version, include_prereleases: bool) -> list[Version]:
    with urllib.request.urlopen(PYPI_JSON_URL, timeout=30) as resp:
        data = json.load(resp)

    versions: list[Version] = []
    for raw, files in data["releases"].items():
        if not files:
            continue
        # A release is yanked only if every uploaded file for it is yanked.
        if all(f.get("yanked", False) for f in files):
            continue
        try:
            v = Version(raw)
        except InvalidVersion:
            continue
        if v < min_version:
            continue
        if v.is_prerelease and not include_prereleases:
            continue
        versions.append(v)

    versions.sort()
    return versions


def run_one(version: Version, timeout: float) -> dict:
    cmd = [
        "uv", "run", "--with", f"polars=={version}",
        "python", str(PROBE_SCRIPT),
        str(FIXTURES["sas"]), str(FIXTURES["stata"]), str(FIXTURES["spss"]),
    ]
    try:
        proc = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"version": str(version), "ok": False, "error": f"timed out after {timeout}s"}

    if proc.returncode == 0 and "PROBE_OK" in proc.stdout:
        return {"version": str(version), "ok": True}

    # Keep the most informative tail of stderr (the actual exception), not the
    # `uv run` resolver chatter that can precede it.
    tail = "\n".join(proc.stderr.strip().splitlines()[-15:])
    return {"version": str(version), "ok": False, "error": tail or proc.stdout.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-version", default="1.28.1", help="Lowest polars version to test (default: 1.28.1)")
    parser.add_argument("--include-prereleases", action="store_true", help="Also test alpha/beta/rc releases")
    parser.add_argument("--versions", help="Comma-separated explicit version list, skips the PyPI lookup")
    parser.add_argument("--timeout", type=float, default=120.0, help="Per-version subprocess timeout in seconds")
    parser.add_argument("--json-out", type=Path, help="Optional path to write full JSON results")
    args = parser.parse_args()

    for name, path in FIXTURES.items():
        if not path.exists():
            print(f"error: {name} fixture not found: {path}", file=sys.stderr)
            return 2

    if args.versions:
        versions = sorted(Version(v.strip()) for v in args.versions.split(","))
    else:
        versions = fetch_versions(Version(args.min_version), args.include_prereleases)

    if not versions:
        print("No matching polars versions found.", file=sys.stderr)
        return 2

    print(f"Testing {len(versions)} polars version(s): {versions[0]} .. {versions[-1]}\n")

    results = []
    for v in versions:
        print(f"polars=={v} ... ", end="", flush=True)
        result = run_one(v, args.timeout)
        results.append(result)
        print("OK" if result["ok"] else "FAIL")
        if not result["ok"]:
            for line in result["error"].splitlines():
                print(f"    {line}")

    passed = [r["version"] for r in results if r["ok"]]
    failed = [r["version"] for r in results if not r["ok"]]

    print("\n--- summary ---")
    print(f"passed ({len(passed)}): {', '.join(passed) or '-'}")
    print(f"failed ({len(failed)}): {', '.join(failed) or '-'}")

    if args.json_out:
        args.json_out.write_text(json.dumps(results, indent=2))
        print(f"\nwrote {args.json_out}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
