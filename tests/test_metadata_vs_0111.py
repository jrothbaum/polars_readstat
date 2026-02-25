from __future__ import annotations

from pathlib import Path
import json
import subprocess
from typing import Any

import polars_readstat as prs


def _run_generator(tmp_path: Path, variant: str) -> Path:
    script = Path(__file__).with_name("generate_long_string_value_labels.py")
    out_path = tmp_path / f"long_string_value_labels_{variant}.sav"
    subprocess.run(
        ["uv", "run", str(script), "--out", str(out_path), "--variant", variant],
        check=True,
        cwd=tmp_path,
    )
    return out_path


def _extract_json(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise AssertionError(f"No JSON found in output:\n{stdout}")


def _legacy_metadata(path: Path, tmp_path: Path) -> dict[str, Any]:
    script = tmp_path / "legacy_metadata.py"
    script.write_text(
        """
import json
import sys
from polars_readstat import ScanReadstat

path = sys.argv[1]
md = ScanReadstat(path).metadata
print(json.dumps(md, ensure_ascii=False))
""".lstrip(),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "uv",
            "run",
            "--with",
            "polars-readstat==0.11.1",
            "--with",
            "polars",
            str(script),
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    return _extract_json(result.stdout)


def _value_labels_map(metadata: dict[str, Any]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    variables = metadata.get("variables") or metadata.get("columns") or []
    for var in variables:
        name = str(var.get("name", ""))
        labels = var.get("value_labels")
        if not name or not isinstance(labels, dict):
            continue
        out[name] = {str(k): str(v) for k, v in labels.items()}
    return out


def test_metadata_value_labels_matches_0111(tmp_path: Path) -> None:
    out_path = _run_generator(tmp_path, "basic")

    current_md = prs.ScanReadstat(path=str(out_path)).metadata
    legacy_md = _legacy_metadata(out_path, tmp_path)

    current_labels = _value_labels_map(current_md)
    legacy_labels = _value_labels_map(legacy_md)

    assert legacy_labels, "expected legacy metadata to include value_labels"
    for var_name, legacy_map in legacy_labels.items():
        assert var_name in current_labels, f"missing value_labels for {var_name}"
        assert current_labels[var_name] == legacy_map
