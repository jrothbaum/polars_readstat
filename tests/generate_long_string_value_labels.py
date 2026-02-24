# /// script
# dependencies = [
#   "pandas",
#   "pyreadstat",
# ]
# ///
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyreadstat


def _label(base: str, suffix: str = "") -> str:
    # Keep labels long to force long string value label records.
    tail = "L" * 260
    return f"{base}{tail}{suffix}"


def _repeat(ch: str) -> str:
    return ch * 300


def build_frame(variant: str):
    if variant == "basic":
        values = [_repeat("A"), _repeat("B"), _repeat("C")]
        df = pd.DataFrame({"longstr": values, "id": [1, 2, 3]})
        value_labels = {
            "longstr": {
                values[0]: _label("Label_"),
                values[1]: _label("Label_"),
                values[2]: _label("Label_"),
            }
        }
        column_labels = {"longstr": "Long string with value labels"}
        return df, value_labels, column_labels

    if variant == "multi":
        values = [_repeat(ch) for ch in ["A", "B", "C", "D", "E"]]
        df = pd.DataFrame({"longstr": values, "id": [1, 2, 3, 4, 5]})
        value_labels = {
            "longstr": {
                values[0]: _label("Label_", "_0"),
                values[1]: _label("Label_", "_1"),
                values[2]: _label("Label_", "_2"),
                values[3]: _label("Label_", "_3"),
                values[4]: _label("Label_", "_4"),
            }
        }
        column_labels = {"longstr": "Long string with multiple labels"}
        return df, value_labels, column_labels

    if variant == "multi-vars":
        values_a = [_repeat("A"), _repeat("B"), _repeat("C")]
        values_b = [_repeat("X"), _repeat("Y"), _repeat("Z")]
        df = pd.DataFrame(
            {
                "longstr_a": values_a,
                "longstr_b": values_b,
                "id": [1, 2, 3],
            }
        )
        value_labels = {
            "longstr_a": {
                values_a[0]: _label("LabelA_", "_0"),
                values_a[1]: _label("LabelA_", "_1"),
                values_a[2]: _label("LabelA_", "_2"),
            },
            "longstr_b": {
                values_b[0]: _label("LabelB_", "_0"),
                values_b[1]: _label("LabelB_", "_1"),
                values_b[2]: _label("LabelB_", "_2"),
            },
        }
        column_labels = {
            "longstr_a": "Long string A",
            "longstr_b": "Long string B",
        }
        return df, value_labels, column_labels

    if variant == "unicode":
        values = [_repeat("A"), _repeat("B"), _repeat("C")]
        df = pd.DataFrame({"longstr": values, "id": [1, 2, 3]})
        unicode_tail = " Café — 測試 "
        label = _label("Label_") + unicode_tail
        value_labels = {
            "longstr": {
                values[0]: label,
                values[1]: label,
                values[2]: label,
            }
        }
        column_labels = {"longstr": "Long string with unicode labels"}
        return df, value_labels, column_labels

    raise ValueError(f"Unknown variant: {variant}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate SPSS .sav with long string value labels via pyreadstat."
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output .sav path",
    )
    parser.add_argument(
        "--variant",
        default="basic",
        choices=["basic", "multi", "multi-vars", "unicode"],
        help="Fixture variant to generate",
    )
    args = parser.parse_args()

    out = args.out
    out.parent.mkdir(parents=True, exist_ok=True)

    df, value_labels, column_labels = build_frame(args.variant)
    pyreadstat.write_sav(
        df,
        out,
        column_labels=column_labels,
        variable_value_labels=value_labels,
    )
    print(out)


if __name__ == "__main__":
    main()
