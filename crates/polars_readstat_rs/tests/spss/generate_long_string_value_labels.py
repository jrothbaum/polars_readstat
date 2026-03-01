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


def build_frame():
    long_value = "A" * 300
    values = [
        long_value,
        long_value.replace("A", "B"),
        long_value.replace("A", "C"),
    ]
    df = pd.DataFrame(
        {
            "longstr": values,
            "id": [1, 2, 3],
        }
    )
    long_label = "Label_" + ("L" * 300)
    value_labels = {
        "longstr": {
            values[0]: long_label,
            values[1]: long_label,
            values[2]: long_label,
        }
    }
    column_labels = {
        "longstr": "Long string with value labels",
    }
    return df, value_labels, column_labels


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate an SPSS .sav with long string value labels via pyreadstat."
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output .sav path",
    )
    args = parser.parse_args()

    out = args.out
    out.parent.mkdir(parents=True, exist_ok=True)

    df, value_labels, column_labels = build_frame()
    pyreadstat.write_sav(
        df,
        out,
        column_labels=column_labels,
        variable_value_labels=value_labels,
    )
    print(out)


if __name__ == "__main__":
    main()
