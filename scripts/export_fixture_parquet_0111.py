#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "polars-readstat==0.11.1",
#   "polars>=1.25.2",
#   "pyarrow>=19.0.1",
# ]
# ///

from export_fixture_parquet import main

if __name__ == "__main__":
    raise SystemExit(main())
