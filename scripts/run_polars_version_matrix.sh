#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHEEL="$(ls -t "$SCRIPT_DIR/../target/wheels/polars_readstat-"*.whl | head -1)"

VERSIONS=(
    "1.25.2"
    "1.26.0"
    "1.28.1"
    "1.31.0"
    "1.33.1"
    "1.36.1"
    "1.40.1"
)

echo "Using wheel: $WHEEL"
echo

for version in "${VERSIONS[@]}"; do
    echo "=== polars==$version ==="
    uv run \
        --with "polars==$version" \
        --with "$WHEEL" \
        --refresh-package polars-readstat \
        "$SCRIPT_DIR/test_polars_versions.py" \
        && STATUS="PASS" || STATUS="FAIL"
    echo "  => $STATUS"
    echo
done
