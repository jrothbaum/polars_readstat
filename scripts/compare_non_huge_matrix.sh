#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES_ROOT="${1:-$REPO_ROOT/../polars_readstat_rs/tests}"
OUT_ROOT="${2:-$REPO_ROOT/.tmp/compare_matrix}"
MAX_BYTES="${MAX_BYTES:-104857600}"
MISSING_STRING_AS_NULL="${MISSING_STRING_AS_NULL:-0}"

CURRENT_MANIFEST="$OUT_ROOT/current_py_pd.json"
OLD_MANIFEST="$OUT_ROOT/old0111.json"
SUMMARY_JSON="$OUT_ROOT/summary.json"

mkdir -p "$OUT_ROOT"

CURRENT_EXTRA_ARGS=()
if [ "$MISSING_STRING_AS_NULL" = "1" ]; then
  CURRENT_EXTRA_ARGS+=("--missing-string-as-null")
fi

echo "Collecting current + pyreadstat + pandas results..."
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" \
uv run --directory "$REPO_ROOT" --group test python "$REPO_ROOT/scripts/collect_matrix_current.py" \
  --fixtures-root "$FIXTURES_ROOT" \
  --out "$CURRENT_MANIFEST" \
  --max-bytes "$MAX_BYTES" \
  "${CURRENT_EXTRA_ARGS[@]}"

echo "Collecting polars-readstat==0.11.1 results..."
uv run "$REPO_ROOT/scripts/collect_matrix_0111.py" \
  --fixtures-root "$FIXTURES_ROOT" \
  --out "$OLD_MANIFEST" \
  --max-bytes "$MAX_BYTES"

echo "Summarizing 4-way comparison..."
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" \
uv run --directory "$REPO_ROOT" --group test python "$REPO_ROOT/scripts/summarize_matrix.py" \
  --current-manifest "$CURRENT_MANIFEST" \
  --old-manifest "$OLD_MANIFEST" \
  --out "$SUMMARY_JSON"

echo "Done. Summary JSON: $SUMMARY_JSON"
