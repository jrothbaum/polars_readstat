#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES_ROOT="${1:-$REPO_ROOT/../polars_readstat_rs/tests}"
OUT_ROOT="${2:-$REPO_ROOT/.tmp/compare_0111}"
MAX_BYTES="${MAX_BYTES:-104857600}"

BASELINE_OUT="$OUT_ROOT/baseline_0111"
CURRENT_OUT="$OUT_ROOT/current"

rm -rf "$BASELINE_OUT" "$CURRENT_OUT"
mkdir -p "$OUT_ROOT"

echo "Exporting baseline (0.11.1) parquet snapshots..."
uv run "$REPO_ROOT/scripts/export_fixture_parquet_0111.py" \
  --fixtures-root "$FIXTURES_ROOT" \
  --out-dir "$BASELINE_OUT" \
  --max-bytes "$MAX_BYTES" \
  --label "0.11.1"

echo "Exporting current parquet snapshots..."
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" \
uv run --directory "$REPO_ROOT" --group test python "$REPO_ROOT/scripts/export_fixture_parquet.py" \
  --fixtures-root "$FIXTURES_ROOT" \
  --out-dir "$CURRENT_OUT" \
  --max-bytes "$MAX_BYTES" \
  --label "current"

echo "Comparing snapshot manifests..."
uv run --directory "$REPO_ROOT" --group test python "$REPO_ROOT/scripts/compare_fixture_parquet.py" \
  --baseline-dir "$BASELINE_OUT" \
  --current-dir "$CURRENT_OUT" \
  --report-json "$OUT_ROOT/report.json"

echo "Done. Report: $OUT_ROOT/report.json"
