#!/usr/bin/env bash
# Fresh uv venv with only polars + polars-readstat (from PyPI), upload one small
# sample per format to S3, then read and write each format against S3.
#
#   source ./env.local && ./setup.sh   # once (bucket)
#   ./run_clean_venv.sh                # PRS_VERSION=0.25.0 to pin
#   PRS_LOCAL_PY=1 ./run_clean_venv.sh # overlay repo .py files on the PyPI wheel
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
data="$here/../../crates/polars_readstat_rs/tests"
: "${PRS_TEST_S3_BUCKET:?source env.local first}"
: "${AWS_REGION:?source env.local first}"
profile_args=()
[ -z "${AWS_ACCESS_KEY_ID:-}" ] && profile_args=(--profile "${AWS_PROFILE_NAME:?}")

work="$(mktemp -d)"; prefix="clean-venv-$(date +%s)"
trap 'aws s3 rm "s3://$PRS_TEST_S3_BUCKET/$prefix/" --recursive "${profile_args[@]}" >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

# one small sample per format, copied so the test never touches the repo tree
mkdir "$work/samples"
cp "$data/sas/data/data/file1.sas7bdat" "$data/stata/data/stata1_115.dta" \
   "$data/spss/data/sample.sav" "$data/spss/data/sample.zsav" \
   "$data/spss/data/sample.por" "$data/sas/data/xpt/sample.xpt" "$work/samples/"

# sas7bdat + its format catalog, kept in a subdir so the per-format glob ignores them
mkdir "$work/samples/catalog"
cp "$data/sas/data/haven/tagged-na.sas7bdat" "$data/sas/data/haven/tagged-na.sas7bcat" "$work/samples/catalog/"

aws s3 cp "$work/samples/" "s3://$PRS_TEST_S3_BUCKET/$prefix/in/" --recursive "${profile_args[@]}"

uv venv "$work/venv" --quiet
uv pip install --python "$work/venv/bin/python" --quiet \
  polars "polars-readstat${PRS_VERSION:+==$PRS_VERSION}"
if [ -n "${PRS_LOCAL_PY:-}" ]; then
  # overlay the repo's Python sources on the PyPI wheel (keeps its compiled extension)
  site="$("$work/venv/bin/python" -c 'import polars_readstat,os;print(os.path.dirname(polars_readstat.__file__))')"
  cp --remove-destination "$here"/../../polars_readstat/polars_readstat/*.py "$site/"  # uv hardlinks from its cache; don't write through
  echo "overlaid repo Python sources onto $site"
fi
uv pip list --python "$work/venv/bin/python"   # should show just polars + polars-readstat

"$work/venv/bin/python" -I "$here/clean_venv_s3_check.py" \
  "$PRS_TEST_S3_BUCKET" "$prefix" "$AWS_REGION" "$work/samples"
