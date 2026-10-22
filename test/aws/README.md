# Real-AWS S3 integration test

Opt-in: skipped unless `PRS_TEST_S3_BUCKET` is set, so it never runs in CI by accident.
(`tests/test_s3_storage_options.py` covers the no-network plumbing.)

## One-time
1. `aws login --region us-east-2 --profile prs-test` (or `aws configure sso`)
2. `cp env.example env.local` and edit (env.local is gitignored)
3. `./setup.sh`   # creates the bucket + 1-day lifecycle expiry

## Each run
```bash
source ./env.local                      # exports short-lived creds + bucket
uv run pytest test/aws -v -s
```
`object_store` does not read AWS profiles, so `env.local` exports
`AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN` via `aws configure export-credentials`.

## Clean-venv check (PyPI install)
`./run_clean_venv.sh` builds a fresh uv venv with only `polars` + `polars-readstat` from PyPI,
uploads one small sample per format, then reads and writes each format against S3 and prints a
PASS/FAIL table. `PRS_VERSION=x.y.z` pins the release. sas7bdat is read-only (no binary writer).

## Cleanup
`./teardown.sh` empties and deletes the bucket.

## Where credentials live (no secrets in this repo)
- Preferred: `aws login` / SSO. The CLI caches short-lived tokens under `~/.aws/` itself;
  `env.local` only *runs* `aws configure export-credentials` when sourced, so it holds no secrets.
- If you must use a long-lived IAM access key: put it in `~/.aws/credentials` under `[prs-test]`
  (`chmod 600 ~/.aws/credentials`), never in the repo, and delete the key when done.
- `env.local` is gitignored; `git check-ignore test/aws/env.local` should print the path.
