#!/usr/bin/env bash
set -euo pipefail
: "${PRS_TEST_S3_BUCKET:?source env.local first}"
aws s3 rb "s3://$PRS_TEST_S3_BUCKET" --force --profile "$AWS_PROFILE_NAME"
