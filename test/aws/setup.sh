#!/usr/bin/env bash
# Create the test bucket (needs s3:CreateBucket + s3:PutLifecycleConfiguration).
set -euo pipefail
: "${PRS_TEST_S3_BUCKET:?source env.local first}"
aws s3api create-bucket --bucket "$PRS_TEST_S3_BUCKET" --region "$AWS_REGION" \
  --create-bucket-configuration "LocationConstraint=$AWS_REGION" --profile "$AWS_PROFILE_NAME"
aws s3api put-public-access-block --bucket "$PRS_TEST_S3_BUCKET" --profile "$AWS_PROFILE_NAME" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-lifecycle-configuration --bucket "$PRS_TEST_S3_BUCKET" --profile "$AWS_PROFILE_NAME" \
  --lifecycle-configuration '{"Rules":[{"ID":"expire","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
echo "ready: s3://$PRS_TEST_S3_BUCKET"
