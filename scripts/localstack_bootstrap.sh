#!/usr/bin/env bash
# Bootstrap LocalStack SQS resources for TP-2.
# Credentials come from the environment, never from code.
set -euo pipefail

ENDPOINT="${VULCANO_SQS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
QUEUE_NAME="${VULCANO_QUEUE_NAME:-vulcano-events-queue}"
DLQ_NAME="${VULCANO_DLQ_NAME:-vulcano-events-dlq}"
VISIBILITY_TIMEOUT="${VULCANO_SQS_VISIBILITY_TIMEOUT_SECONDS:-30}"
MAX_RECEIVE_COUNT="${VULCANO_SQS_MAX_RECEIVE_COUNT:-5}"
: "${AWS_ACCESS_KEY_ID:?Set AWS_ACCESS_KEY_ID (use 'test' for LocalStack)}"
: "${AWS_SECRET_ACCESS_KEY:?Set AWS_SECRET_ACCESS_KEY (use 'test' for LocalStack)}"

if [[ ! "$VISIBILITY_TIMEOUT" =~ ^(0|[1-9][0-9]*)$ ]] || (( VISIBILITY_TIMEOUT > 43200 )); then
  printf 'VULCANO_SQS_VISIBILITY_TIMEOUT_SECONDS must be an integer from 0 to 43200\n' >&2
  exit 2
fi

if [[ ! "$MAX_RECEIVE_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  printf 'VULCANO_SQS_MAX_RECEIVE_COUNT must be a positive integer\n' >&2
  exit 2
fi

aws_local() {
  aws --endpoint-url="$ENDPOINT" --region "$REGION" "$@"
}

aws_local sqs create-queue --queue-name "$DLQ_NAME" >/dev/null
aws_local sqs create-queue --queue-name "$QUEUE_NAME" >/dev/null

DLQ_URL="$(aws_local sqs get-queue-url --queue-name "$DLQ_NAME" --query QueueUrl --output text)"
QUEUE_URL="$(aws_local sqs get-queue-url --queue-name "$QUEUE_NAME" --query QueueUrl --output text)"
DLQ_ARN="$(aws_local sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"

# RedrivePolicy is JSON nested inside the attributes JSON; use Python to encode both levels.
ATTRIBUTES="$(python3 -c 'import json,sys; print(json.dumps({"VisibilityTimeout": sys.argv[2], "RedrivePolicy": json.dumps({"deadLetterTargetArn": sys.argv[1], "maxReceiveCount": sys.argv[3]})}))' "$DLQ_ARN" "$VISIBILITY_TIMEOUT" "$MAX_RECEIVE_COUNT")"
aws_local sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes "$ATTRIBUTES"

printf 'Queues ready: %s (visibility=%s, redrive -> %s, maxReceiveCount=%s)\n' \
  "$QUEUE_NAME" "$VISIBILITY_TIMEOUT" "$DLQ_NAME" "$MAX_RECEIVE_COUNT"
