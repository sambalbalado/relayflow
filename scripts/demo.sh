#!/bin/sh
set -eu

api_url="${RELAYFLOW_URL:-http://localhost:8080}"
key="${RELAYFLOW_KEY:-demo-$(date +%s)}"

response=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"${key}\",\"name\":\"durable echo\",\"steps\":[{\"key\":\"hello\",\"kind\":\"echo\",\"input\":{\"message\":\"hello from RelayFlow\"}}]}" \
  "${api_url}/v1/workflows")

workflow_id=$(printf '%s' "$response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
if [ -z "$workflow_id" ]; then
  echo "could not parse workflow ID: $response" >&2
  exit 1
fi

attempt=0
while [ "$attempt" -lt 50 ]; do
  result=$(curl --fail --silent --show-error "${api_url}/v1/workflows/${workflow_id}")
  status=$(printf '%s' "$result" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p' | head -n 1)
  if [ "$status" = "succeeded" ]; then
    printf '%s\n' "$result"
    exit 0
  fi
  if [ "$status" = "failed" ] || [ "$status" = "cancelled" ]; then
    echo "$result" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done

echo "workflow did not finish: $result" >&2
exit 1
