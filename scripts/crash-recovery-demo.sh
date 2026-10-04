#!/bin/sh
set -eu

api_url="${RELAYFLOW_URL:-http://localhost:8080}"
key="crash-recovery-$(date +%s)-$$"

restore_workers() {
  docker compose up -d worker-a worker-b >/dev/null 2>&1 || true
}
trap restore_workers EXIT

docker compose up --build -d db api worker-a worker-b >/dev/null
docker compose stop worker-b >/dev/null

response=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"${key}\",\"name\":\"crash recovery\",\"steps\":[{\"key\":\"slow-operation\",\"kind\":\"sleep\",\"input\":{\"duration\":\"4s\"}}]}" \
  "${api_url}/v1/workflows")
workflow_id=$(printf '%s' "$response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
if [ -z "$workflow_id" ]; then
  echo "could not parse workflow ID: $response" >&2
  exit 1
fi

attempt=0
owner=""
while [ "$attempt" -lt 50 ]; do
  owner=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
    "SELECT COALESCE(s.worker_id, '') FROM steps s JOIN workflows w ON w.id=s.workflow_id WHERE w.idempotency_key='$key';")
  [ "$owner" = "worker-a" ] && break
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ "$owner" != "worker-a" ]; then
  echo "worker-a did not claim the crash-test step" >&2
  exit 1
fi

docker compose kill -s SIGKILL worker-a >/dev/null
docker compose up -d worker-b >/dev/null

attempt=0
result=""
while [ "$attempt" -lt 150 ]; do
  result=$(curl --fail --silent --show-error "${api_url}/v1/workflows/${workflow_id}")
  status=$(printf '%s' "$result" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p' | head -n 1)
  [ "$status" = "succeeded" ] && break
  attempt=$((attempt + 1))
  sleep 0.1
done
printf '%s' "$result" | grep -q '"status":"succeeded"'
printf '%s' "$result" | grep -q '"attempt_count":2'

attempts=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(status || ':' || worker_id, ',' ORDER BY attempt_no) FROM attempts WHERE step_id=(SELECT s.id FROM steps s JOIN workflows w ON w.id=s.workflow_id WHERE w.idempotency_key='$key');")
if [ "$attempts" != "lease_expired:worker-a,succeeded:worker-b" ]; then
  echo "unexpected attempt history: $attempts" >&2
  exit 1
fi

reclaims=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT count(*) FROM workflow_events e JOIN workflows w ON w.id=e.workflow_id WHERE w.idempotency_key='$key' AND e.event_type='step.lease_expired';")
if [ "$reclaims" -ne 1 ]; then
  echo "expected one lease-expiry event, got $reclaims" >&2
  exit 1
fi

echo "crash recovery: workflow=succeeded; attempts=$attempts; lease_expiry_events=$reclaims"
