#!/bin/sh
set -eu

api_url="${RELAYFLOW_URL:-http://localhost:8080}"
key="graceful-shutdown-$(date +%s)-$$"

restore_workers() {
  docker compose up -d worker-a worker-b >/dev/null 2>&1 || true
}
trap restore_workers EXIT

docker compose up --build -d db api worker-a worker-b >/dev/null
docker compose stop worker-b >/dev/null

response=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"${key}\",\"name\":\"graceful drain\",\"steps\":[{\"key\":\"slow-operation\",\"kind\":\"sleep\",\"input\":{\"duration\":\"2s\"},\"timeout\":\"5s\"}]}" \
  "${api_url}/v1/workflows")
workflow_id=$(printf '%s' "$response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
[ -n "$workflow_id" ] || { echo "could not parse workflow ID: $response" >&2; exit 1; }

attempt=0
owner=""
while [ "$attempt" -lt 50 ]; do
  owner=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
    "SELECT COALESCE(s.worker_id, '') FROM steps s JOIN workflows w ON w.id=s.workflow_id WHERE w.idempotency_key='$key';")
  [ "$owner" = "worker-a" ] && break
  attempt=$((attempt + 1))
  sleep 0.1
done
[ "$owner" = "worker-a" ] || { echo "worker-a did not claim the drain-test step" >&2; exit 1; }

docker compose stop -t 10 worker-a >/dev/null
result=$(curl --fail --silent --show-error "${api_url}/v1/workflows/${workflow_id}")
printf '%s' "$result" | grep -q '"status":"succeeded"'
printf '%s' "$result" | grep -q '"attempt_count":1'

history=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(status || ':' || worker_id, ',' ORDER BY attempt_no) FROM attempts WHERE step_id=(SELECT s.id FROM steps s JOIN workflows w ON w.id=s.workflow_id WHERE w.idempotency_key='$key');")
[ "$history" = "succeeded:worker-a" ] || { echo "unexpected attempt history: $history" >&2; exit 1; }
docker compose logs --no-color worker-a | grep -q '"drained":true'

echo "graceful shutdown: workflow=succeeded; attempts=$history; drain=completed-before-exit"
