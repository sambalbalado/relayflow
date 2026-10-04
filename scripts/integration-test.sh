#!/bin/sh
set -eu

api_url="${RELAYFLOW_URL:-http://localhost:8080}"

attempt=0
until curl --fail --silent "$api_url/readyz" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "API did not become ready" >&2
    docker compose logs api worker-a worker-b >&2
    exit 1
  fi
  sleep 1
done

key="integration-$(date +%s)-$$"
output=$(RELAYFLOW_KEY="$key" ./scripts/demo.sh)
printf '%s' "$output" | grep -q '"status":"succeeded"'
printf '%s' "$output" | grep -q '"attempt_count":1'
printf '%s' "$output" | grep -q 'hello from RelayFlow'

event_count=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc "SELECT count(*) FROM workflow_events e JOIN workflows w ON w.id = e.workflow_id WHERE w.idempotency_key = '$key';")
if [ "$event_count" -ne 5 ]; then
  echo "expected exactly 5 persisted lifecycle events, got $event_count" >&2
  exit 1
fi

persisted=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc "SELECT w.status || '|' || s.status || '|' || s.attempt_count || '|' || a.status FROM workflows w JOIN steps s ON s.workflow_id = w.id JOIN attempts a ON a.step_id = s.id WHERE w.idempotency_key = '$key';")
if [ "$persisted" != "succeeded|succeeded|1|succeeded" ]; then
  echo "unexpected persisted state: $persisted" >&2
  exit 1
fi

if docker compose exec -T db psql -U relayflow -d relayflow -v ON_ERROR_STOP=1 -c "UPDATE workflow_events SET event_type = 'tampered' WHERE workflow_id = (SELECT id FROM workflows WHERE idempotency_key = '$key');" >/dev/null 2>&1; then
  echo "append-only event history accepted an update" >&2
  exit 1
fi

echo "integration: workflow succeeded; persisted=$persisted; events=$event_count; event_mutation=rejected"

heartbeat_key="heartbeat-$(date +%s)-$$"
heartbeat_response=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"${heartbeat_key}\",\"name\":\"heartbeat verification\",\"steps\":[{\"key\":\"long-running\",\"kind\":\"sleep\",\"input\":{\"duration\":\"3s\"}}]}" \
  "$api_url/v1/workflows")
heartbeat_id=$(printf '%s' "$heartbeat_response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
attempt=0
heartbeat_result=""
while [ "$attempt" -lt 80 ]; do
  heartbeat_result=$(curl --fail --silent --show-error "$api_url/v1/workflows/${heartbeat_id}")
  status=$(printf '%s' "$heartbeat_result" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p' | head -n 1)
  [ "$status" = "succeeded" ] && break
  attempt=$((attempt + 1))
  sleep 0.1
done
printf '%s' "$heartbeat_result" | grep -q '"status":"succeeded"'
printf '%s' "$heartbeat_result" | grep -q '"attempt_count":1'
expired_count=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT count(*) FROM workflow_events e JOIN workflows w ON w.id=e.workflow_id WHERE w.idempotency_key='$heartbeat_key' AND e.event_type='step.lease_expired';")
if [ "$expired_count" -ne 0 ]; then
  echo "healthy heartbeat workflow was incorrectly reclaimed" >&2
  exit 1
fi
echo "heartbeat: long-running workflow succeeded in one attempt; lease_expiry_events=0"
