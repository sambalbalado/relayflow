#!/bin/sh
set -eu

api="${RELAYFLOW_URL:-http://localhost:8080}"
run_id="$(date +%s)-$$"

metric_value() {
  curl --fail --silent --show-error "$api/metrics" | awk -v name="$1" '$1 == name {print $2; exit}'
}

wait_for_status() {
  workflow_id=$1
  expected=$2
  attempts=0
  while [ "$attempts" -lt 120 ]; do
    workflow_result=$(curl --fail --silent --show-error "$api/v1/workflows/$workflow_id")
    workflow_status=$(printf '%s' "$workflow_result" | sed -n 's/^{"id":"[^"]*","idempotency_key":"[^"]*","name":"[^"]*","status":"\([^"]*\)".*/\1/p')
    [ "$workflow_status" = "$expected" ] && return 0
    attempts=$((attempts + 1))
    sleep 0.1
  done
  echo "workflow $workflow_id did not reach $expected" >&2
  exit 1
}

health=$(curl --fail --silent --show-error "$api/healthz")
readiness=$(curl --fail --silent --show-error "$api/readyz")
printf '%s' "$health" | grep -q '"status":"ok"'
printf '%s' "$readiness" | grep -q '"database":"ready"'

metrics=$(curl --fail --silent --show-error "$api/metrics")
for name in relayflow_steps relayflow_workflows relayflow_step_claims_total relayflow_step_retries_total relayflow_step_failures_total relayflow_step_timeouts_total relayflow_workflow_cancellations_total relayflow_lease_expirations_total relayflow_step_execution_duration_seconds_bucket; do
  printf '%s' "$metrics" | grep -q "$name"
done

ready_before=$(metric_value 'relayflow_steps{status="ready"}')
docker compose stop worker-a worker-b >/dev/null
index=0
while [ "$index" -lt 12 ]; do
  curl --fail --silent --show-error -H 'Content-Type: application/json' \
    -d "{\"idempotency_key\":\"backlog-$run_id-$index\",\"name\":\"backlog\",\"steps\":[{\"key\":\"work\",\"kind\":\"noop\",\"input\":{}}]}" \
    "$api/v1/workflows" >/dev/null
  index=$((index + 1))
done
ready_backlog=$(metric_value 'relayflow_steps{status="ready"}')
[ "$ready_backlog" -ge $((ready_before + 12)) ] || { echo "queue metric did not observe backlog growth" >&2; exit 1; }
docker compose up -d worker-a worker-b >/dev/null

attempts=0
while [ "$attempts" -lt 120 ]; do
  completed=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
    "SELECT count(*) FROM workflows WHERE idempotency_key LIKE 'backlog-$run_id-%' AND status='succeeded';")
  [ "$completed" -eq 12 ] && break
  attempts=$((attempts + 1))
  sleep 0.1
done
[ "$completed" -eq 12 ] || { echo "backlog did not drain" >&2; exit 1; }

retry_before=$(metric_value relayflow_step_retries_total)
failure_before=$(metric_value relayflow_step_failures_total)
retry_response=$(curl --fail --silent --show-error -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"metrics-retry-$run_id\",\"name\":\"metrics retry\",\"steps\":[{\"key\":\"work\",\"kind\":\"flaky\",\"input\":{\"fail_attempts\":2},\"retry\":{\"max_attempts\":3,\"initial_backoff\":\"10ms\"}}]}" \
  "$api/v1/workflows")
retry_id=$(printf '%s' "$retry_response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
wait_for_status "$retry_id" succeeded

failure_response=$(curl --fail --silent --show-error -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"metrics-failure-$run_id\",\"name\":\"metrics failure\",\"steps\":[{\"key\":\"work\",\"kind\":\"fail\",\"input\":{}}]}" \
  "$api/v1/workflows")
failure_id=$(printf '%s' "$failure_response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
wait_for_status "$failure_id" failed

retry_after=$(metric_value relayflow_step_retries_total)
failure_after=$(metric_value relayflow_step_failures_total)
[ "$retry_after" -eq $((retry_before + 2)) ] || { echo "retry metric delta was not 2" >&2; exit 1; }
[ "$failure_after" -eq $((failure_before + 1)) ] || { echo "failure metric delta was not 1" >&2; exit 1; }

claims_metric=$(metric_value relayflow_step_claims_total)
claims_database=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT count(*) FROM workflow_events WHERE event_type='step.claimed';")
[ "$claims_metric" -eq "$claims_database" ] || { echo "claim metric $claims_metric differs from durable count $claims_database" >&2; exit 1; }

docker compose logs --no-color --tail=200 api | grep -q '"request_id"'
docker compose logs --no-color --tail=200 worker-a worker-b | grep -q '"workflow_id"'

echo "observability: health=ok; readiness=database-ready; backlog=${ready_before}->${ready_backlog}->drained; retry_delta=2; failure_delta=1; durable_claims=$claims_metric; structured_logs=correlated"
