#!/bin/sh
set -eu

api="${RELAYFLOW_URL:-http://localhost:8080}"
run_id="$(date +%s)-$$"

wait_for_status() {
  workflow_id=$1
  expected=$2
  attempts=0
  while [ "$attempts" -lt 120 ]; do
    workflow_result=$(curl --fail --silent --show-error "$api/v1/workflows/$workflow_id")
    workflow_status=$(printf '%s' "$workflow_result" | sed -n 's/^{"id":"[^"]*","idempotency_key":"[^"]*","name":"[^"]*","status":"\([^"]*\)".*/\1/p')
    if [ "$workflow_status" = "$expected" ]; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 0.1
  done
  echo "workflow $workflow_id did not reach $expected; last response: $workflow_result" >&2
  exit 1
}

submit() {
  response=$(curl --fail --silent --show-error -H 'Content-Type: application/json' -d "$1" "$api/v1/workflows")
  workflow_id=$(printf '%s' "$response" | sed -n 's/^{"id":"\([^"]*\)".*/\1/p')
  if [ -z "$workflow_id" ]; then
    echo "submission did not return a workflow id: $response" >&2
    exit 1
  fi
}

cycle_code=$(curl --silent --output /dev/null --write-out '%{http_code}' -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"cycle-$run_id\",\"name\":\"cycle\",\"steps\":[{\"key\":\"a\",\"kind\":\"noop\",\"depends_on\":[\"b\"]},{\"key\":\"b\",\"kind\":\"noop\",\"depends_on\":[\"a\"]}]}" \
  "$api/v1/workflows")
[ "$cycle_code" = "422" ] || { echo "cycle submission returned HTTP $cycle_code" >&2; exit 1; }

submit "{\"idempotency_key\":\"branch-$run_id\",\"name\":\"branching workflow\",\"steps\":[{\"key\":\"root\",\"kind\":\"noop\",\"input\":{}},{\"key\":\"left\",\"kind\":\"sleep\",\"input\":{\"duration\":\"500ms\"},\"depends_on\":[\"root\"]},{\"key\":\"right\",\"kind\":\"sleep\",\"input\":{\"duration\":\"500ms\"},\"depends_on\":[\"root\"]},{\"key\":\"join\",\"kind\":\"echo\",\"input\":{\"joined\":true},\"depends_on\":[\"left\",\"right\"]}]}"
branch_id=$workflow_id
wait_for_status "$branch_id" succeeded
branch_state=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(step_key || ':' || status, ',' ORDER BY step_key) FROM steps WHERE workflow_id='$branch_id'::uuid;")
[ "$branch_state" = "join:succeeded,left:succeeded,right:succeeded,root:succeeded" ] || { echo "unexpected branch state: $branch_state" >&2; exit 1; }
parallel_gap=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT round(abs(extract(epoch FROM (max(a.started_at)-min(a.started_at))))*1000) FROM attempts a JOIN steps s ON s.id=a.step_id WHERE s.workflow_id='$branch_id'::uuid AND s.step_key IN ('left','right');")
[ "$parallel_gap" -lt 300 ] || { echo "independent branches did not start in parallel: gap=${parallel_gap}ms" >&2; exit 1; }

submit "{\"idempotency_key\":\"retry-$run_id\",\"name\":\"retry workflow\",\"steps\":[{\"key\":\"unstable\",\"kind\":\"flaky\",\"input\":{\"fail_attempts\":2},\"retry\":{\"max_attempts\":3,\"initial_backoff\":\"100ms\"},\"timeout\":\"2s\"}]}"
retry_id=$workflow_id
wait_for_status "$retry_id" succeeded
retry_history=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(a.status,',' ORDER BY a.attempt_no) FROM attempts a JOIN steps s ON s.id=a.step_id WHERE s.workflow_id='$retry_id'::uuid;")
[ "$retry_history" = "failed,failed,succeeded" ] || { echo "unexpected retry history: $retry_history" >&2; exit 1; }
retry_events=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT count(*) FROM workflow_events WHERE workflow_id='$retry_id'::uuid AND event_type='step.retry_scheduled';")
[ "$retry_events" -eq 2 ] || { echo "expected 2 retry events, got $retry_events" >&2; exit 1; }
retry_backoffs=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(payload->>'backoff_ms',',' ORDER BY id) FROM workflow_events WHERE workflow_id='$retry_id'::uuid AND event_type='step.retry_scheduled';")
[ "$retry_backoffs" = "100,200" ] || { echo "unexpected retry backoffs: $retry_backoffs" >&2; exit 1; }

submit "{\"idempotency_key\":\"timeout-$run_id\",\"name\":\"timeout workflow\",\"steps\":[{\"key\":\"slow\",\"kind\":\"sleep\",\"input\":{\"duration\":\"1s\"},\"retry\":{\"max_attempts\":2,\"initial_backoff\":\"50ms\"},\"timeout\":\"100ms\"}]}"
timeout_id=$workflow_id
wait_for_status "$timeout_id" failed
timeout_history=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(a.status,',' ORDER BY a.attempt_no) FROM attempts a JOIN steps s ON s.id=a.step_id WHERE s.workflow_id='$timeout_id'::uuid;")
[ "$timeout_history" = "timed_out,timed_out" ] || { echo "unexpected timeout history: $timeout_history" >&2; exit 1; }

submit "{\"idempotency_key\":\"cancel-$run_id\",\"name\":\"cancel workflow\",\"steps\":[{\"key\":\"long\",\"kind\":\"sleep\",\"input\":{\"duration\":\"5s\"},\"timeout\":\"10s\"}]}"
cancel_id=$workflow_id
attempts=0
while [ "$attempts" -lt 50 ]; do
  running=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc "SELECT count(*) FROM steps WHERE workflow_id='$cancel_id'::uuid AND status='running';")
  [ "$running" -eq 1 ] && break
  attempts=$((attempts + 1))
  sleep 0.1
done
[ "$running" -eq 1 ] || { echo "cancellation target never started" >&2; exit 1; }
cancel_result=$(curl --fail --silent --show-error -X POST "$api/v1/workflows/$cancel_id/cancel")
printf '%s' "$cancel_result" | grep -q '"status":"cancelled"'
cancel_state=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT w.status || '|' || s.status || '|' || a.status FROM workflows w JOIN steps s ON s.workflow_id=w.id JOIN attempts a ON a.step_id=s.id WHERE w.id='$cancel_id'::uuid;")
[ "$cancel_state" = "cancelled|cancelled|cancelled" ] || { echo "unexpected cancellation state: $cancel_state" >&2; exit 1; }

submit "{\"idempotency_key\":\"failure-$run_id\",\"name\":\"failure propagation\",\"steps\":[{\"key\":\"root\",\"kind\":\"fail\",\"input\":{}},{\"key\":\"dependent\",\"kind\":\"noop\",\"input\":{},\"depends_on\":[\"root\"]}]}"
failure_id=$workflow_id
wait_for_status "$failure_id" failed
failure_state=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT string_agg(step_key || ':' || status,',' ORDER BY step_key) FROM steps WHERE workflow_id='$failure_id'::uuid;")
[ "$failure_state" = "dependent:cancelled,root:failed" ] || { echo "unexpected failure propagation: $failure_state" >&2; exit 1; }

events=$(curl --fail --silent --show-error "$api/v1/workflows/$branch_id/events?limit=2")
printf '%s' "$events" | grep -q '"events":\['
event_count=$(printf '%s' "$events" | grep -o '"type":' | wc -l | tr -d ' ')
[ "$event_count" -eq 2 ] || { echo "event pagination returned $event_count events" >&2; exit 1; }
ordered=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT bool_and(id>previous_id) FROM (SELECT id,lag(id) OVER (ORDER BY id) previous_id FROM workflow_events WHERE workflow_id='$branch_id'::uuid) events WHERE previous_id IS NOT NULL;")
[ "$ordered" = "t" ] || { echo "event history is not ordered" >&2; exit 1; }

echo "semantics: cycle=rejected; branch=$branch_state; parallel_start_gap=${parallel_gap}ms; retry=$retry_history; backoff_ms=$retry_backoffs; timeout=$timeout_history; cancellation=$cancel_state; failure=$failure_state; event_page=$event_count"
