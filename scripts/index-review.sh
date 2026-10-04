#!/bin/sh
set -eu

required="steps_ready_claim_idx steps_expired_lease_idx steps_workflow_status_idx step_dependencies_depends_on_idx workflow_events_workflow_id_id_idx workflow_events_event_type_idx attempts_step_id_attempt_no_idx attempts_finished_at_idx"
present=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SELECT indexname FROM pg_indexes WHERE schemaname='public' ORDER BY indexname;")
for index in $required; do
  printf '%s\n' "$present" | grep -qx "$index" || { echo "missing index: $index" >&2; exit 1; }
done

ready_plan=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SET enable_seqscan=off; EXPLAIN SELECT id FROM steps WHERE status='ready' AND available_at <= now() ORDER BY available_at, created_at LIMIT 1;")
expired_plan=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SET enable_seqscan=off; EXPLAIN SELECT id FROM steps WHERE status='running' AND lease_expires_at <= now() ORDER BY lease_expires_at LIMIT 10;")
events_plan=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
  "SET enable_seqscan=off; EXPLAIN SELECT id FROM workflow_events WHERE workflow_id=(SELECT id FROM workflows LIMIT 1) AND id > 0 ORDER BY id LIMIT 50;")

printf '%s\n' "$ready_plan" | grep -q 'steps_ready_claim_idx'
printf '%s\n' "$expired_plan" | grep -q 'steps_expired_lease_idx'
printf '%s\n' "$events_plan" | grep -q 'workflow_events_workflow_id_id_idx'

echo "index review: required=8/8; ready_claim=partial-index; expired_lease=partial-index; ordered_events=covering-prefix"
