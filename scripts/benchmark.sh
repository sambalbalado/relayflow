#!/bin/sh
set -eu

workflows=${WORKFLOWS:-1000}
clients=${CLIENTS:-64}
baseline_concurrency=${BASELINE_CONCURRENCY:-1}
optimized_concurrency=${OPTIMIZED_CONCURRENCY:-8}
run_id="$(date +%s)-$$"

docker compose --profile tools build api worker-a worker-b loadgen >/dev/null
docker compose up -d db api >/dev/null

run_case() {
  label=$1
  concurrency=$2
  prefix="benchmark-$run_id-$label"
  docker compose stop worker-a worker-b >/dev/null
  submission=$(docker compose --profile tools run --rm --no-deps loadgen \
    -workflows "$workflows" -concurrency "$clients" -wait=false -timeout=120s -prefix "$prefix")
  submitted=$(printf '%s' "$submission" | sed -n 's/.*"submitted":\([0-9]*\).*/\1/p')
  [ "$submitted" -eq "$workflows" ] || { echo "$label submitted $submitted of $workflows" >&2; exit 1; }
  peak_ready=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
    "SELECT count(*) FROM steps s JOIN workflows w ON w.id=s.workflow_id WHERE w.idempotency_key LIKE '$prefix-%' AND s.status='ready';")
  WORKER_CONCURRENCY="$concurrency" docker compose up -d --force-recreate worker-a worker-b >/dev/null
  attempts=0
  completed=0
  while [ "$attempts" -lt 1200 ]; do
    completed=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc \
      "SELECT count(*) FROM workflows WHERE idempotency_key LIKE '$prefix-%' AND status='succeeded';")
    [ "$completed" -eq "$workflows" ] && break
    attempts=$((attempts + 1))
    sleep 0.05
  done
  [ "$completed" -eq "$workflows" ] || { echo "$label completed $completed of $workflows" >&2; exit 1; }
  measurement=$(docker compose exec -T db psql -U relayflow -d relayflow -Atc "
    WITH target AS (
      SELECT id, completed_at FROM workflows WHERE idempotency_key LIKE '$prefix-%'
    ), per_workflow AS (
      SELECT t.id, t.completed_at, min(a.started_at) AS started_at
      FROM target t JOIN steps s ON s.workflow_id=t.id JOIN attempts a ON a.step_id=s.id
      GROUP BY t.id, t.completed_at
    ), aggregate AS (
      SELECT extract(epoch FROM (max(completed_at)-min(started_at))) AS seconds,
             percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (completed_at-started_at))) * 1000 AS p95_ms
      FROM per_workflow
    )
    SELECT round(seconds::numeric,3) || '|' || round(($workflows/seconds)::numeric,1) || '|' || round(p95_ms::numeric,1) FROM aggregate;")
  drain_seconds=$(printf '%s' "$measurement" | cut -d'|' -f1)
  throughput=$(printf '%s' "$measurement" | cut -d'|' -f2)
  p95_ms=$(printf '%s' "$measurement" | cut -d'|' -f3)
  echo "benchmark: case=$label; worker_concurrency=$concurrency; workflows=$workflows; clients=$clients; peak_ready=$peak_ready; drain_seconds=$drain_seconds; throughput_rps=$throughput; workflow_p95_ms=$p95_ms"
}

run_case baseline "$baseline_concurrency"
run_case optimized "$optimized_concurrency"
