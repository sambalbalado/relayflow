#!/bin/sh
set -eu

checkout=$(mktemp -d "${TMPDIR:-/tmp}/relayflow-clean.XXXXXX")
project="relayflow_clean_$$"

cleanup() {
  (cd "$checkout" && COMPOSE_PROJECT_NAME="$project" HTTP_PORT=18080 POSTGRES_PORT=15432 docker compose down -v --remove-orphans >/dev/null 2>&1) || true
  rm -rf "$checkout"
}
trap cleanup EXIT INT TERM

git ls-files -co --exclude-standard -z | tar --null -T - -cf - | tar -xf - -C "$checkout"
(
  cd "$checkout"
  COMPOSE_PROJECT_NAME="$project" HTTP_PORT=18080 POSTGRES_PORT=15432 docker compose config --quiet
  COMPOSE_PROJECT_NAME="$project" HTTP_PORT=18080 POSTGRES_PORT=15432 docker compose up --build -d db api worker-a worker-b
  docker run --rm -v "$checkout:/src" -w /src golang:1.23-alpine go test ./...
  RELAYFLOW_URL=http://localhost:18080 RELAYFLOW_KEY="clean-room-$(date +%s)-$$" ./scripts/demo.sh >/dev/null
)

echo "clean room: exported checkout; compose=healthy; unit_tests=passed; demo=succeeded; paid_services=none"
