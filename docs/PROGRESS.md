# Capability status

## Implemented and verified

| Area | Current capability | Verification entry point |
|---|---|---|
| Persistence | Workflows, steps, attempts, leases, operation keys, dependencies, and append-only events | `make test-integration` |
| API | Strict workflow submission, status queries, idempotent replay, mismatch conflict, health, and readiness | `go test ./internal/api` plus integration suite |
| Coordination | Atomic `SKIP LOCKED` claims across workers | `make test-store-integration` |
| Leases | Configurable expiry and heartbeat renewal | Store concurrency suite and long-running end-to-end check |
| Recovery | Concurrent-safe expired-lease reclamation and attempt rollover | `make test-store-integration` |
| Crash tolerance | Forced worker death followed by another worker completing attempt 2 | `make crash-demo` |
| DAG semantics | Cycle rejection, dependency release, joins, and parallel branches | `make semantics-test` |
| Retries | Failure classification, attempt budgets, and exponential backoff | `make semantics-test` |
| Deadlines | Per-step timeouts persisted as timed-out attempts | `make semantics-test` |
| Cancellation | Durable, idempotent cancellation of pending and running work | Store integration and semantics suites |
| Failure propagation | Permanent or exhausted failures cancel remaining work | `make semantics-test` |
| Auditability | Immutable and paginated event history for all lifecycle decisions | Integration and semantics suites |
| Observability | Prometheus queue/state/counter/histogram metrics and correlated JSON logs | `make observability-test` |
| Load testing | Concurrent Dockerized workload generator with latency percentiles and queue sampling | `make load` |
| Performance | Reproducible backlog-drain benchmark and measured concurrency optimization | `make benchmark` |
| Developer experience | Dockerized API, two workers, PostgreSQL, tests, and demos | `docker compose up --build -d` |
| Input/security hardening | Media type, size, JSON, UUID, identifier, policy, DAG, and handler-schema validation | API/domain unit tests and `docs/SECURITY.md` |
| Graceful shutdown | Stop claims and drain owned work with continued heartbeats and a bounded deadline | `make graceful-shutdown-test` |
| Database indexes | Hot-path index inventory plus executable planner checks | `make index-review` |
| Continuous integration | Formatting, vet, unit/race, PostgreSQL integration, Compose, and image builds | `.github/workflows/ci.yml` |
| Clean-room setup | Isolated exported checkout starts, tests, and completes a workflow | `make clean-room-test` |

## Current boundary

- At most 100 steps per workflow
- Built-in `echo`, `noop`, bounded `sleep`, and deterministic verification handlers
- At-least-once execution
- One PostgreSQL coordination authority
- No authentication or tenant isolation

## Core completion

The documented core scope is complete. Future work belongs to the limitations and scale path rather than the local portfolio acceptance boundary.

## Known limitations

- PostgreSQL is a deliberate single coordination point; database high availability is outside the local runtime.
- A lease fences RelayFlow state commits but cannot guarantee exactly-once side effects in another system.
- Reclamation is poll-based and may add up to one poll interval after lease expiry.
- Worker concurrency is bounded and configurable; excessive lanes can increase PostgreSQL contention.
- Completion decisions serialize per workflow to prevent dependency-release write skew.
- Failure handling is fail-fast; optional edges, compensation, and continue-on-error policies are not implemented.
- Handler cancellation is cooperative and cannot revoke an external side effect already in progress.
- Authentication, authorization, multi-tenancy, arbitrary-code sandboxing, cron triggers, and multi-region coordination are excluded.
- Metrics scrape cost grows with retained attempt history because duration buckets are derived from durable attempts.
