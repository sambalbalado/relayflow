# Architecture decision log

## ADR-001: PostgreSQL is the state store and work queue

- **Status:** accepted
- **Decision:** use PostgreSQL transactions, row locks, constraints, JSONB, partial indexes, and leases for durable state and worker coordination.
- **Rationale:** one inspectable consistency boundary makes ownership and state transitions easier to defend than a database-plus-broker design.
- **Tradeoff:** queue throughput and availability are coupled to the primary database and must be measured.

## ADR-002: At-least-once execution with cooperative idempotency

- **Status:** accepted
- **Decision:** lease tokens fence database commits; stable operation keys let downstream operations deduplicate repeated attempts.
- **Rationale:** RelayFlow cannot atomically commit its PostgreSQL state and an arbitrary external side effect.
- **Tradeoff:** operation authors must honor idempotency keys or otherwise tolerate repeats.

## ADR-003: Separate API and worker processes in one Go module

- **Status:** accepted
- **Decision:** build separate binaries that share typed domain and store packages.
- **Rationale:** process crashes and horizontal worker concurrency remain realistic without premature service boundaries.
- **Tradeoff:** the binaries share a release cadence and schema.

## ADR-004: Allowlisted handlers instead of arbitrary code execution

- **Status:** accepted
- **Decision:** execute registered handlers; the current set is `echo`, `noop`, bounded `sleep`, and deterministic failure handlers used for verification.
- **Rationale:** untrusted code would require sandboxing, resource isolation, and a substantially larger security model.
- **Tradeoff:** RelayFlow demonstrates an orchestration core rather than a general function platform.

## ADR-005: Lease expiry is an ownership boundary

- **Status:** accepted
- **Decision:** heartbeats cannot revive an expired lease, and completion requires an unexpired matching token.
- **Rationale:** allowing late renewal creates ambiguous dual ownership during pauses or partitions.
- **Tradeoff:** a slow or paused healthy worker can lose ownership and must tolerate its result being rejected.

## ADR-006: Poll-based reclamation

- **Status:** accepted
- **Decision:** one dedicated loop per worker process scans bounded expired-lease batches independently of execution lanes.
- **Rationale:** it avoids a separate scheduler service and avoids multiplying reclamation queries when process concurrency increases.
- **Tradeoff:** recovery latency includes lease duration plus up to a poll interval, and heartbeat/reclamation writes add database load.

## ADR-007: Validate the complete DAG before persistence

- **Status:** accepted
- **Decision:** reject unknown, duplicate, and self-dependencies and cycles before persisting any workflow rows.
- **Rationale:** a complete preflight check prevents permanently blocked definitions and lets submission remain atomic.
- **Tradeoff:** validation is in-memory and definitions are deliberately limited to 100 steps.

## ADR-008: Serialize aggregate scheduling decisions

- **Status:** accepted
- **Decision:** successful and failed step completions lock the parent workflow row before dependency release or terminal propagation.
- **Rationale:** sibling completions must not create write skew by each observing the other before commit.
- **Tradeoff:** completions within one workflow serialize, limiting the maximum completion throughput of a single extremely wide workflow.

## ADR-009: Typed failure policy with fail-fast propagation

- **Status:** accepted
- **Decision:** retry only transient failures and timeouts within the configured attempt budget; permanent or exhausted failures fail the aggregate and cancel all remaining work.
- **Rationale:** retrying permanent failures wastes capacity, while fail-fast propagation makes downstream semantics deterministic.
- **Tradeoff:** RelayFlow does not currently support optional dependencies, compensation, or continue-on-error branches.

## ADR-010: Cancellation is durable but cooperative at the handler boundary

- **Status:** accepted
- **Decision:** cancellation atomically marks non-terminal steps and running attempts cancelled; workers observe lease loss and cancel the handler context.
- **Rationale:** no cancelled step can be newly claimed, and cooperative handlers stop promptly without asynchronous thread termination.
- **Tradeoff:** a non-cooperative external operation may continue outside RelayFlow even though its result is fenced from persistence.

## ADR-011: Reliability metrics are derived from durable state

- **Status:** accepted
- **Decision:** expose Prometheus gauges, counters, and execution histograms by querying PostgreSQL state, attempts, and append-only events.
- **Rationale:** metrics survive process restarts and can be reconciled with the audit trail instead of diverging across worker-local counters.
- **Tradeoff:** scrape cost grows with retained attempt history; production scale would use incremental aggregation or bounded retention.

## ADR-012: Bounded execution lanes within each worker

- **Status:** accepted
- **Decision:** make per-process step concurrency configurable from 1 to 64, with four lanes per Compose worker by default.
- **Rationale:** a controlled 1,000-workflow backlog showed one lane per process was the dominant throughput limit; eight lanes raised drain throughput by roughly three times in repeated runs.
- **Tradeoff:** higher concurrency increases database contention and individual workflow latency, so it remains an explicit capacity setting rather than an unbounded goroutine per step.

## ADR-013: Graceful drain before lease-based recovery

- **Status:** accepted
- **Decision:** on normal termination, stop claims immediately and allow active handlers to heartbeat and finish for a bounded `SHUTDOWN_TIMEOUT`; force-cancel only after the deadline.
- **Rationale:** routine deployments should not manufacture retries, while abrupt death must still use the exact same lease-recovery path.
- **Tradeoff:** process termination can take up to the drain timeout, and non-cooperative external calls still cannot be forcibly undone.

## ADR-014: Validate at the trust boundary and again at execution

- **Status:** accepted
- **Decision:** the API rejects malformed media types, documents, identifiers, policies, DAGs, and handler payloads before persistence; workers retain defensive decoding of durable input.
- **Rationale:** early rejection keeps invalid work out of the queue, while execution-side checks protect against legacy or manually corrupted rows.
- **Tradeoff:** handler schemas currently live in Go rather than a separately versioned schema registry.
