# Interview discussion guide

## Two-minute system explanation

RelayFlow accepts a validated DAG, persists the workflow and audit events atomically, and uses PostgreSQL as both durable state and the worker coordination boundary. Workers claim ready rows with `FOR UPDATE SKIP LOCKED`, create an attempt and time-bounded lease, renew ownership while executing, and commit only with the current unexpired lease token. Completion releases children only when every dependency succeeded. A killed worker stops heartbeating; another worker expires the attempt and redelivers the step. This is at-least-once execution, with stable operation keys for downstream deduplication.

## Questions worth preparing

### Why PostgreSQL instead of Kafka or Redis?

The core problem is transactional ownership plus workflow state, not message fan-out. PostgreSQL keeps claims, attempts, state transitions, dependencies, and events in one consistency boundary. A broker becomes justified when measured dispatch pressure exceeds that design, and then requires an outbox rather than replacing durable state.

### Does `SKIP LOCKED` provide exactly-once execution?

No. It prevents two committed claims of the same ready row at one instant. A worker can perform an external effect and die before recording success, so a lease expiry legitimately causes another attempt. Exactly-once external effects require the target system to cooperate with the stable operation key.

### Why both worker ID and lease token?

Worker ID is diagnostic identity. The random token identifies one ownership epoch. A restarted worker with the same name must not be able to complete work owned by a newer epoch.

### What race was most subtle?

Parallel sibling completions can each observe the other as unfinished and both fail to release their shared child. Locking the workflow row serializes that aggregate scheduling decision while leaving handler execution parallel.

### What happens on graceful shutdown versus crash?

Graceful termination stops new claims, continues heartbeats, and drains active handlers up to a deadline. `SIGKILL` skips cleanup; the lease expires and another worker creates a new attempt. Both paths have executable demonstrations.

### How are retries controlled?

Only typed transient failures and timeouts retry. Attempts are bounded, delays grow exponentially from a validated base, and terminal failure atomically fails the workflow and cancels irrelevant work.

### Why derive metrics from PostgreSQL?

Durable metrics reconcile after restarts and can be checked against audit history. The cost is scrape-time work over retained history; at scale, incremental aggregates and retention replace full reconstruction.

### What is the immediate measured bottleneck?

One execution lane per worker limited backlog drain. Bounded lanes improved repeated throughput by 3.04× and 3.26×, while per-workflow p95 rose because database concurrency increased. That tradeoff is why concurrency is explicit and bounded.

### What would you change first for production?

Add authn/authz, tenant quotas, TLS and secret management; establish PostgreSQL backups/failover; add admission control and SLOs; then load-test representative handler durations and queue distributions before changing coordination architecture.

### What is intentionally absent?

Arbitrary code execution, multi-tenancy, cron, compensation, optional edges, multi-region consensus, and a separate broker. Each would materially expand the reliability or security model without being needed to prove the orchestration core.
