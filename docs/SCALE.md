# 100× scale discussion

The local optimized measurement is about 3,000 one-step workflows per second. “100×” is therefore a design exercise around a workload near 300,000 steps per second, not a claim that the current implementation can reach it.

## Measure before redesign

Capture claim/completion transaction latency, lock waits, database CPU, WAL bytes, connection saturation, heartbeat writes, queue age, workflow width, event growth, and scrape cost. Separate handler time from coordination time. Preserve the existing correctness suite as the semantic oracle while changing the architecture.

## Likely staged changes

1. **Reduce database round trips:** batch claims and completions where semantics allow, prepare statements, and use explicit connection budgets.
2. **Bound write amplification:** adapt heartbeat intervals to lease length, avoid heartbeats for very short handlers, and aggregate metrics incrementally.
3. **Partition working sets:** partition ready work and retained events, then route workers consistently while keeping a workflow's completion decisions on one authority.
4. **Separate read load:** move history/status reads to replicas with an explicit staleness contract; keep ownership decisions on the primary.
5. **Archive history:** time-partition events/attempts and export cold audit data so queue indexes stay small.
6. **Introduce a broker only with evidence:** a broker can absorb dispatch fan-out, but PostgreSQL must still own workflow state and idempotent transitions. Consumers remain at-least-once, and an outbox bridges committed readiness to publication.
7. **Shard deliberately:** shard by workflow ID so dependency and aggregate transitions remain local. Cross-shard DAG edges would need prohibition or a new coordination protocol.

## What does not change

- Attempts remain durable and at-least-once.
- Ownership remains time-bounded and fenced.
- Downstream effects still require idempotency.
- Dependency release and terminal propagation remain atomic within a workflow.
- Backpressure and admission control become mandatory rather than optional.

Multi-region active/active scheduling is a different consistency design, not a configuration toggle. A defensible first production step would be one highly available regional PostgreSQL authority, horizontally scaled stateless APIs/workers, controlled admission, and measured failover behavior.
