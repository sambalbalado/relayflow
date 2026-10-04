# Database index review

## Hot-path mapping

| Index | Query or invariant served |
|---|---|
| `workflows_idempotency_key_key` | idempotent submission conflict/lookup |
| `steps_ready_claim_idx` | partial ordered scan for claimable `ready` work |
| `steps_expired_lease_idx` | partial ordered scan for reclaimable `running` work |
| `steps_workflow_status_idx` | aggregate completion, cancellation, and status lookup |
| `step_dependencies_pkey` | dependency membership by child |
| `step_dependencies_depends_on_idx` | reverse edge traversal by completed parent |
| `attempts_step_id_attempt_no_idx` | ordered attempt history by step |
| `attempts_finished_at_idx` | completed-attempt duration metrics |
| `workflow_events_workflow_id_id_idx` | ordered cursor pagination for one workflow |
| `workflow_events_event_type_idx` | durable reliability-counter reconciliation |

The claim and expiry indexes are partial, keeping terminal rows out of the coordination working set. The event index begins with `workflow_id` and then monotonic `id`, matching `WHERE workflow_id = ? AND id > ? ORDER BY id LIMIT ?`.

## Executable review

`make index-review` checks the expected non-constraint indexes and runs planner probes for ready claims, expired leases, and ordered event history. It disables sequential scans only for these probes because the small local dataset can legitimately prefer a sequential scan; the goal is to prove that each intended access path is usable, not to falsify a production cost estimate.

No additional index was added. Workflow and step status metrics scan low-cardinality whole-table aggregates, while event and duration metrics scan retained history by design. Extra low-selectivity indexes would add write amplification without fixing that architectural cost. At larger retention, incremental metric tables and event partitioning/archival are better responses.
