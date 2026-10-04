# RelayFlow demonstration guide

## Standard execution

Start the local system:

```sh
docker compose up --build -d
docker compose ps
```

Run a durable one-step workflow:

```sh
./scripts/demo.sh
```

Point out the workflow and step terminal states, `attempt_count: 1`, stable `operation_key`, and echoed output. Then correlate execution identifiers in the logs:

```sh
docker compose logs --no-color worker-a worker-b
```

## Workflow semantics

Run:

```sh
make semantics-test
```

This executable demonstration proves:

1. A cyclic graph is rejected with HTTP `422`.
2. A root releases two independent 500 ms branches; separate workers start them within the asserted concurrency window.
3. A join becomes ready only after both branches succeed.
4. A transient handler fails twice, waits for 100 ms then 200 ms backoff, and succeeds on attempt 3.
5. A 100 ms timeout produces two `timed_out` attempts and then fails the workflow.
6. Cancellation persists `cancelled` on the workflow, running step, and active attempt.
7. A permanent root failure cancels its blocked dependent.
8. The HTTP event endpoint returns a bounded ordered page.

The summary includes the measured sibling start gap and every final state, for example:

```text
semantics: cycle=rejected; branch=join:succeeded,left:succeeded,right:succeeded,root:succeeded; parallel_start_gap=1ms; retry=failed,failed,succeeded; backoff_ms=100,200; timeout=timed_out,timed_out; cancellation=cancelled|cancelled|cancelled; failure=dependent:cancelled,root:failed; event_page=2
```

## Crash recovery

Run:

```sh
./scripts/crash-recovery-demo.sh
```

The script performs an actual process failure:

1. Start two workers and stop `worker-b`.
2. Submit a four-second step and wait for `worker-a` to claim it.
3. Kill `worker-a` with `SIGKILL`, preventing cleanup or a final heartbeat.
4. Start `worker-b` and wait for the two-second lease to expire.
5. Verify `worker-b` records the first attempt as `lease_expired`, claims attempt 2, and completes it.
6. Assert exactly one lease-expiry event and restore both workers.

Expected summary:

```text
crash recovery: workflow=succeeded; attempts=lease_expired:worker-a,succeeded:worker-b; lease_expiry_events=1
```

## Graceful shutdown

Run:

```sh
make graceful-shutdown-test
```

The script lets `worker-a` claim a two-second step, sends `SIGTERM`, and verifies that the worker stops taking work but continues heartbeats until attempt 1 succeeds. Contrast this with `SIGKILL`: graceful termination preserves the attempt; abrupt death produces expiry and redelivery.

Expected summary:

```text
graceful shutdown: workflow=succeeded; attempts=succeeded:worker-a; drain=completed-before-exit
```

## Heartbeat behavior

Run:

```sh
./scripts/integration-test.sh
```

The check submits a three-second step under a two-second lease. A healthy worker renews ownership every 500 milliseconds, so the workflow finishes in one attempt with no lease-expiry event.

## Concurrency behavior

Run:

```sh
make test-store-integration
```

The PostgreSQL suite races 16 claimers for one ready step and eight reclaimers for one expired lease. It requires exactly one claim winner and exactly one successful reclamation.

## Observability and backlog behavior

Run:

```sh
make observability-test
```

The check stops both workers, creates a ready backlog, verifies the `relayflow_steps{status="ready"}` gauge rises, restarts workers, and verifies the backlog drains. It also checks retry and failure counter deltas, durable claim reconciliation, readiness detail, execution histograms, and correlated API/worker logs.

## Load and optimization evidence

Run:

```sh
make load
make benchmark
```

The load command reports submission and end-to-end throughput, submission p50/p95/p99 latency, failure count, and peak ready queue depth. The controlled benchmark preloads 1,000 workflows with workers stopped, then compares the same backlog with one and eight execution lanes per worker. See [PERFORMANCE.md](PERFORMANCE.md) for measured results and interpretation.

## Interview explanation

`SKIP LOCKED` prevents workers from waiting on the same candidate and the atomic update establishes one committed owner. The lease token is a fencing value: after expiry and reclamation, the previous token cannot commit. This protects internal state but cannot guarantee exactly-once external effects, so the stable operation key must be honored by downstream operations.

For DAG scheduling, emphasize the workflow-row lock around completion. A test initially exposed write skew when two sibling transactions each saw the other as unfinished and failed to release their join. Serializing that aggregate decision keeps independent execution parallel while making dependency release correct.

## Clean-room acceptance

Run:

```sh
make clean-room-test
```

This exports repository files to an isolated temporary checkout, validates Compose, starts the stack on alternate ports, runs unit tests in Docker, and executes the same demo a new clone would use. It requires no host Go installation or paid service.
