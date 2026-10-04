# Performance analysis

## Method

`make benchmark` builds the same API, worker, and load-generator images for both cases. It stops the workers, submits 1,000 one-step `noop` workflows with 64 clients, confirms all 1,000 steps are durably `ready`, then starts two workers and measures backlog drain from the first attempt start to the last workflow completion.

The only changed setting is `WORKER_CONCURRENCY`: one execution lane per worker for the baseline and eight lanes per worker for the optimized case. Results are local measurements, not capacity claims for other machines or production databases.

## Results

| Run | Lanes per worker | Backlog | Drain time | Throughput | Workflow p95 |
|---|---:|---:|---:|---:|---:|
| 1 baseline | 1 | 1,000 | 1.005 s | 995.2 workflows/s | 1.3 ms |
| 1 optimized | 8 | 1,000 | 0.330 s | 3,028.9 workflows/s | 3.5 ms |
| 2 baseline | 1 | 1,000 | 1.039 s | 962.5 workflows/s | 1.4 ms |
| 2 optimized | 8 | 1,000 | 0.319 s | 3,137.9 workflows/s | 3.7 ms |

The repeated optimized runs delivered 3.04× and 3.26× baseline throughput. Individual workflow p95 rose because more transactions competed concurrently, but total backlog drain time fell by about 68–69%.

## Bottleneck and optimization

The baseline kept only two steps in flight because each worker process executed one step at a time. PostgreSQL claim and completion transactions were short, yet the queue remained fully backlogged, identifying execution-lane serialization rather than submission throughput as the immediate limit.

The optimization added bounded per-process execution lanes, immediate re-claim after successful work, and a dedicated reclamation loop so concurrency does not multiply expired-lease scans. The limit is explicit and validated between 1 and 64; Compose defaults to four.

The result illustrates a real tradeoff: greater aggregate throughput at the cost of higher per-workflow latency and database concurrency. Increasing the setting indefinitely is not a valid scaling plan.

## Load smoke result

A separate `make load` run submitted and completed 200 workflows with 32 clients and zero failures:

- submission throughput: 1,940.2 workflows/s;
- end-to-end throughput: 1,157.3 workflows/s;
- submission latency: p50 15.1 ms, p95 30.0 ms, p99 32.6 ms;
- peak ready queue: 142 steps.

## Next bottlenecks to measure

- database connection saturation and transaction latency as execution lanes increase;
- `steps_ready_claim_idx` scan and lock behavior under larger queues;
- workflow-row completion serialization for very wide DAGs;
- heartbeat write volume for long-running work;
- scrape cost from reconstructing duration histograms over retained attempt history;
- audit-event and attempt retention, archival, and read amplification.
