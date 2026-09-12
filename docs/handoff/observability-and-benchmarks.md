# Topic: Observability (metrics, logging) and benchmarks

Covers: `src/observability/`, `src/benchmarks/`.

## Prometheus metrics (`src/observability/metrics.ts`)

Confirmed exact metric objects, by direct source inspection:

**Counters:**
`workflowStartedTotal`, `workflowCompletedTotal`, `workflowFailedTotal`,
`workflowCancelledTotal`, `stepDispatchedTotal`, `stepRetryTotal`,
`stepCompletedTotal`, `stepFailedTotal`, `deadLetterTotal`,
`recoveryAttemptTotal`.

**Histograms:**
`stepDurationSeconds`, `workflowDurationSeconds` (the latter computed
from `repo.getWorkflowCreatedAt()` — real elapsed wall-clock time, not
an estimate).

**Gauges — computed live, not tracked incrementally:**
`queueDepthGauge` (metric name `queue_depth`), `workflowsRunningGauge`
(metric name `workflows_running`). Confirmed directly in
`renderMetrics()`: both are `.set()` from a fresh Postgres query
**on every single scrape**, not incremented/decremented as events
happen. This is a deliberate design choice (commented in the source)
specifically because an in-process gauge would be wrong the instant
there's more than one worker replica — each replica's process memory
only knows about the steps it personally touched.

Plus `collectDefaultMetrics()` — standard Node.js process metrics
(event loop lag, memory, etc.) via `prom-client`.

## The two-endpoint scrape model — why it exists

`workflowCompletedTotal`, `stepCompletedTotal`, etc. are incremented
inside `coordinator.ts` and `pool.ts`, which run **in the worker
process**, not the API process. `GET /metrics` on the API only reads
whatever counters live in the API process's own memory — for a
non-trivial workflow, that's effectively just `workflow_started_total`
(incremented at `POST /workflows` time, which is API-side). Every other
counter requires scraping each **worker replica's own** `/metrics`,
served on `WORKER_METRICS_PORT` (default `9100`, set in
`src/worker/main.ts`). This is standard multi-target Prometheus
practice, not a bug — but it means "scrape the API's `/metrics`" alone
gives an incomplete picture.

## Structured logging (`src/observability/logger.ts`)

- `formatLogEntry(fields): string` — pure JSON formatting, deliberately
  separated from...
- `log(fields): void` — the actual side-effecting function.
  **Suppressed entirely when `process.env.NODE_ENV === "test"`**
  (checked fresh on every call, not cached at import time) — this
  exists purely to keep `npm test` output readable; Jest sets
  `NODE_ENV=test` automatically.
- Event names emitted across the codebase (grep-derived, from
  `coordinator.ts`, `pool.ts`, `reaper.ts`): `workflow_started`,
  `step_dispatched`, `step_retry_scheduled`, `step_execution_started`,
  `step_execution_finished`, `step_completed`, `step_failed`,
  `dead_letter_recorded`, `worker_crash_detected`, `workflow_completed`
  / `workflow_failed` / `workflow_cancelled`.
- Every step-level event carries `workflowId`, `stepId`; execution
  events additionally carry `workerId` and `durationMs` — this is what
  makes the workflow → step → attempt → worker → result chain
  reconstructable from logs alone, verified by
  `tests/integration/logging.test.ts`'s real end-to-end trace test
  (temporarily un-suppressing `NODE_ENV` for that one test to actually
  capture and assert on the real log lines a live run produces).

## Benchmarks (`src/benchmarks/`)

- `dagGenerator.ts` — generates layered DAGs with fan-out/fan-in for
  controlled-workload benchmarking.
- `throughputBenchmark.ts` — real measurement (not simulated), returns
  `stepsPerMinute`, `p50QueueLatencyMs`, `p95QueueLatencyMs`,
  `p99QueueLatencyMs`, `workerUtilizationPct` — percentiles computed
  by sorting real recorded queue latencies, not estimated.
- `scalingExperiment.ts` — runs `throughputBenchmark` at worker counts
  `[1, 2, 4, 8]` against the *same* fixed workload (60 workflows),
  isolating worker count as the only variable. Writes results to
  `src/benchmarks/scaling-results.json`.
- `speedupBenchmark.ts` — sequential vs. parallel comparison.
- `failureRecoveryBenchmark.ts` — injects crashes, measures recovery
  time.
- `report.ts` — the `npm run benchmark` entrypoint tying these
  together.

**Numbers reported in MASTER.md §4 are `NOT VERIFIED` by this
document** — they are the repo's own committed benchmark output
(`scaling-results.json`, and figures quoted in `README.md`), not
re-executed as part of producing this handoff doc. Re-running
`npm run benchmark` and `npm run benchmark:scaling` against a real
Postgres/Redis would take several minutes and was out of scope for
this pass; anyone who needs current numbers should re-run them
directly rather than trust a stale snapshot indefinitely.

## Known gap in this subsystem

No per-worker health/liveness gauge exists (e.g., `worker_active` /
`worker_idle`) — confirmed absent by direct inspection of
`metrics.ts`. `workflows_running` and `queue_depth` give system-level
signal, but there is no metric answering "is worker replica N alive
and doing work right now" beyond what `collectDefaultMetrics()`'s
generic process metrics imply.
