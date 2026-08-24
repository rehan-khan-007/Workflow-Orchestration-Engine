# Workflow Orchestration Engine

Distributed workflow orchestration engine with DAG-based dependency resolution, Redis-backed queues, and Kubernetes workers.

Built with TypeScript, Node.js, Redis, PostgreSQL, Docker, and Kubernetes.

## Architecture

```
src/
├── core/          — DAG resolution, workflow state machine
├── queue/         — Redis producer/consumer, step/result queues
├── worker/        — K8s worker pool, leases, heartbeats
├── storage/       — PostgreSQL state persistence
├── scheduler/     — Workflow scheduling & dispatch
├── api/           — REST API for workflow submission & status
└── benchmarks/    — Throughput, latency, recovery benchmarks
```

## Setup

```bash
# Start Redis + Postgres (see docker-compose.yml)
docker compose up -d

# Apply the schema
npm run migrate

# Run tests (requires Postgres running, see .env.example)
npm test
```

## Status

🚧 Active development.

- [x] Phase 1 — PostgreSQL persistence (`src/storage/`): workflows, steps, and
      idempotent step-execution tracking. Engine state is now durable, not in-memory.
- [x] Phase 2 — Real Redis-backed parallel scheduling (`src/core/coordinator.ts`,
      `src/worker/pool.ts`): the scheduler enqueues all currently-runnable steps
      at once; a pool of concurrent Redis consumers executes them in parallel;
      completions trigger re-checking the DAG for newly-unblocked steps.
- [x] Phase 3 — Fault tolerance (`src/worker/leaseManager.ts`, `src/worker/reaper.ts`):
      workers hold a Redis lease (with TTL) while executing a step and heartbeat
      it periodically; a reaper detects steps whose lease expired without
      completion (a crashed worker) and retries them, up to a max-attempts
      limit, after which the step and workflow are permanently failed.
- [x] Phase 4 — REST API + CLI (`src/api/`, `scripts/cli.ts`): the API and
      worker are separate processes talking only through Redis/Postgres —
      run `npm run start:api` and `npm run start:worker` independently.
      Endpoints: `POST /workflows`, `GET /workflows`, `GET /workflows/:id`,
      `POST /workflows/:id/cancel`, and `GET /workflows/:id/stream` (SSE,
      fed by Redis Pub/Sub for live step/workflow status updates). CLI:
      `npm run cli -- <create|list|get|cancel|watch> [arg]`.
- [x] Phase 5 — Docker + Kubernetes (`docker/Dockerfile`, `k8s/`): a
      multi-stage Dockerfile builds separate `api` and `worker` images from
      one build. `docker-compose.yml` runs the whole stack (Postgres,
      Redis, migration job, API, 2 worker replicas) locally. `k8s/`
      contains manifests for a local cluster (Minikube/Kind) — the worker
      Deployment's replica count is the literal "N Kubernetes workers"
      from the resume; `kubectl scale deployment woe-worker --replicas=N`
      changes concurrency with no code change.
- [x] Phase 6 — Benchmarks (`src/benchmarks/`): `npm run benchmark` runs
      three real measurements against a live Postgres+Redis — throughput/
      latency/utilization across a batch of synthetic DAG workflows,
      parallel-vs-sequential speedup on the same workload, and recovery
      rate across many injected worker crashes. Results below were
      measured locally — re-run with `npm run benchmark` for numbers
      that reflect your own hardware before citing them anywhere.
- [x] DAG validation (`src/core/validation.ts`): workflow submissions are
      checked at creation time — duplicate step ids, dependencies on
      nonexistent steps, self-dependencies, and dependency cycles are all
      rejected with a 400 and a specific error message, instead of being
      accepted and failing (or hanging) later during dispatch.
- 75 automated tests across 9 files (`npm test`): unit tests for DAG
      dispatch logic, lease acquisition/expiry/atomic renewal, the
      crash-detection reaper, and DAG validation (all fast, no DB/Redis
      needed except where the component itself is Redis-backed);
      integration tests exercising the real engine end-to-end through
      Postgres, Redis, and real HTTP.
- [x] Phase 7 — Correctness hardening: an explicit `queued` state
      (dispatched-but-not-yet-picked-up) distinct from `running`
      (a worker is actively executing it) — `src/worker/pool.ts` marks
      the transition the instant a worker actually acquires the lease.
      Lease renewal (`src/worker/leaseManager.ts`) is now atomic and
      owner-checked (a Lua script, not a bare `PEXPIRE`) — closes a real
      race where a worker whose lease had already expired and been
      reclaimed elsewhere could otherwise extend a lease it no longer
      owned. Plus cancellation edge-case tests (cancelling a
      not-yet-started or already-cancelled workflow) and a duplicate-
      dispatch regression test.
- [x] Phase 8 — Reliability: crash-triggered retries (the reaper's path)
      now back off exponentially (`src/core/coordinator.ts`) — a new
      `retrying` status distinguishes "waiting out a backoff delay" from
      "queued and ready to run." Scoped deliberately to crash recovery
      only, not task-level failures (a crash is transient infra trouble
      worth retrying; a task's own thrown error might mean a side effect
      already happened, so it fails permanently instead). Steps can
      specify `timeoutMs`; a step whose executor doesn't resolve in time
      is treated as failed (`src/worker/runner.ts`'s `withTimeout` — a
      documented best-effort limitation: Node can't truly cancel an
      in-flight Promise). Permanently-failed steps are now recorded to a
      `dead_letters` table, queryable via `GET /dead-letters`, whether
      they failed via exhausted retries or an immediate task error.
      Restart recovery — a crashed step being recoverable by entirely
      fresh process instances with no shared in-memory state — is now an
      explicit, passing integration test rather than an unverified claim.
- [x] Phase 9a — Prometheus metrics (`src/observability/metrics.ts`):
      counters for workflow/step lifecycle events, histograms for
      workflow/step duration, and gauges for queue depth and workflows
      currently running (the gauges are computed live from Postgres on
      every scrape, not tracked incrementally in-process, so they can't
      drift from reality across multiple replicas). See "Observability"
      below for the two-endpoint scrape model this requires.
- [x] Phase 9b — Structured JSON logging (`src/observability/logger.ts`):
      one JSON object per line to stdout — the format every container
      log aggregator (CloudWatch, Loki, Datadog, `kubectl logs | jq`)
      already expects. Traces the workflow → step → attempt → worker →
      result chain via events like `workflow_started`, `step_dispatched`,
      `step_execution_started`/`_finished` (with `workerId` and
      `durationMs`), `step_completed`/`_failed`, `dead_letter_recorded`,
      `worker_crash_detected`, `workflow_completed`/`_failed`/
      `_cancelled` — all carrying the same `workflowId`, so a real
      incident can be traced with a single `grep` across both the API's
      and every worker's logs. Suppressed under `NODE_ENV=test` to keep
      the test suite's own output clean; verified with a real end-to-end
      trace test (temporarily lifting the suppression) and a manual run
      of the actual API + worker processes, not just asserted in
      isolation.
- [x] Phase 10 — Worker-scaling benchmark (`src/benchmarks/scalingExperiment.ts`,
      `npm run benchmark:scaling`): the same fixed workload run at 1, 2,
      4, and 8 workers, isolating the effect of worker count on
      throughput and latency (p50/p95/p99, not just an average) from
      everything else. See "Worker-scaling experiment" under Benchmark
      Results below for the actual measured numbers — near-linear up to
      8 workers on this hardware.
- [x] Phase 11 — API key authentication + rate limiting
      (`src/api/auth.ts`): every route except `GET /metrics` requires a
      matching `Authorization: Bearer <API_KEY>` header — but only if
      `API_KEY` is actually set. Left unset (the default), auth is fully
      disabled, so local dev/demo usage is unchanged. Rate limiting
      (`express-rate-limit`) caps requests per IP (100/minute by
      default), also exempting `/metrics` since Prometheus scrapes it
      frequently from trusted infra. Verified against the real running
      API process, not just in tests.

## Authentication

Disabled by default — set `API_KEY` (on the API process) to require it:
```bash
API_KEY=your-secret-here npm run start:api
curl http://localhost:3000/workflows -H "Authorization: Bearer your-secret-here"
```
`GET /metrics` is always exempt (Prometheus scrapers don't send app-level
auth headers by default). Rate limiting is always on regardless of
`API_KEY` (100 requests/minute per IP by default).

## Observability

Metrics are split across **two** endpoints, deliberately — this is standard
practice for a multi-process/multi-replica deployment, not a workaround:

- **API process** — `GET :3000/metrics` (or whatever `PORT` is set to).
  Reliably shows `workflow_started_total` for every workflow. Does **not**
  reliably show most `workflow_completed_total`/`workflow_failed_total`/
  `workflow_duration_seconds` — those get incremented wherever
  `handleStepResult()` actually executes, which for any workflow that runs
  real steps is the *worker* process, not the API process.
- **Worker process** — `GET :9100/metrics` (`WORKER_METRICS_PORT`, one
  per replica). Shows step-level counters (`step_dispatched_total`,
  `step_completed_total`, `step_failed_total`, `step_duration_seconds`,
  `recovery_attempt_total`) and, for the normal case, the workflow-level
  completion counters and duration too.

A real Prometheus deployment should scrape **both** targets (every worker
replica individually, plus the API), then aggregate with PromQL
(`sum by (job) (...)`) rather than expecting either single endpoint to
hold the complete picture — this is exactly how Prometheus is meant to be
used against any multi-process system, not specific to this project.

## Benchmark Results

Measured 2026-08-21 (see `src/benchmarks/results.json` for raw output). Task
duration simulated as 100–300ms per step (representative lightweight work,
not artificially slow or fast) — these numbers measure orchestration
overhead, not any particular task's business logic.

| Metric | Result | Setup |
|---|---|---|
| Throughput | 1148 steps/minute | 120 workflows, 1032 total steps, 4 workers |
| Avg queue latency | 11.5s (p95: 17.2s) | Same run — workers were ~96% utilized, i.e. saturated by design (120 workflows dispatched at once against only 4 workers) |
| Worker utilization | 96.2% | — |
| Speedup vs. sequential | 4.75× (78.9% latency reduction) | 30 workflows, same task durations, 4 workers |
| Failure recovery | 100% within 10s (avg 4.27s, p95 4.56s) | 20 injected worker crashes (lease acquired, then abandoned — no heartbeat, no completion) |

Reproduce with:
```bash
DATABASE_URL=... REDIS_URL=... npm run benchmark
```

### Worker-scaling experiment

Measured 2026-08-24 (see `src/benchmarks/scaling-results.json` for raw
output). The exact same fixed workload (60 workflows, same 100–300ms task
duration distribution) run four times, varying only the worker count —
this isolates the effect of worker count from everything else, unlike a
single throughput number at one fixed worker count.

| Workers | Throughput/min | Speedup | p50 latency | p95 latency | p99 latency | Utilization |
|---|---|---|---|---|---|---|
| 1 | 290 | 1.0× | 25.8s | 36.1s | 37.3s | 97.2% |
| 2 | 579 | 2.0× | 12.1s | 17.6s | 18.3s | 97.7% |
| 4 | 1150 | 3.97× | 5.5s | 8.7s | 8.9s | 96.5% |
| 8 | 2274 | 7.84× | 2.7s | 4.3s | 4.4s | 94.6% |

Near-linear: doubling worker count roughly doubles throughput and halves
latency, all the way to 8 workers, with utilization staying above 94%
throughout — the system isn't yet bottlenecked by Postgres/Redis at this
scale. Reproduce with:
```bash
DATABASE_URL=... REDIS_URL=... npm run benchmark:scaling
```

## Running with Docker Compose

```bash
docker compose up --build
curl -X POST http://localhost:3000/workflows \
  -H "Content-Type: application/json" \
  -d '{"name":"demo","steps":[{"id":"a","dependsOn":[],"status":"pending"}]}'
```

## Running on Kubernetes (Minikube)

```bash
minikube start
eval $(minikube docker-env)   # point the docker CLI at Minikube's own daemon
docker build --target api    -t woe-api:local    -f docker/Dockerfile .
docker build --target worker -t woe-worker:local -f docker/Dockerfile .
kubectl apply -f k8s/
kubectl get pods -n workflow-engine -w    # wait for everything Running
minikube service woe-api -n workflow-engine --url
# then curl <that url>/workflows same as above

kubectl scale deployment woe-worker -n workflow-engine --replicas=6   # more parallelism, no code change
```

## Engineering log

[ENGINEERING_LOG.md](./ENGINEERING_LOG.md) — real bugs found while
building this (and how they were actually diagnosed and fixed), plus
scale stats for the codebase.