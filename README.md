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
      measured on a 2-vCPU sandbox VM — re-run locally for numbers that
      reflect your own hardware before citing them anywhere.

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