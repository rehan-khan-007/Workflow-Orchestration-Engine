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
- [ ] Phase 4 — REST API + CLI
- [ ] Phase 5 — Docker + Kubernetes worker deployment
- [ ] Phase 6 — Benchmarks