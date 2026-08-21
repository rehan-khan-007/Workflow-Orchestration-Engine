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
- [ ] Phase 2 — Real Redis-backed parallel scheduling
- [ ] Phase 3 — Fault tolerance (leases, heartbeats, retries)
- [ ] Phase 4 — REST API + CLI
- [ ] Phase 5 — Docker + Kubernetes worker deployment
- [ ] Phase 6 — Benchmarks