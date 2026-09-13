# Workflow Orchestration Engine — MASTER handoff

This document is self-contained: read only this file and leave with a
real, accurate understanding of the project. Topic files in
`docs/handoff/` go deeper on one subsystem each, for when you need
exact implementation detail.

## 1. Release status

- **Not frozen — actively evolving.** This document was originally
  written after 11 build phases; a 12th (a live browser demo, plus the
  two small backend additions it required — CORS and an auth
  query-param fallback) was added afterward and is reflected below.
  Treat "release status" as a snapshot at time of writing, not a
  permanent claim — check `git log` for what's actually landed since.
- Not CI/CD-gated — there is no `.github/workflows/` or other pipeline
  config in the repo. Tests are run manually (`npm test`).
- No version tag / release exists in the repo (no `.git` history was
  available to check this from — see §6). Treat this as an
  unversioned, single-branch (`main`) project.

## 2. Executive summary

A distributed workflow orchestration engine: submit a DAG of steps with
dependencies, and it schedules, executes, and retries them across a
pool of worker processes, backed by real Postgres persistence and a
real Redis queue — not an in-memory simulation. Concretely:

- A step only runs once every step it `dependsOn` has completed.
- Independent steps run **in parallel**, across multiple worker
  processes, via a real Redis-backed queue (`BRPOP`), not `Promise.all`
  in one process.
- A worker crash mid-step is detected (via an expired Redis lease) and
  the step is automatically retried on a different worker, with
  exponential backoff and a bounded attempt count before the step is
  recorded as a permanent failure ("dead letter").
- All of this is observable from outside the process: Prometheus
  metrics, structured JSON logs, a REST API, a CLI, and Server-Sent
  Events for live status.

It is a resume/portfolio project (see `README.md`'s own framing:
compared explicitly to "a typical student CRUD project," not to
Temporal/Cadence/Conductor as a production competitor) — see §8 for
what that means concretely was and wasn't built.

## 3. Architecture & topology

```
                     Client / CLI (scripts/cli.ts)
                              │
                              ▼
                    ┌──────────────────┐
                    │   REST API        │  src/api/server.ts
                    │  (Express)        │  Auth: src/api/auth.ts
                    │  Rate limiting    │  (express-rate-limit)
                    └────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │   PostgreSQL       │  src/storage/
                    │  workflows/steps/  │  (schema.sql,
                    │  step_executions/  │   workflowRepository.ts)
                    │  dead_letters      │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  DagCoordinator    │  src/core/coordinator.ts
                    │  (dispatch, retry, │
                    │   backoff, dead-   │
                    │   letter logic)    │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │   Redis Queue      │  src/queue/
                    │  (BRPOP per        │  (producer.ts,
                    │   worker)          │   consumer.ts)
                    └─────────┬─────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         Worker proc 1   Worker proc 2   Worker proc N   src/worker/
              │               │               │          pool.ts,
         Lease (Redis    Lease           Lease            leaseManager.ts,
         SET NX PX) +    + Heartbeat     + Heartbeat       reaper.ts,
         Heartbeat                                          runner.ts
              └───────────────┴───────────────┘
                              │
                    Postgres (execution results) +
                    Redis Pub/Sub (live status) → SSE → client
```

Two independent runtime processes, started separately:
- `npm run start:api` → `src/api/index.ts`
- `npm run start:worker` → `src/worker/main.ts`

They coordinate **only** through Postgres and Redis — no direct
process-to-process calls. This is why the two-endpoint metrics split
exists (§ observability topic file) and why restart recovery works
(the worker process can be killed and restarted independently of the
API process; all state survives in Postgres/Redis).

## 4. Data / benchmark summary

All numbers below are from `README.md`'s documented benchmark runs and
`src/benchmarks/scaling-results.json`, both present in this snapshot.
**`NOT VERIFIED` by this document's author** — no benchmark was
re-executed during this investigation; these are the numbers already
committed to the repo, taken at face value from the repo's own
records, not independently reproduced.

Worker-scaling experiment (`npm run benchmark:scaling`), fixed 60-workflow
workload, only worker count varied:

| Workers | Throughput/min | Speedup | p50 | p95 | p99 | Utilization |
|---|---|---|---|---|---|---|
| 1 | 290 | 1.0× | 25.8s | 36.1s | 37.3s | 97.2% |
| 2 | 579 | 2.0× | 12.1s | 17.6s | 18.3s | 97.7% |
| 4 | 1150 | 3.97× | 5.5s | 8.7s | 8.9s | 96.5% |
| 8 | 2274 | 7.84× | 2.7s | 4.3s | 4.4s | 94.6% |

Main throughput benchmark (`npm run benchmark`), per README: 4.75×
speedup and 78.9% latency reduction (4 workers vs. sequential), 100%
recovery from injected worker crashes within 10s (avg 4.27s).

These are single-run measurements, not averaged across repeated runs —
see §8.

## 5. Critical contracts & invariants

Each one traces to a real, specific reason found in code or the
engineering log — not generic advice.

1. **A step's status only becomes `running` after its worker actually
   acquires the Redis lease** (`src/worker/pool.ts`, calls
   `coordinator.markRunning()` post-lease-acquisition) — not at
   dispatch time. *Why:* lets `src/worker/reaper.ts` distinguish "sent
   to the queue but not yet picked up" (`queued`) from "a specific
   worker is actually executing this" (`running`), so crash detection
   reasons about real ownership, not a database row that merely says
   "running."

2. **Lease renewal and release use Lua scripts that check
   `current_owner == workerId` before acting**
   (`src/worker/leaseManager.ts`) — a bare `PEXPIRE`/`DEL` would let a
   slow/stale worker renew or release a lease another worker has since
   acquired after the first one's lease expired. This is a real race
   the atomic check closes.

3. **`step_executions` has a `UNIQUE(workflow_id, step_id,
   attempt_number)` constraint** (`schema.sql`) — the actual
   idempotency guard. A worker retrying an already-recorded attempt
   gets a constraint violation instead of double-executing it. Found
   necessary in Phase 1 (`ENGINEERING_LOG.md` bug #1: parallel test
   suites hitting the same DB without this).

4. **`reaper.ts`'s sweep has a `minAgeMs` guard before treating a
   dispatched-but-unleased step as abandoned** — without it, the
   reaper can fire before a legitimately-just-dispatched worker has had
   time to acquire its own lease, causing a false-positive "crash"
   retry of a step that was never actually abandoned.

5. **Dead-letter recording happens from two separate code paths**
   (`coordinator.ts`: `dispatchStep()`'s attempts-exhausted branch,
   *and* `handleStepResult()`'s immediate-task-failure branch) — a
   task's own thrown error is treated as immediately permanent (not
   retried, since a thrown error may mean a side effect already
   happened) and takes a different path than crash-retry exhaustion.
   `ENGINEERING_LOG.md` bug #7 documents this being missing from the
   second path originally.

6. **`jest.config.js` sets `maxWorkers: 1`** — integration tests share
   one real Postgres instance and each file `TRUNCATE`s shared tables
   in `beforeEach`; parallel test workers would let one file's cleanup
   wipe another's in-flight data (`ENGINEERING_LOG.md` bug #1).

7. **`GET /metrics` is exempt from both `requireApiKey` and the rate
   limiter** (`src/api/server.ts`) — Prometheus scrapers hit this
   frequently and don't send app-level auth headers by default; gating
   it would break scraping the moment `API_KEY` is set.

8. **The Prometheus gauges (`queue_depth`, `workflows_running`) are
   `.set()` fresh from a Postgres query inside `renderMetrics()` on
   every scrape** (`src/observability/metrics.ts`), not incremented in
   process memory — verified directly in this investigation (no
   in-process counter exists for these two). *Why:* an in-process gauge
   would be wrong the moment there's more than one worker replica, since
   each replica's memory only knows about the steps it personally
   touched.

9. **CORS is enabled globally, permissively** (`src/api/cors.ts`,
   `Access-Control-Allow-Origin: *`) — deliberate, not an oversight: this
   API has no cookie-based session to protect against CSRF, and a bearer
   token (when `API_KEY` is set) is never sent automatically by a
   browser, so an open CORS policy doesn't weaken that auth. Correct for
   a single-operator demo/portfolio tool; would need reconsidering
   before use in a multi-tenant service with user sessions.

10. **`GET /workflows/:id/stream` accepts `?api_key=` as an alternate to
    the `Authorization` header** (`src/api/auth.ts`) — not a weaker
    parallel auth scheme by accident, but the only way to authenticate
    this specific route at all from a browser: the built-in
    `EventSource` API cannot set custom headers, full stop. Real,
    stated tradeoff: a key passed this way can land in server access
    logs or browser history, which a header never would.

## 6. Provenance — how this document was produced

- Investigated a `.zip` snapshot of the repository uploaded on
  2026-09-12, filename `Workflow-Orchestration-Engine-main.zip` — this
  is a GitHub "download ZIP" archive (no `.git` directory), so **no
  commit-by-commit history was available**. Every claim about *current*
  code, schema, config, and test results below is directly verified
  against this snapshot's actual files. Every claim about *how the
  project got here* (bug history, phase-by-phase evolution) is sourced
  from this same snapshot's own `ENGINEERING_LOG.md` and `README.md` —
  i.e., the project's own account of its history, not independently
  re-derived from commit diffs. Tag: `INFERRED FROM PROJECT'S OWN DOCS,
  NOT FROM COMMIT HISTORY` for anything historical.
- `npm install` + `npm test` were actually re-run against a freshly
  installed Postgres 16 and Redis 7 in the investigation environment:
  **102/102 tests passed** at the time of this original investigation
  (now 107/107 after Phase 12's additions — see §7 for the current
  count). Directly verified either way, not taken on faith from the
  README.
- Table/column existence (`workflows`, `steps`, `step_executions`,
  `dead_letters`) was checked directly via `psql \d`, not assumed from
  `schema.sql` alone (though in this case they matched).
- The absence of AgentOS integration code was checked directly:
  `grep -rn "AgentOS" src/ tests/ README.md ENGINEERING_LOG.md` returned
  zero matches. This directly contradicts a claim made elsewhere (an
  external resume) that this engine is "integrated as the execution
  backbone for AgentOS" — that integration **does not exist in this
  codebase**. See §9.
- Benchmark numbers (§4) are `NOT VERIFIED` by this document's
  author — taken from the repo's own committed results, not
  re-executed here.
- **Phase 12 (CORS, the `?api_key=` fallback, `demo/index.html`) has
  stronger provenance than the rest of this document**: unlike the
  original snapshot-based investigation, this addition was verified by
  starting real API + worker processes and driving the exact HTTP/SSE
  calls the demo page makes via `curl` — confirming the real
  `Access-Control-Allow-Origin` header, the real SSE event shapes, and
  the query-param auth path against a live `API_KEY`-protected
  instance. The page's actual rendering in a browser was confirmed
  separately by the project's developer, not by this document's
  author, who cannot open a browser.

## 7. Security & testing summary

- **Auth:** optional API-key (`Authorization: Bearer <API_KEY>`, or
  `?api_key=` for `GET /workflows/:id/stream` specifically — §5
  invariant 10), off by default (`src/api/auth.ts`, `src/config.ts`).
  No OAuth, no RBAC, no multi-tenancy — single shared key model only.
- **CORS:** enabled globally, all origins (`src/api/cors.ts`) — §5
  invariant 9 for why this is a deliberate choice, not an oversight.
- **Rate limiting:** `express-rate-limit`, 100 req/min/IP default,
  always on regardless of `API_KEY` (`src/api/server.ts`).
- **Testing:** 107 tests across 15 files, **verified passing** in this
  investigation (not just claimed). Split: unit tests
  (`tests/unit/`: coordinator, leaseManager, reaper, validation,
  logger — no DB/Redis needed except where the component itself is
  Redis-backed) and integration tests (`tests/integration/`: real
  Postgres + Redis + real HTTP, including `auth.test.ts`,
  `cors.test.ts`, and `logging.test.ts`).
- **No CI/CD**: no `.github/workflows` or equivalent exists in this
  snapshot — tests are a manual `npm test` run, not gated on push/PR.
- **No dependency-vulnerability tracking** beyond what `npm audit`
  reports ad hoc (5 vulnerabilities — 4 moderate, 1 high — were flagged
  by `npm install` during this investigation; not investigated further
  here, not in the original scope of this project's own review passes).

## 8. Known limitations (as stated in the project's own README, verified present)

Directly present in `README.md`'s "Known limitations / not yet done"
section in this snapshot:

- No workflow-level timeout (only per-step `timeoutMs`).
- Recovery after an actual Postgres/Redis **container** restart is
  untested — what was tested is recovery via fresh *application*
  processes against a still-running database (`restartRecovery.test.ts`).
- No per-worker health gauges (`worker_active`/`worker_idle`) — checked
  directly in `metrics.ts`: confirmed absent.
- Benchmark numbers are single-run, not averaged with variance across
  repeated runs.
- The CLI (`scripts/cli.ts`) hasn't been revisited since it was first
  built.

Additional limitation identified in this investigation, not previously
documented in the repo:

- **In-process retry backoff** (`setTimeout` in
  `coordinator.ts`'s `dispatchStep()`): if the coordinator process
  crashes during a backoff wait, that scheduled retry is lost — there
  is no persisted `retry_due_at` or durable retry poller. A step stuck
  in `retrying` at the moment of a coordinator crash will not
  automatically resume without a fresh dispatch trigger.

## 9. Boundary with related/sibling projects

- **AgentOS** (a separate project, not in this repository): **no
  integration exists in this codebase** — directly verified via
  `grep` in this investigation (§6). An external document (a resume)
  describes this engine as "integrated as the execution backbone for
  AgentOS, transforming long-running multi-step agent tasks into
  fault-tolerant, dependency-resolved DAG jobs." That description does
  not correspond to anything in this repository as of this snapshot.
  If that integration is built later, it should appear as: AgentOS
  code calling this engine's REST API (or a shared library) to submit
  DAGs — nothing here currently does that, and nothing in this repo
  imports or references AgentOS.

## 10. AI contributor guardrails

1. **Do not remove the `maxWorkers: 1` setting in `jest.config.js`**
   without also restructuring how integration tests share the
   Postgres instance — it exists specifically to prevent one test
   file's `TRUNCATE` from wiping another's in-flight data
   (`ENGINEERING_LOG.md` bug #1).
2. **Do not call `closePool()` (`src/storage/db.ts`) in any `describe`
   block's `afterAll` unless it is genuinely the last thing to run in
   that test file** — the connection pool is a shared module-level
   singleton; closing it early silently 500s every subsequent test in
   the same file (`ENGINEERING_LOG.md` bug #9 — this actually happened
   during this project's own build).
3. **Do not give two tests, or two `WorkerPool` instances, the same
   Redis queue name** unless that's deliberately the point — whichever
   pool's worker calls `BRPOP` first wins the race, which can silently
   make a test exercise the wrong executor entirely
   (`ENGINEERING_LOG.md` bug #8).
4. **Do not add `recordDeadLetter()` calls without checking both call
   sites** (`dispatchStep()`'s attempts-exhausted branch and
   `handleStepResult()`'s immediate-failure branch) — a change to one
   without the other silently drops one class of permanent failure
   from `GET /dead-letters` again (`ENGINEERING_LOG.md` bug #7).
5. **Do not claim or imply an AgentOS integration exists in this repo**
   — it does not (§9). If asked to add one, it does not yet exist
   anywhere in this codebase to build on top of.
6. **Do not replace the Lua-script-based lease renew/release in
   `leaseManager.ts` with a plain `PEXPIRE`/`DEL`** — the
   `current_owner == workerId` check inside the Lua script is the
   actual race-condition fix (§5 invariant 2); removing it silently
   reintroduces the race.
7. **Do not assume `/metrics` requires the same auth as other routes**
   — it is deliberately exempt (§5 invariant 7); adding auth to it
   without updating the Prometheus scrape config (which sends no auth
   header) breaks metrics collection silently.
8. **Do not remove or restrict the global CORS policy
   (`src/api/cors.ts`) without checking `demo/index.html`'s use case
   first** — it's the reason the demo page can call the API at all
   from a different origin (§5 invariant 9). A well-intentioned
   "lock down CORS to known origins" change will silently break the
   demo unless the new origin list is kept in sync with wherever the
   demo page is actually opened from (which varies — it's a local
   file, not a fixed URL).
9. **Do not remove the `?api_key=` query-param fallback in
   `requireApiKey()`** on the assumption that header-only auth is
   strictly better — for `GET /workflows/:id/stream` specifically, it
   is the *only* way a browser can authenticate at all (§5 invariant
   10). Removing it breaks the live demo the moment `API_KEY` is set,
   not just weakens it.

## 11. Local development / quickstart

```bash
# from repo root
npm install
docker compose up -d postgres redis     # or run local Postgres 16 / Redis 7
npm run migrate                          # applies src/storage/schema.sql

# two separate terminals:
DATABASE_URL=postgresql://app:app@localhost:5432/workflow_engine \
REDIS_URL=redis://localhost:6379 \
npm run start:api

DATABASE_URL=postgresql://app:app@localhost:5432/workflow_engine \
REDIS_URL=redis://localhost:6379 \
npm run start:worker

# run tests (needs the same DATABASE_URL/REDIS_URL exported)
npm test

# CLI, once the API is running
npm run cli -- create <workflow.json>
```

`.env.example` in this snapshot shows `postgresql://...@localhost:5433`
and `redis://localhost:6380` — these are port remaps to avoid
colliding with another local project's containers on the default
ports (5432/6379), per the developer's own setup. Adjust to your
actual local ports.

## 12. Repository reference map

| Path | Subsystem |
|---|---|
| `src/core/coordinator.ts` | DAG dispatch, retry/backoff, dead-letter logic, workflow status transitions |
| `src/core/engine.ts` | Workflow creation/listing (thin wrapper over the repository) |
| `src/core/validation.ts` | DAG structure validation (duplicate step ids, cycles, etc.) |
| `src/scheduler/scheduler.ts` | Computes initially-runnable steps, kicks off `coordinator.start()` |
| `src/worker/pool.ts` | Worker process: consumes queue, acquires lease, executes, reports result |
| `src/worker/leaseManager.ts` | Redis lease acquire/renew/release (Lua-script atomic ownership checks) |
| `src/worker/reaper.ts` | Detects abandoned (lease-expired) steps, triggers retry |
| `src/worker/runner.ts` | Step execution + timeout wrapper (`withTimeout`) |
| `src/worker/main.ts` | Worker process entrypoint; also serves its own `/metrics` on `WORKER_METRICS_PORT` |
| `src/storage/schema.sql` | Full Postgres schema — 4 tables (§5 invariant 3) |
| `src/storage/workflowRepository.ts` | All Postgres access — the only place SQL lives |
| `src/storage/db.ts` | Shared module-level Postgres connection pool (§10 guardrail 2) |
| `src/queue/producer.ts` / `consumer.ts` | Redis list-based queue (`BRPOP`) |
| `src/queue/eventBus.ts` | Redis Pub/Sub for live status → SSE |
| `src/api/server.ts` | Express app: routes, auth + rate-limit middleware wiring |
| `src/api/auth.ts` | API-key middleware (§5 invariants 7, 10; §7) |
| `src/api/cors.ts` | Global CORS middleware (§5 invariant 9) |
| `src/api/index.ts` | API process entrypoint |
| `src/observability/metrics.ts` | All Prometheus counters/histograms/gauges (§5 invariant 8) |
| `src/observability/logger.ts` | Structured JSON logger, suppressed under `NODE_ENV=test` |
| `src/benchmarks/` | `dagGenerator.ts` (workload gen), `throughputBenchmark.ts`, `scalingExperiment.ts`, `speedupBenchmark.ts`, `failureRecoveryBenchmark.ts`, `report.ts` (runner) |
| `scripts/cli.ts` | CLI (create/list/get/cancel/watch) |
| `demo/index.html` | Self-contained live demo page — real SVG DAG viz over real SSE (§5 invariants 9, 10) |
| `docker/Dockerfile` | Multi-stage build → `api` and `worker` targets |
| `docker-compose.yml` | Full local stack: redis, postgres, migrate (one-shot), api, worker |
| `k8s/*.yaml` | Kubernetes manifests: `woe-api`, `woe-worker` Deployments + Postgres/Redis |
| `tests/unit/` | `coordinator.test.ts`, `leaseManager.test.ts`, `reaper.test.ts`, `validation.test.ts`, `logger.test.ts` |
| `tests/integration/` | `api.test.ts`, `auth.test.ts`, `cors.test.ts`, `engine.test.ts`, `faultTolerance.test.ts`, `logging.test.ts`, `reliability.test.ts`, `restartRecovery.test.ts`, `scheduler.test.ts`, `workflowRepository.test.ts` |

See `docs/handoff/` for deeper detail on each subsystem.
