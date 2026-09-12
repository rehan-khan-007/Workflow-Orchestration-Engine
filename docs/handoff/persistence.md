# Topic: Persistence (PostgreSQL schema, idempotency, restart recovery)

Covers: `src/storage/`. All table/column names below were verified by
directly running the migration and inspecting the resulting schema
with `psql \d` during this investigation — not just read from
`schema.sql`.

## Schema — 4 tables, confirmed present after migration

### `workflows`
| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `name` | TEXT NOT NULL | |
| `status` | TEXT NOT NULL DEFAULT 'pending' | `pending`/`running`/`completed`/`failed`/`cancelled` |
| `created_at`, `updated_at` | TIMESTAMPTZ | |

### `steps`
| Column | Type | Notes |
|---|---|---|
| `workflow_id` | UUID, FK → `workflows.id` ON DELETE CASCADE | |
| `step_id` | TEXT | |
| `depends_on` | JSONB DEFAULT '[]' | |
| `status` | TEXT DEFAULT 'pending' | see state machine in core-execution-engine.md |
| `attempt_count` | INT DEFAULT 0 | |
| `timeout_ms` | INT, nullable | per-step timeout, added later via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` |
| `created_at`, `updated_at` | TIMESTAMPTZ | |
| **PK** | `(workflow_id, step_id)` | composite |

### `step_executions`
| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `workflow_id`, `step_id` | | FK → `steps(workflow_id, step_id)` ON DELETE CASCADE |
| `attempt_number` | INT | |
| `status`, `worker_id`, `started_at`, `finished_at`, `error` | | |
| **UNIQUE** | `(workflow_id, step_id, attempt_number)` | **the idempotency guard** — see below |

### `dead_letters`
| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `workflow_id`, `step_id` | | not FK-constrained to `steps` (no `ON DELETE CASCADE` here) |
| `attempt_count` | INT | |
| `last_error` | TEXT, nullable | |
| `failed_at` | TIMESTAMPTZ DEFAULT now() | |

Migration applied cleanly against a fresh Postgres 16 instance during
this investigation (`npm run migrate` → "Migration applied
successfully.").

## The idempotency guard, concretely

The `UNIQUE(workflow_id, step_id, attempt_number)` constraint on
`step_executions` is what actually prevents double-execution: a worker
retrying an attempt number that was already recorded (e.g., because
the coordinator re-dispatched a step whose result was already written,
in some race) gets a Postgres unique-violation instead of silently
recording a second row for the same attempt.
`tests/integration/workflowRepository.test.ts` has a test named
exactly `"rejects a duplicate attempt number instead of double-
recording it"` exercising this directly.

## Restart recovery — what is and isn't actually tested

`tests/integration/restartRecovery.test.ts` (1 test,
`"a step abandoned by one 'process' is recovered and completed by an
entirely different one"`) constructs **entirely fresh** repository /
coordinator / worker-pool instances after simulating an abandonment —
proving recovery relies only on Postgres + Redis state, not on any
in-memory state surviving.

**What this does NOT test** (also stated in MASTER.md §8): restarting
the actual Postgres or Redis **containers** themselves. The test
simulates an application-process restart against a database that never
went down. Whether the system recovers correctly after the database
itself restarts (e.g., a brief connection-pool disruption, in-flight
transactions rolled back) is untested.

## `src/storage/db.ts` — the shared connection pool

A single module-level `Pool` instance, created lazily via `getPool()`
and torn down via `closePool()`. This is shared across the whole
process — including, notably, across every test file in the same Jest
process (`jest.config.js` sets `maxWorkers: 1`, so this is one process
for the whole test run). See MASTER.md §10 guardrail 2 for the real
bug this caused: calling `closePool()` in a `describe` block that
wasn't actually the last one in the file killed the connection for
every subsequent test.

## `WorkflowRepository` (`src/storage/workflowRepository.ts`)

The only place raw SQL lives in the codebase — every other module goes
through this. Confirmed methods referenced elsewhere in the codebase
(via grep across `src/`, not an exhaustive read of every method body):
`createWorkflow`, `getWorkflow`, `listWorkflows`, `updateStepStatus`,
`updateWorkflowStatus`, `recordExecutionAttempt`, `completeExecutionAttempt`,
`incrementAttempt`, `getAttemptCount`, `getLastError`, `recordDeadLetter`,
`listDeadLetters`, `listDispatchedSteps`, `getWorkflowCreatedAt`.
