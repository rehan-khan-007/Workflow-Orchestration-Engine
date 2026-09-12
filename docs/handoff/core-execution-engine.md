# Topic: Core execution engine (DAG dispatch, workers, fault tolerance)

Covers: `src/core/`, `src/scheduler/`, `src/worker/`, `src/queue/`.

## Step state machine

Exact states, from `src/types/index.ts` and `coordinator.ts` usage:

```
pending → queued → running → completed
                       ↓
                    failed  (permanent, no retry — task threw)
                       ↓
                   retrying (crash-detected, will re-dispatch after backoff)
```

- `pending`: initial state, dependencies not yet all satisfied.
- `queued`: `coordinator.dispatchStep()` has pushed it to Redis, no
  worker has picked it up yet.
- `running`: set by `coordinator.markRunning()`, called from
  `worker/pool.ts` **only after** the worker has successfully acquired
  the step's Redis lease. This ordering is invariant §5.1 in MASTER.md.
- `completed` / `failed`: terminal, from `handleStepResult()`.
- `retrying`: set when a crash (lease-expired, detected by
  `reaper.ts`) triggers a backoff-then-redispatch, distinct from an
  immediate task-level failure (which goes straight to `failed`, no
  retry — see below).

## Retry policy — two genuinely different failure paths

1. **Crash-detected failure** (`reaper.ts` → `coordinator.retryStep()`
   → `dispatchStep()`): the step's worker died mid-execution (lease
   expired). This path retries, with exponential backoff
   (`computeBackoffMs`: base 200ms, max 5000ms, `delayMs = 0` on first
   attempt), up to `maxAttempts` (constructor param on
   `DagCoordinator`, `3` in the codebase's own usage).
2. **Task-level failure** (`handleStepResult(success=false)`): the
   step's own code threw an error. This is treated as **immediately
   permanent** — no retry — because a thrown error might mean a side
   effect already happened, and the codebase deliberately does not try
   to guess whether it's safe to retry that.

Both paths independently call `recordDeadLetter()` once attempts are
exhausted / immediately for a task failure — see MASTER.md §5 invariant
5 for why this is two call sites, not one.

## Lease mechanics (`src/worker/leaseManager.ts`)

- **Acquire**: `SET <key> <workerId> NX PX <ttl>` — atomic
  create-if-absent with a TTL.
- **Renew**: a Lua script checking `redis.call('get', KEYS[1]) ==
  ARGV[1]` before `PEXPIRE` — refuses to renew if a different worker
  now owns the key.
- **Release**: same ownership check before `DEL`.

The ownership check in renew/release is the actual fix for a real
race: without it, a worker whose lease already expired (and was
reacquired by a different worker, e.g. after the reaper detected the
"crash") could still renew or delete the new owner's lease if its own
renewal heartbeat fires late. `pool.ts` runs a `setInterval` heartbeat
at `heartbeatIntervalMs` (constructor param) calling `leases.renew()`.

## Reaper (`src/worker/reaper.ts`)

- Scans `repo.listDispatchedSteps()` — steps in `queued` or `running`.
- For each, checks `leases.exists()`. If the lease is gone, calls
  `coordinator.retryStep()`.
- **`minAgeMs` guard**: a step is only eligible for reaping if it's
  been dispatched for at least this long — prevents the reaper racing
  a legitimately-just-dispatched step whose worker hasn't acquired its
  lease yet (which would otherwise look identical to an abandoned
  step).

## Step timeout (`src/worker/runner.ts`)

- `withTimeout()` races the step's executor promise against a timer.
- **Does not forcibly cancel the in-flight computation** — Node.js
  cannot cancel an arbitrary Promise. On timeout, the step is marked
  failed while the original executor may still be running in the
  background. This is a documented, real limitation, not an oversight
  — see MASTER.md §8.
- Only step-level, not workflow-level. A workflow with no single slow
  step but many moderately slow ones (or successful-but-slow steps)
  has no overall time bound.

## Cancellation

- `coordinator.cancel()`-driven; `coordinator.test.ts` includes
  cancellation-during-retry edge cases and a duplicate-dispatch
  regression test
  (`"a duplicate handleStepResult call for the same step does not
  double-dispatch its dependent"`).

## DAG validation (`src/core/validation.ts`)

- Checked at workflow-creation time (`POST /workflows`), before
  anything is persisted or dispatched.
- Verified cases (from `tests/unit/validation.test.ts`): duplicate
  step ids rejected (`400`), presumably cycle detection and
  dangling-dependency checks — **not individually re-verified in this
  investigation beyond what the test file's own assertions show**;
  read `validation.test.ts` directly for the exact set of rejected
  shapes.

## Known limitation specific to this subsystem

**In-process retry backoff.** `dispatchStep()`'s retry path uses a
bare `setTimeout(() => this.enqueueStep(...), delayMs)`. If the
coordinator process crashes while a step is mid-backoff, that
scheduled timer is lost with the process — the step remains stuck in
`retrying` in Postgres with no in-memory timer to ever redispatch it.
There is no persisted `retry_due_at` column and no separate durable
poller that would pick this up on a fresh coordinator process. This is
not documented in the repo's own README as of this snapshot — it's an
observation from reading `coordinator.ts` directly during this
investigation.
