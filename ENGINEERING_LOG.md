# Engineering Log

A record of what actually happened building this project — not a feature
list, but the bugs that were found by running things for real, and how
they were fixed. Kept here because "walk me through a bug you found and
fixed" is one of the most common interview questions, and these are all
real, verifiable ones: every fix below was reproduced, fixed, and
re-verified against a live Postgres + Redis before being committed.

## Scale, for context

- 2,206 lines of source code across 29 files
- 2,161 lines of test code — 102 tests, all passing, across 14 test files
- 11 phases taking the project from an in-memory skeleton to a
  Postgres-backed, Redis-parallel, fault-tolerant, containerized,
  Kubernetes-deployed, benchmarked, observable, authenticated system

## Bugs found and fixed

### 1. Parallel test suites silently corrupting each other's data

**Symptom:** A test asserting a duplicate execution attempt was rejected
intermittently failed with "expected 1, got 0" — but only when running
the full `npm test`, never when running a single file in isolation.

**Cause:** Jest's default is to run separate test *files* in parallel
worker processes. Two integration test files each ran `TRUNCATE workflows
CASCADE` in their own `beforeEach`, both against the same real Postgres
database. One file's cleanup could wipe out a row the other file had
just inserted, mid-test.

**Fix:** `maxWorkers: 1` in `jest.config.js` — integration tests share
one real database, so they need to run sequentially, not in parallel.

### 2. Docker image missing the SQL schema file at runtime

**Symptom:** `docker compose up` built successfully, but the migration
container failed: `ENOENT: no such file or directory,
'/app/dist/storage/schema.sql'`.

**Cause:** `tsc` only compiles `.ts` files — it has no reason to know
about `schema.sql`, so it never copied it into the `dist/` output the
container actually runs from. This never showed up in earlier testing
because the migration script had only ever been run via `ts-node`
directly from source, never from a container's compiled output in
isolation.

**Fix:** An explicit `RUN cp src/storage/schema.sql dist/storage/schema.sql`
step in the Dockerfile's build stage.

### 3. A malformed request hanging the API forever instead of returning 404

**Symptom:** Requesting `/workflows/not-a-real-id` (not valid UUID
syntax) never returned a response — the request just hung, which made an
integration test time out.

**Cause:** Postgres rejects a malformed UUID with an error, and Express
4 does not automatically catch a rejected promise inside an `async` route
handler — the rejection just vanished, and the response was never sent.

**Fix:** A `asyncRoute()` wrapper on every route that forwards a caught
error to Express's error-handling middleware, plus treating any lookup
failure (including a Postgres syntax error on a malformed id) the same
as "not found" — a bad id and a nonexistent id both just mean 404 to the
API consumer.

### 4. Queue latency measuring as exactly 0 for every request

**Symptom:** A benchmark meant to measure how long a step waited in the
queue before a worker picked it up reported `0ms` on every single run —
plausible-looking, but wrong.

**Cause:** The calculation compared a step's `updated_at` timestamp
(intended to capture "time of dispatch") against its execution's
`started_at` (time of pickup) — but `updated_at` gets overwritten again
when the step *completes*, hours or milliseconds later. By the time the
benchmark queried it, it no longer held the dispatch time at all.

**Fix:** Attach a `dispatchedAt` timestamp directly to the Redis queue
payload at the moment of dispatch, and measure elapsed time the instant
a worker picks the item up — exact, and independent of any mutable
database column.

### 5. Worker utilization measuring as 244.9% — mathematically impossible

**Symptom:** With exactly 4 concurrent workers, utilization can
structurally never exceed 100%. It measured 244.9% on a real Docker run
(though not in an earlier sandbox run on different hardware).

**Cause:** The calculation compared `wallClockMs` — measured by
`Date.now()` on the host machine — against Postgres's own
`started_at`/`finished_at` timestamps, recorded by the database server's
clock *inside a Docker container*. Those are two different clocks, and
Docker Desktop on macOS is known to let its VM clock drift from the
host, especially after sleep/wake. The drift silently corrupted the
result.

**Fix:** Measure busy-time entirely within one process, using one clock
— the same fix pattern as bug #4. Never compare a host-side timestamp
against a container-side one for a precision measurement again.

### 6. Test isolation bugs in the lease manager's own tests, twice

**Symptom (first pass):** New tests for `LeaseManager` failed
intermittently, always the same handful of tests, always failing
together.

**Cause (first pass):** Every test reused hardcoded keys (`"wf1"`,
`"step1"`) with no cleanup between tests. A lease acquired in one test,
still within its TTL, was still held when the next test ran.

**Fix (first pass):** Generate a unique workflow/step id per test via an
incrementing counter.

**Symptom (second pass):** Still flaky — but only when the same test
file was run several times in quick succession (exactly what repeated
verification does).

**Cause (second pass):** The counter resets to 0 every time the test
file loads in a fresh process. Two separate `npx jest` invocations
seconds apart could reuse the exact same key, while a 5-second-TTL lease
from the first run was still alive in Redis (an external server, not
per-process state) when the second run started.

**Fix (second pass):** `crypto.randomUUID()` instead of a counter —
globally unique regardless of process boundaries. Verified with 6 rapid
back-to-back runs, all clean.

### 7. Dead-letter table missing a whole class of permanent failures

**Symptom:** `GET /dead-letters` correctly showed steps that failed
after exhausting all retries — but a step whose *first* attempt threw a
real error (not a crash) never showed up there at all, despite the
workflow itself correctly ending up `failed`.

**Cause:** `recordDeadLetter()` was only ever called from
`dispatchStep()`'s attempts-exhausted branch — the path a *crash-then-
retry* failure takes. A task's own thrown error is deliberately treated
as immediately permanent (not retried at all, since a thrown error might
mean a side effect already happened), which routes through a completely
different code path (`handleStepResult()`) that never called
`recordDeadLetter()` at all.

**Fix:** Call `recordDeadLetter()` from both places — retry exhaustion
*and* an immediate task-level failure — each attaching the real
attempt count and error message from Postgres.

### 8. A test's own healthy worker pool "stealing" and succeeding a step meant to fail

**Symptom:** A test asserting a workflow ends up `failed` after an
always-throwing executor instead asserted `completed` — the workflow
actually *succeeded*, despite every task genuinely throwing.

**Cause:** The test's dedicated failing `WorkerPool` and an earlier
test's still-running healthy `WorkerPool` were both listening on the
*same* Redis queue name. Whichever pool's worker happened to `BRPOP` the
item first won the race — and the healthy pool usually won, executing
the step successfully with its own (working) executor instead of the
one the test intended to exercise.

**Fix:** Give the test its own dedicated queue name (and its own
producer/coordinator/scheduler bound to it), so nothing else can ever
compete for its items.

### 9. Closing a shared database connection pool mid-test-file, silently 500-ing every later test

**Symptom:** In a new test file with three separate `describe` blocks,
the first block's tests all passed — but every test in the second and
third blocks failed with an unexpected `500`, even for the simplest
possible request (`GET /workflows` with no auth needed at all).

**Cause:** `WorkflowRepository` (and everything built on it) shares one
module-level Postgres connection pool (`getPool()` in `src/storage/db.ts`)
for the lifetime of the process — by design, so many components don't
each open their own connections. The first `describe` block's `afterAll`
called `closePool()`, correctly cleaning up *if it were the last thing
in the file* — but Jest runs all three `describe` blocks in the same
process, sequentially, so closing the pool after the first one killed
the database connection for the second and third blocks' entire test
run.

**Fix:** Only call `closePool()` once, in the very last `describe`
block's `afterAll` for the whole file — never in an individual block
that isn't actually the end of the file's test run.

## What this adds up to

None of these were caught by writing more tests in the abstract — every
one was found by actually running the system against real Postgres,
real Redis, and (for #2, #3, #5) a real Docker container, and taking a
"received: X, expected: Y" mismatch seriously enough to trace it to a
real cause instead of loosening the assertion. #8 and #9 are a useful
reminder that this applies to test infrastructure itself, not just
application code — a test that silently exercises the wrong code path,
or fails for a reason unrelated to what it's actually checking, is its
own kind of bug.
