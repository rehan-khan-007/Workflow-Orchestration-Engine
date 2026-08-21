-- Workflow Orchestration Engine — persistence schema
-- Applied via `npm run migrate` (src/storage/migrate.ts)

CREATE TABLE IF NOT EXISTS workflows (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS steps (
  workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  step_id TEXT NOT NULL,
  depends_on JSONB NOT NULL DEFAULT '[]',
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, step_id)
);

-- One row per attempt at executing a step. The unique constraint on
-- (workflow_id, step_id, attempt_number) is the idempotency guard:
-- a worker retrying a step after a crash gets a constraint violation
-- if that attempt was already recorded, instead of double-executing it.
CREATE TABLE IF NOT EXISTS step_executions (
  id UUID PRIMARY KEY,
  workflow_id UUID NOT NULL,
  step_id TEXT NOT NULL,
  attempt_number INT NOT NULL,
  status TEXT NOT NULL,
  worker_id TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  error TEXT,
  FOREIGN KEY (workflow_id, step_id) REFERENCES steps(workflow_id, step_id) ON DELETE CASCADE,
  UNIQUE (workflow_id, step_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_steps_workflow ON steps(workflow_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_workflow_step ON step_executions(workflow_id, step_id);
