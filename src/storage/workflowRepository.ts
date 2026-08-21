import { Pool } from "pg";
import { Workflow, WorkflowStatus, StepStatus } from "../types";
import { getPool } from "./db";

const UNIQUE_VIOLATION = "23505";

export class WorkflowRepository {
  constructor(private pool: Pool = getPool()) {}

  async createWorkflow(workflow: Workflow): Promise<Workflow> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO workflows (id, name, status) VALUES ($1, $2, $3)`,
        [workflow.id, workflow.name, workflow.status]
      );
      for (const step of workflow.steps) {
        await client.query(
          `INSERT INTO steps (workflow_id, step_id, depends_on, status)
           VALUES ($1, $2, $3, $4)`,
          [workflow.id, step.id, JSON.stringify(step.dependsOn), step.status]
        );
      }
      await client.query("COMMIT");
      return workflow;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async getWorkflow(id: string): Promise<Workflow | undefined> {
    const wfResult = await this.pool.query(
      `SELECT id, name, status FROM workflows WHERE id = $1`,
      [id]
    );
    if (wfResult.rows.length === 0) return undefined;

    const stepsResult = await this.pool.query(
      `SELECT step_id, depends_on, status FROM steps WHERE workflow_id = $1 ORDER BY step_id`,
      [id]
    );

    const row = wfResult.rows[0];
    return {
      id: row.id,
      name: row.name,
      status: row.status as WorkflowStatus,
      steps: stepsResult.rows.map((s) => ({
        id: s.step_id,
        dependsOn: s.depends_on as string[],
        status: s.status as StepStatus,
      })),
    };
  }

  async listWorkflows(): Promise<Workflow[]> {
    const wfResult = await this.pool.query(
      `SELECT id FROM workflows ORDER BY created_at`
    );
    const workflows: Workflow[] = [];
    for (const row of wfResult.rows) {
      const wf = await this.getWorkflow(row.id);
      if (wf) workflows.push(wf);
    }
    return workflows;
  }

  async updateWorkflowStatus(id: string, status: WorkflowStatus): Promise<void> {
    await this.pool.query(
      `UPDATE workflows SET status = $1, updated_at = now() WHERE id = $2`,
      [status, id]
    );
  }

  async updateStepStatus(
    workflowId: string,
    stepId: string,
    status: StepStatus
  ): Promise<void> {
    await this.pool.query(
      `UPDATE steps SET status = $1, updated_at = now()
       WHERE workflow_id = $2 AND step_id = $3`,
      [status, workflowId, stepId]
    );
  }

  /**
   * Records a new execution attempt for a step. Returns false instead of
   * throwing if this (workflowId, stepId, attemptNumber) was already
   * recorded — this is the idempotency guard a retrying worker checks
   * before doing real work, so a duplicate dispatch is a no-op instead
   * of a double-execution.
   */
  async recordExecutionAttempt(
    workflowId: string,
    stepId: string,
    attemptNumber: number,
    workerId: string
  ): Promise<boolean> {
    try {
      await this.pool.query(
        `INSERT INTO step_executions
           (id, workflow_id, step_id, attempt_number, status, worker_id, started_at)
         VALUES ($1, $2, $3, $4, 'running', $5, now())`,
        [crypto.randomUUID(), workflowId, stepId, attemptNumber, workerId]
      );
      return true;
    } catch (err: unknown) {
      if ((err as { code?: string }).code === UNIQUE_VIOLATION) return false;
      throw err;
    }
  }

  async completeExecutionAttempt(
    workflowId: string,
    stepId: string,
    attemptNumber: number,
    status: "completed" | "failed",
    error?: string
  ): Promise<void> {
    await this.pool.query(
      `UPDATE step_executions SET status = $1, finished_at = now(), error = $2
       WHERE workflow_id = $3 AND step_id = $4 AND attempt_number = $5`,
      [status, error ?? null, workflowId, stepId, attemptNumber]
    );
  }
}
