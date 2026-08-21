import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { getPool, closePool } from "../../src/storage/db";
import { Workflow } from "../../src/types";

describe("WorkflowRepository — idempotency guard", () => {
  const repo = new WorkflowRepository();

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
  });

  afterAll(async () => {
    await closePool();
  });

  async function seedWorkflow(): Promise<Workflow> {
    return repo.createWorkflow({
      id: crypto.randomUUID(),
      name: "idempotency-test",
      status: "running",
      steps: [{ id: "step-1", dependsOn: [], status: "pending" }],
    });
  }

  it("records a first execution attempt successfully", async () => {
    const wf = await seedWorkflow();
    const recorded = await repo.recordExecutionAttempt(wf.id, "step-1", 1, "worker-1");
    expect(recorded).toBe(true);
  });

  it("rejects a duplicate attempt number instead of double-recording it", async () => {
    // Simulates two workers (or a retry racing an in-flight execution)
    // both trying to claim attempt #1 of the same step.
    const wf = await seedWorkflow();
    const first = await repo.recordExecutionAttempt(wf.id, "step-1", 1, "worker-1");
    const duplicate = await repo.recordExecutionAttempt(wf.id, "step-1", 1, "worker-2");

    expect(first).toBe(true);
    expect(duplicate).toBe(false);

    const { rows } = await getPool().query(
      `SELECT count(*) FROM step_executions WHERE workflow_id = $1 AND step_id = $2 AND attempt_number = 1`,
      [wf.id, "step-1"]
    );
    expect(Number(rows[0].count)).toBe(1);
  });

  it("allows a new attempt number after a failed attempt (real retry)", async () => {
    const wf = await seedWorkflow();
    await repo.recordExecutionAttempt(wf.id, "step-1", 1, "worker-1");
    await repo.completeExecutionAttempt(wf.id, "step-1", 1, "failed", "simulated crash");

    const retry = await repo.recordExecutionAttempt(wf.id, "step-1", 2, "worker-2");
    expect(retry).toBe(true);
  });
});
