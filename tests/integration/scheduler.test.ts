import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkerPool } from "../../src/worker/pool";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { getPool, closePool } from "../../src/storage/db";
import Redis from "ioredis";
import { Step, Workflow } from "../../src/types";

// These tests run a real DAG through real Redis + Postgres, using a
// dedicated queue name per test run so parallel test files (if ever run
// that way) don't collide.
describe("DagCoordinator + WorkerPool (Redis + Postgres backed)", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-scheduler-${Date.now()}`;
  let producer: QueueProducer;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let pool: WorkerPool;
  let redisRaw: Redis;

  beforeAll(() => {
    producer = new QueueProducer(queueName);
    coordinator = new DagCoordinator(repo, producer);
    scheduler = new DagScheduler(coordinator);
    redisRaw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
    await redisRaw.del(`${queueName}:queue`);
  });

  afterEach(async () => {
    if (pool) await pool.stop();
  });

  afterAll(async () => {
    await producer.close();
    await redisRaw.quit();
    await closePool();
  });

  async function waitForStatus(
    workflowId: string,
    status: string,
    timeoutMs = 5000
  ): Promise<Workflow> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const wf = await repo.getWorkflow(workflowId);
      if (wf && wf.status === status) return wf;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`Timed out waiting for workflow ${workflowId} to reach status ${status}`);
  }

  it("executes independent branches concurrently and completes the workflow", async () => {
    // a and b have no dependencies and can run in parallel; c depends on both.
    const overlapProof: { step: string; start: number; end: number }[] = [];
    const trackingExecutor = async (step: Step) => {
      const start = Date.now();
      await new Promise((r) => setTimeout(r, 300));
      overlapProof.push({ step: step.id, start, end: Date.now() });
    };

    pool = new WorkerPool(queueName, coordinator, 4, trackingExecutor);
    pool.start();

    const wf = await engine.createWorkflow("parallel-test", [
      { id: "a", dependsOn: [], status: "pending" },
      { id: "b", dependsOn: [], status: "pending" },
      { id: "c", dependsOn: ["a", "b"], status: "pending" },
    ]);

    await scheduler.schedule(wf);
    const finished = await waitForStatus(wf.id, "completed");

    expect(finished.steps.every((s) => s.status === "completed")).toBe(true);

    // Prove a and b actually overlapped in wall-clock time — this is the
    // real evidence of concurrent execution, not just "both eventually ran".
    const a = overlapProof.find((r) => r.step === "a")!;
    const b = overlapProof.find((r) => r.step === "b")!;
    const overlapped = a.start < b.end && b.start < a.end;
    expect(overlapped).toBe(true);

    // c must not have started until both a and b were recorded as finished.
    const c = overlapProof.find((r) => r.step === "c")!;
    expect(c.start).toBeGreaterThanOrEqual(Math.max(a.end, b.end) - 5);
  }, 10000);

  it("marks the workflow failed when a step fails, without running its dependents", async () => {
    const executed: string[] = [];
    const failingExecutor = async (step: Step) => {
      executed.push(step.id);
      if (step.id === "will-fail") throw new Error("simulated failure");
    };

    pool = new WorkerPool(queueName, coordinator, 2, failingExecutor);
    pool.start();

    const wf = await engine.createWorkflow("failure-test", [
      { id: "will-fail", dependsOn: [], status: "pending" },
      { id: "never-runs", dependsOn: ["will-fail"], status: "pending" },
    ]);

    await scheduler.schedule(wf);
    const finished = await waitForStatus(wf.id, "failed");

    expect(finished.steps.find((s) => s.id === "will-fail")!.status).toBe("failed");
    expect(executed).not.toContain("never-runs");
  }, 10000);

  it("marks an empty workflow completed immediately", async () => {
    const wf = await engine.createWorkflow("empty", []);
    await scheduler.schedule(wf);
    const finished = await repo.getWorkflow(wf.id);
    expect(finished!.status).toBe("completed");
  });
});
