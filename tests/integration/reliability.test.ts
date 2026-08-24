import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkerPool } from "../../src/worker/pool";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { getPool, closePool } from "../../src/storage/db";
import Redis from "ioredis";

// Real timeout enforcement and dead-letter recording, against real
// Redis + Postgres — a step whose executor genuinely never resolves in
// time, and permanently-failed steps genuinely landing in the
// dead_letters table.
describe("Step timeouts and dead-letter recording (Redis + Postgres backed)", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-reliability-${Date.now()}`;
  let producer: QueueProducer;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let pool: WorkerPool | undefined;
  let redisRaw: Redis;

  beforeAll(() => {
    producer = new QueueProducer(queueName);
    // maxAttempts=1 so a timeout goes straight to permanent failure —
    // makes the dead-letter assertion simple and deterministic.
    coordinator = new DagCoordinator(repo, producer, 1);
    scheduler = new DagScheduler(coordinator);
    redisRaw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
    await getPool().query("TRUNCATE dead_letters");
    await redisRaw.del(`${queueName}:queue`);
  });

  afterEach(async () => {
    if (pool) {
      await pool.stop();
      pool = undefined;
    }
  });

  afterAll(async () => {
    await producer.close();
    await redisRaw.quit();
    await closePool();
  });

  async function waitForStatus(id: string, status: string, timeoutMs = 5000): Promise<any> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const wf = await repo.getWorkflow(id);
      if (wf && wf.status === status) return wf;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`Timed out waiting for ${id} to reach ${status}`);
  }

  it("a step whose executor never resolves is treated as failed once its timeoutMs elapses", async () => {
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      // Deliberately never resolves — proves the timeout, not the task,
      // is what ends this step.
      await new Promise(() => {});
    });
    pool.start();

    const wf = await engine.createWorkflow("timeout-test", [
      { id: "hangs-forever", dependsOn: [], status: "pending", timeoutMs: 300 },
    ]);
    await scheduler.schedule(wf);

    const finished = await waitForStatus(wf.id, "failed", 5000);
    expect(finished.steps[0].status).toBe("failed");
  }, 10000);

  it("a step that finishes well within its timeout completes normally", async () => {
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      await new Promise((r) => setTimeout(r, 50));
    });
    pool.start();

    const wf = await engine.createWorkflow("no-timeout-test", [
      { id: "quick", dependsOn: [], status: "pending", timeoutMs: 5000 },
    ]);
    await scheduler.schedule(wf);

    const finished = await waitForStatus(wf.id, "completed", 5000);
    expect(finished.steps[0].status).toBe("completed");
  }, 10000);

  it("a step with no timeoutMs set runs with no time limit at all", async () => {
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      await new Promise((r) => setTimeout(r, 400)); // would exceed a short timeout, but none was set
    });
    pool.start();

    const wf = await engine.createWorkflow("untimed-test", [
      { id: "no-limit", dependsOn: [], status: "pending" },
    ]);
    await scheduler.schedule(wf);

    const finished = await waitForStatus(wf.id, "completed", 5000);
    expect(finished.steps[0].status).toBe("completed");
  }, 10000);

  it("a permanently-failed step is recorded in the dead_letters table", async () => {
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      throw new Error("simulated permanent task failure");
    });
    pool.start();

    const wf = await engine.createWorkflow("dead-letter-test", [
      { id: "always-fails", dependsOn: [], status: "pending" },
    ]);
    await scheduler.schedule(wf);

    await waitForStatus(wf.id, "failed", 5000);

    const deadLetters = await repo.listDeadLetters();
    const entry = deadLetters.find((d) => d.workflowId === wf.id && d.stepId === "always-fails");
    expect(entry).toBeDefined();
    expect(entry!.lastError).toMatch(/simulated permanent task failure/);
  }, 10000);
});
