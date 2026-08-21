import Redis from "ioredis";
import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkerPool } from "../../src/worker/pool";
import { Reaper } from "../../src/worker/reaper";
import { LeaseManager } from "../../src/worker/leaseManager";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { QueueConsumer } from "../../src/queue/consumer";
import { getPool, closePool } from "../../src/storage/db";
import { Workflow } from "../../src/types";

// Short timings so tests run fast while still exercising real TTL expiry
// and real interval-based polling — nothing here is mocked or faked.
const LEASE_TTL_MS = 800;
const REAPER_INTERVAL_MS = 300;
const REAPER_MIN_AGE_MS = 900;

describe("Fault tolerance: crashed-worker recovery (Redis + Postgres backed)", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-fault-${Date.now()}`;
  let producer: QueueProducer;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let leases: LeaseManager;
  let reaper: Reaper;
  let pool: WorkerPool | undefined;
  let redisRaw: Redis;

  beforeAll(() => {
    producer = new QueueProducer(queueName);
    coordinator = new DagCoordinator(repo, producer, 3);
    scheduler = new DagScheduler(coordinator);
    leases = new LeaseManager();
    reaper = new Reaper(repo, leases, coordinator, REAPER_INTERVAL_MS, REAPER_MIN_AGE_MS);
    redisRaw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
    await redisRaw.del(`${queueName}:queue`);
    reaper.start();
  });

  afterEach(async () => {
    reaper.stop();
    if (pool) {
      await pool.stop();
      pool = undefined;
    }
  });

  afterAll(async () => {
    await producer.close();
    await leases.close();
    await redisRaw.quit();
    await closePool();
  });

  async function waitForStatus(
    workflowId: string,
    status: string,
    timeoutMs: number
  ): Promise<Workflow> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const wf = await repo.getWorkflow(workflowId);
      if (wf && wf.status === status) return wf;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`Timed out waiting for workflow ${workflowId} to reach status ${status}`);
  }

  /**
   * Simulates a worker that picks a step off the queue, acquires its
   * lease, records the execution attempt — then dies. No heartbeat, no
   * completion, nothing. This is what an actual process crash mid-step
   * looks like from the system's point of view: the item is gone from
   * the queue, a lease briefly exists, then nothing renews it.
   */
  async function simulateWorkerCrash(): Promise<void> {
    const raw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
    const result = await raw.brpop(`${queueName}:queue`, 5);
    if (!result) throw new Error("Crashing worker never received a step to crash on");
    const payload = JSON.parse(result[1]) as {
      workflowId: string;
      stepId: string;
      attempt: number;
    };
    const crashingWorkerId = "crashing-worker";
    await leases.acquire(payload.workflowId, payload.stepId, crashingWorkerId, LEASE_TTL_MS);
    await repo.recordExecutionAttempt(
      payload.workflowId,
      payload.stepId,
      payload.attempt,
      crashingWorkerId
    );
    // Deliberately: no heartbeat, no completeExecutionAttempt, no
    // coordinator.handleStepResult. The lease will expire on its own.
    await raw.quit();
  }

  it("recovers a workflow after its worker crashes mid-step, within 10 seconds", async () => {
    const wf = await engine.createWorkflow("crash-recovery", [
      { id: "flaky-step", dependsOn: [], status: "pending" },
    ]);

    await scheduler.schedule(wf); // dispatches flaky-step, attempt 1
    await simulateWorkerCrash(); // "worker" takes attempt 1 off the queue and dies

    // A real worker pool comes online after the crash — like a
    // replacement process starting up — and should pick up the retry
    // once the reaper notices the lease expired and requeues it.
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      await new Promise((r) => setTimeout(r, 100));
    }, LEASE_TTL_MS);
    pool.start();

    const finished = await waitForStatus(wf.id, "completed", 10000);
    expect(finished.steps[0].status).toBe("completed");

    // Confirm it actually took two attempts — one abandoned, one that
    // succeeded — proving recovery happened rather than the crash
    // somehow not mattering.
    const { rows } = await getPool().query(
      `SELECT attempt_number, status FROM step_executions
       WHERE workflow_id = $1 AND step_id = $2 ORDER BY attempt_number`,
      [wf.id, "flaky-step"]
    );
    expect(rows.length).toBeGreaterThanOrEqual(2);
    expect(rows[0].status).toBe("running"); // the crashed attempt, never completed
    expect(rows[rows.length - 1].status).toBe("completed");
  }, 15000);

  it("permanently fails a step after exhausting max attempts, no infinite retry loop", async () => {
    // maxAttempts is 3 for this coordinator (set in beforeAll). We crash
    // every attempt with no real worker pool ever completing it, so it
    // must exhaust retries and fail rather than retry forever.
    const wf = await engine.createWorkflow("exhausted-retries", [
      { id: "always-crashes", dependsOn: [], status: "pending" },
    ]);

    await scheduler.schedule(wf);

    for (let i = 0; i < 3; i++) {
      await simulateWorkerCrash();
    }

    const finished = await waitForStatus(wf.id, "failed", 10000);
    expect(finished.steps[0].status).toBe("failed");

    const { rows } = await getPool().query(
      `SELECT count(*) FROM step_executions WHERE workflow_id = $1 AND step_id = $2`,
      [wf.id, "always-crashes"]
    );
    // Exactly maxAttempts recorded attempts — not more, proving the
    // retry loop actually stopped rather than continuing indefinitely.
    expect(Number(rows[0].count)).toBe(3);
  }, 15000);
});
