import Redis from "ioredis";
import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkerPool } from "../../src/worker/pool";
import { Reaper } from "../../src/worker/reaper";
import { LeaseManager } from "../../src/worker/leaseManager";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { getPool, closePool } from "../../src/storage/db";

const LEASE_TTL_MS = 800;
const REAPER_INTERVAL_MS = 300;
const REAPER_MIN_AGE_MS = 900;

/**
 * Fault tolerance's own test proves a crashed worker gets recovered
 * within a still-running process. This test proves something stronger:
 * recovery works even when the ENTIRE process that dispatched the work
 * is gone — a brand new WorkflowRepository, DagCoordinator, Reaper, and
 * WorkerPool, none of which share any in-memory state with whatever
 * dispatched the original work, can still pick it up and finish it. That
 * property only holds because everything needed to resume genuinely
 * lives in Postgres and Redis, not in any process's memory — which is
 * the actual claim behind "recovers after an API/worker restart".
 *
 * This test simulates a process restart directly (constructing entirely
 * fresh objects mid-test). It does not restart the actual Postgres/Redis
 * containers — that would be an infrastructure-level test outside what
 * an automated suite can reasonably do, and isn't needed to prove this
 * property: Postgres/Redis restarting cleanly is what "durable storage"
 * already means, not something this codebase's own logic could break.
 */
describe("Restart recovery: fresh process instances resume crashed work", () => {
  const queueName = `test-restart-${Date.now()}`;
  let redisRaw: Redis;

  beforeAll(() => {
    redisRaw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
    await redisRaw.del(`${queueName}:queue`);
  });

  afterAll(async () => {
    await redisRaw.quit();
    await closePool();
  });

  it("a step abandoned by one 'process' is recovered and completed by an entirely different one", async () => {
    // --- "Process A": dispatches the work, then simulates crashing ---
    const repoA = new WorkflowRepository();
    const engineA = new WorkflowEngine(repoA);
    const producerA = new QueueProducer(queueName);
    const coordinatorA = new DagCoordinator(repoA, producerA, 3);
    const schedulerA = new DagScheduler(coordinatorA);
    const leasesA = new LeaseManager();

    const wf = await engineA.createWorkflow("restart-recovery-test", [
      { id: "step", dependsOn: [], status: "pending" },
    ]);
    await schedulerA.schedule(wf); // dispatches attempt 1

    // A worker in "process A" picks the step up and then the whole
    // process disappears — no heartbeat, no completion, nothing.
    const raw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
    const item = await raw.brpop(`${queueName}:queue`, 5);
    const payload = JSON.parse(item![1]) as { stepId: string; attempt: number };
    await leasesA.acquire(wf.id, payload.stepId, "process-a-worker", LEASE_TTL_MS);
    await repoA.recordExecutionAttempt(wf.id, payload.stepId, payload.attempt, "process-a-worker");
    await raw.quit();

    // "Process A" is now gone. Discard every object it created — no
    // references to repoA/coordinatorA/leasesA are used again below.
    await producerA.close();
    await leasesA.close();

    // --- "Process B": entirely fresh instances, no shared state at all ---
    const repoB = new WorkflowRepository();
    const producerB = new QueueProducer(queueName);
    const coordinatorB = new DagCoordinator(repoB, producerB, 3);
    const leasesB = new LeaseManager();
    const reaperB = new Reaper(repoB, leasesB, coordinatorB, REAPER_INTERVAL_MS, REAPER_MIN_AGE_MS);
    const poolB = new WorkerPool(queueName, coordinatorB, repoB, 2, async () => {
      await new Promise((r) => setTimeout(r, 100));
    }, LEASE_TTL_MS);

    reaperB.start();
    poolB.start();

    const deadline = Date.now() + 10000;
    let finalStatus: string | undefined;
    while (Date.now() < deadline) {
      const current = await repoB.getWorkflow(wf.id);
      if (current && (current.status === "completed" || current.status === "failed")) {
        finalStatus = current.status;
        break;
      }
      await new Promise((r) => setTimeout(r, 100));
    }

    reaperB.stop();
    await poolB.stop();
    await producerB.close();
    await leasesB.close();

    expect(finalStatus).toBe("completed");
  }, 15000);
});
