import Redis from "ioredis";
import { WorkflowEngine } from "../core/engine";
import { DagCoordinator } from "../core/coordinator";
import { DagScheduler } from "../scheduler/scheduler";
import { WorkerPool } from "../worker/pool";
import { Reaper } from "../worker/reaper";
import { LeaseManager } from "../worker/leaseManager";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { getPool } from "../storage/db";

export interface FailureRecoveryResult {
  injectedFailures: number;
  recoveredWithinThreshold: number;
  recoveryRatePct: number;
  avgRecoveryMs: number;
  p95RecoveryMs: number;
  thresholdMs: number;
}

const LEASE_TTL_MS = 3000;
const REAPER_INTERVAL_MS = 1000;
const REAPER_MIN_AGE_MS = 3500;

/**
 * Creates `count` single-step workflows, injects a real crash into every
 * one of them (a "worker" acquires the lease and dies with no heartbeat
 * or completion — the same mechanism proven in the Phase 3 tests, just
 * run at volume here), then brings a healthy worker pool + reaper online
 * and measures how long each one actually takes to recover.
 */
export async function runFailureRecoveryBenchmark(
  count: number,
  thresholdMs = 10000
): Promise<FailureRecoveryResult> {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `bench-failure-${Date.now()}`;
  const producer = new QueueProducer(queueName);
  const coordinator = new DagCoordinator(repo, producer, 3);
  const scheduler = new DagScheduler(coordinator);
  const leases = new LeaseManager();
  const reaper = new Reaper(repo, leases, coordinator, REAPER_INTERVAL_MS, REAPER_MIN_AGE_MS);
  const redisRaw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

  await getPool().query("TRUNCATE workflows CASCADE");
  await redisRaw.del(`${queueName}:queue`);

  const workflowIds: string[] = [];
  for (let i = 0; i < count; i++) {
    const wf = await engine.createWorkflow(`crash-bench-${i}`, [
      { id: "step", dependsOn: [], status: "pending" },
    ]);
    workflowIds.push(wf.id);
  }

  reaper.start();
  await Promise.all(
    workflowIds.map(async (id) => {
      const wf = await engine.getWorkflow(id);
      await scheduler.schedule(wf!);
    })
  );

  // Inject a crash into every one of them: pop the step off the queue,
  // acquire the lease, record the attempt — then abandon it entirely.
  const crashTimestamps = new Map<string, number>();
  await Promise.all(
    workflowIds.map(async (workflowId) => {
      const raw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
      const result = await raw.brpop(`${queueName}:queue`, 10);
      if (result) {
        const payload = JSON.parse(result[1]) as { stepId: string; attempt: number };
        await leases.acquire(workflowId, payload.stepId, `crashing-${workflowId}`, LEASE_TTL_MS);
        await repo.recordExecutionAttempt(
          workflowId,
          payload.stepId,
          payload.attempt,
          `crashing-${workflowId}`
        );
        crashTimestamps.set(workflowId, Date.now());
      }
      await raw.quit();
    })
  );

  // Now bring a healthy pool online to recover them.
  const pool = new WorkerPool(queueName, coordinator, repo, 4, async () => {
    await new Promise((r) => setTimeout(r, 100));
  }, LEASE_TTL_MS);
  pool.start();

  const recoveryTimes: number[] = [];
  const deadline = Date.now() + 60000;
  const pending = new Set(workflowIds);

  while (pending.size > 0 && Date.now() < deadline) {
    for (const id of Array.from(pending)) {
      const wf = await repo.getWorkflow(id);
      if (wf && (wf.status === "completed" || wf.status === "failed")) {
        const crashedAt = crashTimestamps.get(id)!;
        recoveryTimes.push(Date.now() - crashedAt);
        pending.delete(id);
      }
    }
    if (pending.size > 0) await new Promise((r) => setTimeout(r, 100));
  }

  reaper.stop();
  await pool.stop();
  await producer.close();
  await leases.close();
  await redisRaw.quit();

  recoveryTimes.sort((a, b) => a - b);
  const recoveredWithinThreshold = recoveryTimes.filter((t) => t <= thresholdMs).length;
  const avgRecoveryMs =
    recoveryTimes.reduce((sum, v) => sum + v, 0) / (recoveryTimes.length || 1);
  const p95RecoveryMs = recoveryTimes[Math.floor(recoveryTimes.length * 0.95)] ?? 0;

  return {
    injectedFailures: count,
    recoveredWithinThreshold,
    recoveryRatePct: (recoveredWithinThreshold / count) * 100,
    avgRecoveryMs,
    p95RecoveryMs,
    thresholdMs,
  };
}
