import { WorkflowEngine } from "../core/engine";
import { DagCoordinator } from "../core/coordinator";
import { DagScheduler } from "../scheduler/scheduler";
import { WorkerPool } from "../worker/pool";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { getPool } from "../storage/db";
import { generateWorkloadBatch } from "./dagGenerator";

export interface ThroughputResult {
  workflowCount: number;
  totalSteps: number;
  wallClockMs: number;
  stepsPerMinute: number;
  avgQueueLatencyMs: number;
  p95QueueLatencyMs: number;
  workerUtilizationPct: number;
  workerCount: number;
}

/**
 * Submits a batch of synthetic DAG workflows through the real
 * coordinator + Redis-backed worker pool, waits for all of them to
 * finish, then computes throughput/latency/utilization purely from what
 * actually happened in Postgres — no numbers are assumed, everything is
 * derived from real timestamps recorded during real execution.
 */
export async function runThroughputBenchmark(
  workflowCount: number,
  workerCount: number,
  taskDurationMs: () => number
): Promise<ThroughputResult> {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `bench-throughput-${Date.now()}`;
  const producer = new QueueProducer(queueName);
  const coordinator = new DagCoordinator(repo, producer, 3);
  const scheduler = new DagScheduler(coordinator);
  const queueLatencies: number[] = [];
  const busyDurationsMs: number[] = [];
  const pool = new WorkerPool(
    queueName,
    coordinator,
    repo,
    workerCount,
    async () => {
      await new Promise((r) => setTimeout(r, taskDurationMs()));
    },
    undefined,
    undefined,
    (latencyMs) => queueLatencies.push(latencyMs),
    (durationMs) => busyDurationsMs.push(durationMs)
  );

  await getPool().query("TRUNCATE workflows CASCADE");
  pool.start();

  const { workloads, totalSteps } = generateWorkloadBatch(workflowCount);

  const startedAt = Date.now();
  const created = await Promise.all(
    workloads.map((w) => engine.createWorkflow(w.name, w.steps))
  );
  await Promise.all(created.map((wf) => scheduler.schedule(wf)));

  // Poll until every workflow reaches a terminal state.
  const deadline = Date.now() + 5 * 60 * 1000; // 5 minute hard ceiling
  let allDone = false;
  while (!allDone && Date.now() < deadline) {
    const { rows } = await getPool().query(
      `SELECT count(*) FROM workflows WHERE status NOT IN ('completed','failed')`
    );
    allDone = Number(rows[0].count) === 0;
    if (!allDone) await new Promise((r) => setTimeout(r, 200));
  }
  const wallClockMs = Date.now() - startedAt;

  await pool.stop();
  await producer.close();

  if (!allDone) {
    throw new Error("Benchmark timed out before all workflows completed");
  }

  // Queue latency: measured exactly, at the moment a worker picks up
  // each item, as (pickup time - dispatchedAt timestamp attached by the
  // coordinator when it enqueued). This is precise and doesn't rely on
  // any DB column, since steps.updated_at gets overwritten again on
  // completion and can't be used to recover the original dispatch time.
  const latencies = [...queueLatencies].sort((a, b) => a - b);
  const avgQueueLatencyMs =
    latencies.reduce((sum, v) => sum + v, 0) / (latencies.length || 1);
  const p95QueueLatencyMs = latencies[Math.floor(latencies.length * 0.95)] ?? 0;

  // Worker utilization: total busy-time across all step executions,
  // divided by total available worker-time (workerCount * wall clock).
  // Measured entirely via Date.now() in this same process — deliberately
  // NOT computed by comparing wallClockMs (host clock) against Postgres
  // server timestamps, since those are two different clocks (especially
  // across a Docker container boundary) that can drift relative to each
  // other and silently produce impossible results like >100% utilization.
  const busySeconds = busyDurationsMs.reduce((sum, v) => sum + v, 0) / 1000;
  const availableSeconds = (workerCount * wallClockMs) / 1000;
  const workerUtilizationPct = (busySeconds / availableSeconds) * 100;

  const stepsPerMinute = (totalSteps / (wallClockMs / 1000)) * 60;

  return {
    workflowCount,
    totalSteps,
    wallClockMs,
    stepsPerMinute,
    avgQueueLatencyMs,
    p95QueueLatencyMs,
    workerUtilizationPct,
    workerCount,
  };
}
