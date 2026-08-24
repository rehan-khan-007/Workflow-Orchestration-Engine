import { Registry, Counter, Histogram, Gauge, collectDefaultMetrics } from "prom-client";
import { getPool } from "../storage/db";

export const register = new Registry();

// Standard Node.js process metrics (CPU, memory, event loop lag, GC) —
// free operational visibility that costs nothing to add.
collectDefaultMetrics({ register });

// --- Counters: incremented at the exact moment the real event happens,
// via DagCoordinator's setWorkflowStatus/dispatchStep (every status
// transition already funnels through those two methods, so this is the
// one place that needs instrumenting to cover every path). ---

export const workflowStartedTotal = new Counter({
  name: "workflow_started_total",
  help: "Total number of workflows that began running",
  registers: [register],
});

export const workflowCompletedTotal = new Counter({
  name: "workflow_completed_total",
  help: "Total number of workflows that completed successfully",
  registers: [register],
});

export const workflowFailedTotal = new Counter({
  name: "workflow_failed_total",
  help: "Total number of workflows that ended in failure",
  registers: [register],
});

export const workflowCancelledTotal = new Counter({
  name: "workflow_cancelled_total",
  help: "Total number of workflows that were cancelled",
  registers: [register],
});

export const stepDispatchedTotal = new Counter({
  name: "step_dispatched_total",
  help: "Total number of step dispatches (first attempts and retries combined)",
  registers: [register],
});

export const stepRetryTotal = new Counter({
  name: "step_retry_total",
  help: "Total number of step dispatches that were retries (attempt > 1)",
  registers: [register],
});

export const stepCompletedTotal = new Counter({
  name: "step_completed_total",
  help: "Total number of steps that completed successfully",
  registers: [register],
});

export const stepFailedTotal = new Counter({
  name: "step_failed_total",
  help: "Total number of steps that permanently failed",
  registers: [register],
});

export const deadLetterTotal = new Counter({
  name: "dead_letter_total",
  help: "Total number of steps recorded to the dead-letter table",
  registers: [register],
});

export const recoveryAttemptTotal = new Counter({
  name: "recovery_attempt_total",
  help: "Total number of retries triggered by the reaper detecting an abandoned (crashed-worker) step",
  registers: [register],
});

// --- Histograms ---

export const stepDurationSeconds = new Histogram({
  name: "step_duration_seconds",
  help: "Wall-clock time a worker spent executing a single step attempt (lease acquire through completion)",
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
  registers: [register],
});

export const workflowDurationSeconds = new Histogram({
  name: "workflow_duration_seconds",
  help: "Wall-clock time from a workflow's creation to it reaching a terminal state",
  buckets: [0.5, 1, 2.5, 5, 10, 30, 60, 300],
  registers: [register],
});

// --- Gauges: NOT tracked incrementally in-process, deliberately. An
// in-process counter would only reflect this one process's view and
// drift from reality across multiple API/worker replicas. These are
// computed fresh from Postgres every time /metrics is scraped instead —
// see renderMetrics() below. ---

const queueDepthGauge = new Gauge({
  name: "queue_depth",
  help: "Current number of steps waiting to run (queued or in a backoff retry wait), read live from Postgres",
  registers: [register],
});

const workflowsRunningGauge = new Gauge({
  name: "workflows_running",
  help: "Current number of workflows in the running state, read live from Postgres",
  registers: [register],
});

/**
 * Renders the current metrics snapshot as Prometheus text format. Updates
 * the live-computed gauges from Postgres immediately before rendering,
 * so every scrape reflects the actual current state, not a value that
 * could have drifted since the last update.
 */
export async function renderMetrics(): Promise<{ contentType: string; body: string }> {
  const pool = getPool();
  const [{ rows: queueRows }, { rows: runningRows }] = await Promise.all([
    pool.query(`SELECT count(*) FROM steps WHERE status IN ('queued', 'retrying')`),
    pool.query(`SELECT count(*) FROM workflows WHERE status = 'running'`),
  ]);
  queueDepthGauge.set(Number(queueRows[0].count));
  workflowsRunningGauge.set(Number(runningRows[0].count));

  return { contentType: register.contentType, body: await register.metrics() };
}
