import path from "path";
import express from "express";
import { WorkflowEngine } from "./core/engine";
import { DagCoordinator } from "./core/coordinator";
import { DagScheduler } from "./scheduler/scheduler";
import { WorkflowRepository } from "./storage/workflowRepository";
import { QueueProducer } from "./queue/producer";
import { EventBus } from "./queue/eventBus";
import { LeaseManager } from "./worker/leaseManager";
import { Reaper } from "./worker/reaper";
import { WorkerPool } from "./worker/pool";
import { QUEUE_NAME, API_PORT, API_KEY, WORKER_POOL_SIZE } from "./config";
import { createApp } from "./api/server";

/**
 * Combined entrypoint: runs the API and one worker pool in a single
 * process. This exists ONLY for hosting the public demo on a platform
 * whose free tier doesn't include a separate background-worker service
 * (e.g. Render) — it is NOT how this system is designed to run, and NOT
 * what src/api/index.ts + src/worker/main.ts (run as genuinely separate
 * processes, as in tests/integration/restartRecovery.test.ts and the
 * Kubernetes manifests in k8s/) demonstrate.
 *
 * Real, stated consequence of combining them: the "API stays up even if
 * every worker crashes" property this project is built around no longer
 * holds for this specific deployment — a crash here takes down both.
 * The actual distributed architecture (independent processes, surviving
 * each other's crashes) is what the test suite and k8s/ manifests prove;
 * this file trades that away for a single free-tier instance.
 *
 * One genuine simplification this enables, not just a downside: because
 * everything shares one process, GET /metrics here reports every
 * counter (workflow_completed_total, step_dispatched_total, etc.) from
 * one endpoint — the two-endpoint scrape story in the README exists
 * specifically because the real deployment has separate processes with
 * separate in-memory counters; that split doesn't apply here.
 */
const repo = new WorkflowRepository();
const engine = new WorkflowEngine(repo);
const producer = new QueueProducer(QUEUE_NAME);
const eventBus = new EventBus();
const coordinator = new DagCoordinator(repo, producer, 3, eventBus);
const scheduler = new DagScheduler(coordinator);
const leases = new LeaseManager();
const reaper = new Reaper(repo, leases, coordinator);
const pool = new WorkerPool(QUEUE_NAME, coordinator, repo, WORKER_POOL_SIZE);

const app = createApp(engine, scheduler, coordinator, eventBus, repo, API_KEY);

// Serve the demo page from the same origin as the API — so the public
// link is one URL, not two, and the page's own JS can default to
// "wherever I'm being served from" instead of needing a manually-typed
// API base URL. See demo/index.html's origin-detection logic.
app.use(express.static(path.join(__dirname, "..", "demo")));

pool.start();
reaper.start();
if (!API_KEY) {
  console.log("API_KEY not set — running without authentication (local/dev default).");
}
console.log(`Combined process: API + ${WORKER_POOL_SIZE}-worker pool on queue "${QUEUE_NAME}"`);

const server = app.listen(API_PORT, () => {
  console.log(`Listening on port ${API_PORT} — demo at "/", API at "/workflows" etc.`);
});

async function shutdown(): Promise<void> {
  console.log("Shutting down combined process...");
  reaper.stop();
  await pool.stop();
  server.close();
  await producer.close();
  await eventBus.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
