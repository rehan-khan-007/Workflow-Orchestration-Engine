import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { EventBus } from "../queue/eventBus";
import { DagCoordinator } from "../core/coordinator";
import { WorkerPool } from "./pool";
import { Reaper } from "./reaper";
import { LeaseManager } from "./leaseManager";
import { QUEUE_NAME, WORKER_POOL_SIZE, WORKER_METRICS_PORT } from "../config";
import { renderMetrics } from "../observability/metrics";
import { createServer } from "http";

/**
 * Standalone worker process: pulls steps off the shared Redis queue and
 * executes them, with lease-based crash detection via the Reaper. Runs
 * independently of the API process — in Phase 5 this is the entrypoint
 * that becomes the container image deployed as N Kubernetes pods.
 */
const repo = new WorkflowRepository();
const producer = new QueueProducer(QUEUE_NAME);
const eventBus = new EventBus();
const coordinator = new DagCoordinator(repo, producer, 3, eventBus);
const leases = new LeaseManager();
const reaper = new Reaper(repo, leases, coordinator);
const pool = new WorkerPool(QUEUE_NAME, coordinator, repo, WORKER_POOL_SIZE);

pool.start();
reaper.start();
console.log(`Worker pool started: ${WORKER_POOL_SIZE} workers on queue "${QUEUE_NAME}"`);

// Most counters end up incremented here, in the worker process — not
// just step-level ones. setWorkflowStatus() (which increments
// workflow_completed_total/workflow_failed_total and observes
// workflow_duration_seconds) is called from inside handleStepResult(),
// and for any workflow that actually executes steps (the normal case),
// that call happens via THIS process's WorkerPool executor callback,
// using this process's own DagCoordinator instance — not the API
// process's. The API process's /metrics only reliably shows
// workflow_started_total for every workflow, plus completed/failed for
// the edge cases that resolve synchronously inside start() itself (an
// empty workflow, or a cycle detected before any step ever dispatches).
// Prometheus scrape config needs BOTH this port and the API's — this is
// standard multi-target scraping, not a workaround; a real Prometheus
// server aggregates across instances/jobs via PromQL, it doesn't expect
// one endpoint to hold the whole picture. See README's Observability
// section.
const metricsServer = createServer(async (req, res) => {
  if (req.url === "/metrics") {
    const { contentType, body } = await renderMetrics();
    res.writeHead(200, { "Content-Type": contentType });
    res.end(body);
  } else {
    res.writeHead(404);
    res.end();
  }
});
metricsServer.listen(WORKER_METRICS_PORT, () => {
  console.log(`Worker metrics available on port ${WORKER_METRICS_PORT} (GET /metrics)`);
});

async function shutdown(): Promise<void> {
  console.log("Shutting down worker pool...");
  reaper.stop();
  await pool.stop();
  await producer.close();
  await eventBus.close();
  metricsServer.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
