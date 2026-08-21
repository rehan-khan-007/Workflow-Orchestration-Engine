import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { EventBus } from "../queue/eventBus";
import { DagCoordinator } from "../core/coordinator";
import { WorkerPool } from "./pool";
import { Reaper } from "./reaper";
import { LeaseManager } from "./leaseManager";
import { QUEUE_NAME, WORKER_POOL_SIZE } from "../config";

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

async function shutdown(): Promise<void> {
  console.log("Shutting down worker pool...");
  reaper.stop();
  await pool.stop();
  await producer.close();
  await eventBus.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
