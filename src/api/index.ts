import { WorkflowEngine } from "../core/engine";
import { DagCoordinator } from "../core/coordinator";
import { DagScheduler } from "../scheduler/scheduler";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { EventBus } from "../queue/eventBus";
import { QUEUE_NAME, API_PORT } from "../config";
import { createApp } from "./server";

const repo = new WorkflowRepository();
const engine = new WorkflowEngine(repo);
const producer = new QueueProducer(QUEUE_NAME);
const eventBus = new EventBus();
const coordinator = new DagCoordinator(repo, producer, 3, eventBus);
const scheduler = new DagScheduler(coordinator);

const app = createApp(engine, scheduler, coordinator, eventBus, repo);
// This process's own GET /metrics (wired inside createApp) reliably
// shows workflow_started_total for every workflow, but NOT most
// completed/failed/duration counters — those get incremented wherever
// handleStepResult() actually runs, which for any workflow that
// executes real steps is the worker process, not this one. See
// src/worker/main.ts's own metrics server and the README's
// Observability section for the full two-target picture.

const server = app.listen(API_PORT, () => {
  console.log(`API listening on port ${API_PORT} (queue: ${QUEUE_NAME})`);
});

async function shutdown(): Promise<void> {
  console.log("Shutting down API...");
  server.close();
  await producer.close();
  await eventBus.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
