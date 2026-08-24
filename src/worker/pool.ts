import { randomUUID } from "crypto";
import { QueueConsumer } from "../queue/consumer";
import { DagCoordinator } from "../core/coordinator";
import { LeaseManager } from "./leaseManager";
import { WorkflowRepository } from "../storage/workflowRepository";
import { StepExecutor, defaultStepExecutor, withTimeout } from "./runner";
import * as metrics from "../observability/metrics";
import { log } from "../observability/logger";

interface StepQueuePayload {
  workflowId: string;
  stepId: string;
  attempt: number;
  dispatchedAt?: number;
  timeoutMs?: number;
}

const DEFAULT_LEASE_TTL_MS = 5000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 1500;

/**
 * A pool of `size` independent Redis consumers, all listening on the same
 * queue — real parallel execution, same as Phase 2. Phase 3 adds: each
 * worker acquires a lease before running a step and heartbeats it while
 * running, so a crashed worker's abandonment is detectable (the lease
 * just stops being renewed and expires) instead of the step vanishing
 * silently when BRPOP removes it from the queue.
 */
export class WorkerPool {
  private consumers: QueueConsumer[] = [];
  private leases: LeaseManager;

  constructor(
    private queueName: string,
    private coordinator: DagCoordinator,
    private repo: WorkflowRepository,
    private size: number = 4,
    private executor: StepExecutor = defaultStepExecutor,
    private leaseTtlMs: number = DEFAULT_LEASE_TTL_MS,
    private heartbeatIntervalMs: number = DEFAULT_HEARTBEAT_INTERVAL_MS,
    // Optional: called with exact queue wait time (ms) the instant a
    // worker picks up an item, using the dispatchedAt timestamp the
    // coordinator attaches at enqueue. Used by benchmarks; production
    // code has no reason to pass this.
    private onQueueLatency?: (latencyMs: number) => void,
    // Optional: called with the exact wall-clock time (ms) spent actually
    // executing a step (lease acquire through completion recording),
    // measured entirely with this process's own Date.now() — deliberately
    // never compared against a Postgres server timestamp, since that's a
    // different clock (especially across a Docker container boundary) and
    // can drift relative to the host, producing bogus deltas.
    private onTaskBusy?: (durationMs: number) => void
  ) {
    this.leases = new LeaseManager();
  }

  start(): void {
    for (let i = 0; i < this.size; i++) {
      const workerId = `worker-${i}-${randomUUID()}`;
      const consumer = new QueueConsumer(this.queueName);
      // Fire-and-forget: start() runs its own internal loop until stop()/close().
      consumer.start(async (payload) => {
        const { workflowId, stepId, attempt, dispatchedAt, timeoutMs } =
          payload as unknown as StepQueuePayload;

        if (this.onQueueLatency && dispatchedAt) {
          this.onQueueLatency(Date.now() - dispatchedAt);
        }

        const acquired = await this.leases.acquire(workflowId, stepId, workerId, this.leaseTtlMs);
        if (!acquired) {
          // Someone else already holds this step's lease (e.g. a retry
          // raced with the original worker finishing right as we picked
          // this up). Don't double-execute — just skip.
          return;
        }

        const recorded = await this.repo.recordExecutionAttempt(
          workflowId,
          stepId,
          attempt,
          workerId
        );
        if (!recorded) {
          // This exact attempt was already recorded — idempotency guard
          // catching a duplicate dispatch. Release the lease and stop.
          await this.leases.release(workflowId, stepId, workerId);
          return;
        }

        await this.coordinator.markRunning(workflowId, stepId);
        log({ event: "step_execution_started", workflowId, stepId, attempt, workerId });

        const heartbeat = setInterval(() => {
          this.leases.renew(workflowId, stepId, workerId, this.leaseTtlMs).catch(() => {});
        }, this.heartbeatIntervalMs);

        const busyStart = Date.now();
        try {
          await withTimeout(
            this.executor({ id: stepId, dependsOn: [], status: "running", timeoutMs }),
            timeoutMs
          );
          clearInterval(heartbeat);
          await this.repo.completeExecutionAttempt(workflowId, stepId, attempt, "completed");
          const durationMs = Date.now() - busyStart;
          metrics.stepDurationSeconds.observe(durationMs / 1000);
          log({
            event: "step_execution_finished",
            workflowId,
            stepId,
            attempt,
            workerId,
            durationMs,
            result: "completed",
          });
          this.onTaskBusy?.(durationMs);
          await this.leases.release(workflowId, stepId, workerId);
          await this.coordinator.handleStepResult(workflowId, stepId, true);
        } catch (err) {
          clearInterval(heartbeat);
          await this.repo.completeExecutionAttempt(
            workflowId,
            stepId,
            attempt,
            "failed",
            (err as Error).message
          );
          const durationMs = Date.now() - busyStart;
          metrics.stepDurationSeconds.observe(durationMs / 1000);
          log({
            event: "step_execution_finished",
            workflowId,
            stepId,
            attempt,
            workerId,
            durationMs,
            result: "failed",
            error: (err as Error).message,
          });
          this.onTaskBusy?.(durationMs);
          await this.leases.release(workflowId, stepId, workerId);
          await this.coordinator.handleStepResult(workflowId, stepId, false);
        }
      });
      this.consumers.push(consumer);
    }
  }

  async stop(): Promise<void> {
    await Promise.all(this.consumers.map((c) => c.close()));
    this.consumers = [];
    await this.leases.close();
  }
}
