import { WorkflowRepository } from "../storage/workflowRepository";
import { LeaseManager } from "./leaseManager";
import { DagCoordinator } from "../core/coordinator";

const DEFAULT_INTERVAL_MS = 1000;
const DEFAULT_MIN_AGE_MS = 4000;

/**
 * Periodically scans for steps Postgres says are "running" and checks
 * whether their Redis lease still exists. A step whose lease is gone but
 * that never reported completion means its worker died mid-execution —
 * the reaper retries it through the same attempt-limited path a normal
 * dispatch uses.
 *
 * minAgeMs guards against a race: a step is marked "running" in Postgres
 * slightly before the worker that picked it up off the queue has had a
 * chance to actually acquire its lease. Only steps that have been
 * "running" for at least minAgeMs are considered reap candidates, giving
 * that window time to close.
 */
export class Reaper {
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private repo: WorkflowRepository,
    private leases: LeaseManager,
    private coordinator: DagCoordinator,
    private intervalMs: number = DEFAULT_INTERVAL_MS,
    private minAgeMs: number = DEFAULT_MIN_AGE_MS
  ) {}

  start(): void {
    this.timer = setInterval(() => {
      this.sweep().catch(() => {});
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async sweep(): Promise<void> {
    const running = await this.repo.listDispatchedSteps();
    const now = Date.now();

    for (const { workflowId, stepId, updatedAtMs } of running) {
      if (now - updatedAtMs < this.minAgeMs) continue;

      const leaseHeld = await this.leases.exists(workflowId, stepId);
      if (!leaseHeld) {
        await this.coordinator.retryStep(workflowId, stepId);
      }
    }
  }
}
