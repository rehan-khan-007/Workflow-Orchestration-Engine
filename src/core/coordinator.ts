import { Workflow, Step } from "../types";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { EventBus } from "../queue/eventBus";
import * as metrics from "../observability/metrics";

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BASE_BACKOFF_MS = 200;
const DEFAULT_MAX_BACKOFF_MS = 5000;

/**
 * Owns the DAG-aware dispatch logic: given a workflow's current state,
 * decides which steps are runnable right now, and reacts to a step's
 * completion by re-checking the DAG for newly-unblocked steps.
 *
 * Also owns retry/attempt-limit logic, since a retry (triggered by the
 * reaper after a worker crash) and a first dispatch are the same
 * operation — "put this step on the queue" — just with a different
 * trigger and a check against how many times it's already been tried.
 * A retry (attempt > 1) waits an exponentially-increasing backoff delay
 * before actually re-enqueueing, so a struggling downstream dependency
 * isn't immediately hammered again.
 *
 * Note on scope: backoff/retry applies to crash-detected abandonment
 * (the reaper's retryStep), not to a step whose own executor threw an
 * error — see handleStepResult. Those are treated as immediate permanent
 * failures deliberately: a worker crash is a transient infrastructure
 * problem worth retrying, but a task's own logic failing partway through
 * might mean it already had a side effect, and blindly retrying risks
 * duplicating it.
 */
export class DagCoordinator {
  constructor(
    private repo: WorkflowRepository,
    private producer: QueueProducer,
    private maxAttempts: number = DEFAULT_MAX_ATTEMPTS,
    private eventBus?: EventBus,
    private baseBackoffMs: number = DEFAULT_BASE_BACKOFF_MS,
    private maxBackoffMs: number = DEFAULT_MAX_BACKOFF_MS
  ) {}

  private isReady(step: Step, all: Step[]): boolean {
    if (step.status !== "pending") return false;
    return step.dependsOn.every(
      (depId) => all.find((s) => s.id === depId)?.status === "completed"
    );
  }

  private async publishStep(workflowId: string, stepId: string, status: string): Promise<void> {
    if (!this.eventBus) return;
    await this.eventBus.publish({
      type: "step_status",
      workflowId,
      stepId,
      status,
      at: new Date().toISOString(),
    });
  }

  private async publishWorkflow(workflowId: string, status: string): Promise<void> {
    if (!this.eventBus) return;
    await this.eventBus.publish({
      type: "workflow_status",
      workflowId,
      status,
      at: new Date().toISOString(),
    });
  }

  private async setWorkflowStatus(workflowId: string, status: Workflow["status"]): Promise<void> {
    await this.repo.updateWorkflowStatus(workflowId, status);
    await this.publishWorkflow(workflowId, status);

    if (status === "completed" || status === "failed" || status === "cancelled") {
      if (status === "completed") metrics.workflowCompletedTotal.inc();
      if (status === "failed") metrics.workflowFailedTotal.inc();
      if (status === "cancelled") metrics.workflowCancelledTotal.inc();

      const createdAt = await this.repo.getWorkflowCreatedAt(workflowId);
      if (createdAt) {
        const seconds = (Date.now() - new Date(createdAt).getTime()) / 1000;
        metrics.workflowDurationSeconds.observe(seconds);
      }
    }
  }

  /** Exponential backoff: 0 for a first attempt, doubling from baseBackoffMs on each retry, capped at maxBackoffMs. */
  private computeBackoffMs(attempt: number): number {
    if (attempt <= 1) return 0;
    return Math.min(this.baseBackoffMs * 2 ** (attempt - 2), this.maxBackoffMs);
  }

  /**
   * Dispatches a single step: increments its attempt counter, and either
   * permanently fails it (attempts exhausted — also recorded to the
   * dead-letter table), waits out a backoff delay before re-enqueueing
   * (a retry), or enqueues immediately (first attempt). `stepHint`, when
   * the caller already has the Step object in hand (start/handleStepResult
   * do), avoids an extra DB round trip on the common first-dispatch path.
   */
  private async dispatchStep(workflowId: string, stepId: string, stepHint?: Step): Promise<void> {
    const attempt = await this.repo.incrementAttempt(workflowId, stepId);
    if (attempt > this.maxAttempts) {
      const lastError = await this.repo.getLastError(workflowId, stepId);
      await this.repo.recordDeadLetter(workflowId, stepId, attempt - 1, lastError);
      metrics.deadLetterTotal.inc();
      metrics.stepFailedTotal.inc();
      await this.repo.updateStepStatus(workflowId, stepId, "failed");
      await this.publishStep(workflowId, stepId, "failed");
      await this.setWorkflowStatus(workflowId, "failed");
      return;
    }

    const delayMs = this.computeBackoffMs(attempt);
    if (delayMs > 0) {
      // Known limitation, deliberately out of scope for now: this delay
      // is an in-process setTimeout. If the coordinator's process
      // crashes while a step is waiting here, the timer is lost and the
      // step stays stuck in "retrying" forever. The reaper does NOT
      // cover this — its grace period (a few seconds) can be shorter
      // than maxBackoffMs, so having it scan "retrying" steps risks
      // prematurely double-retrying a step that's still legitimately
      // waiting. A real fix needs a persisted "retry due at" time and a
      // separate poller, which is a bigger feature than this pass.
      await this.repo.updateStepStatus(workflowId, stepId, "retrying");
      await this.publishStep(workflowId, stepId, "retrying");
      setTimeout(() => {
        this.enqueueStep(workflowId, stepId, attempt).catch(() => {});
      }, delayMs);
      return;
    }

    await this.enqueueStep(workflowId, stepId, attempt, stepHint);
  }

  /** Actually puts a step on the Redis queue — called immediately for a first dispatch, or after a backoff wait for a retry. */
  private async enqueueStep(
    workflowId: string,
    stepId: string,
    attempt: number,
    stepHint?: Step
  ): Promise<void> {
    let timeoutMs = stepHint?.timeoutMs;
    if (stepHint === undefined) {
      // No hint (the backoff-delayed path) — re-fetch, which also
      // re-checks cancellation: time has passed since dispatchStep was
      // first called, and a cancel could have happened during the wait.
      const workflow = await this.repo.getWorkflow(workflowId);
      if (workflow?.status === "cancelled") return;
      timeoutMs = workflow?.steps.find((s) => s.id === stepId)?.timeoutMs;
    }

    await this.repo.updateStepStatus(workflowId, stepId, "queued");
    await this.publishStep(workflowId, stepId, "queued");
    metrics.stepDispatchedTotal.inc();
    if (attempt > 1) metrics.stepRetryTotal.inc();
    // dispatchedAt lets a worker (or a benchmark) compute exact queue
    // wait time at pickup, without relying on any DB column — steps.updated_at
    // gets overwritten again on completion, so it can't be used for this.
    await this.producer.enqueue({ workflowId, stepId, attempt, dispatchedAt: Date.now(), timeoutMs });
  }

  /**
   * Called by a worker the instant it actually starts executing a step
   * (lease acquired, execution attempt recorded) — transitions the step
   * from "queued" (dispatched, waiting) to "running" (actively being
   * worked on).
   */
  async markRunning(workflowId: string, stepId: string): Promise<void> {
    await this.repo.updateStepStatus(workflowId, stepId, "running");
    await this.publishStep(workflowId, stepId, "running");
  }

  /** Kicks off a workflow: marks it running and dispatches every step with no unmet dependencies. */
  async start(workflow: Workflow): Promise<void> {
    metrics.workflowStartedTotal.inc();

    if (workflow.steps.length === 0) {
      await this.setWorkflowStatus(workflow.id, "completed");
      return;
    }

    await this.setWorkflowStatus(workflow.id, "running");

    const ready = workflow.steps.filter((s) => this.isReady(s, workflow.steps));
    if (ready.length === 0) {
      // No zero-dependency steps to start from — the DAG can never progress
      // (most likely a dependency cycle). Fail fast rather than hang forever.
      await this.setWorkflowStatus(workflow.id, "failed");
      return;
    }

    for (const step of ready) {
      await this.dispatchStep(workflow.id, step.id, step);
    }
  }

  /** Called by a worker after it finishes executing a step, success or failure. */
  async handleStepResult(
    workflowId: string,
    stepId: string,
    success: boolean
  ): Promise<void> {
    await this.repo.updateStepStatus(
      workflowId,
      stepId,
      success ? "completed" : "failed"
    );
    await this.publishStep(workflowId, stepId, success ? "completed" : "failed");
    if (success) {
      metrics.stepCompletedTotal.inc();
    } else {
      metrics.stepFailedTotal.inc();
    }

    if (!success) {
      // A task's own logic failing (as opposed to a crash — that path
      // is retried via retryStep/dispatchStep) is immediately permanent,
      // so it needs its own dead-letter record here rather than relying
      // on dispatchStep's attempts-exhausted branch, which this path
      // never goes through.
      const [lastError, attemptCount] = await Promise.all([
        this.repo.getLastError(workflowId, stepId),
        this.repo.getAttemptCount(workflowId, stepId),
      ]);
      await this.repo.recordDeadLetter(workflowId, stepId, attemptCount, lastError);
      metrics.deadLetterTotal.inc();
    }

    const workflow = await this.repo.getWorkflow(workflowId);
    if (!workflow) return;

    // A cancelled workflow shouldn't come back to life just because an
    // in-flight step (started before the cancel) eventually reports in.
    if (workflow.status === "cancelled") return;

    const anyFailed = workflow.steps.some((s) => s.status === "failed");
    if (anyFailed) {
      await this.setWorkflowStatus(workflowId, "failed");
      return;
    }

    const stillOutstanding = workflow.steps.some(
      (s) =>
        s.status === "pending" ||
        s.status === "queued" ||
        s.status === "retrying" ||
        s.status === "running"
    );
    if (!stillOutstanding) {
      await this.setWorkflowStatus(workflowId, "completed");
      return;
    }

    const newlyReady = workflow.steps.filter((s) => this.isReady(s, workflow.steps));
    for (const step of newlyReady) {
      await this.dispatchStep(workflowId, step.id, step);
    }
  }

  /**
   * Called by the reaper when it detects a step marked "queued" or
   * "running" whose worker lease has expired — i.e. the worker that
   * claimed it is presumed dead. Re-dispatches through the same
   * attempt-limited, backoff-delayed path as a first dispatch.
   */
  async retryStep(workflowId: string, stepId: string): Promise<void> {
    const workflow = await this.repo.getWorkflow(workflowId);
    if (workflow?.status === "cancelled") return;
    const step = workflow?.steps.find((s) => s.id === stepId);
    await this.dispatchStep(workflowId, stepId, step);
  }

  /**
   * Marks a workflow cancelled. Steps already in flight are not forcibly
   * stopped (a worker mid-execution has no way to be interrupted from
   * outside), but no further steps will be dispatched once this is set —
   * handleStepResult checks for cancellation before continuing the DAG,
   * and a step waiting out a backoff delay re-checks at the end of it.
   */
  async cancel(workflowId: string): Promise<void> {
    const workflow = await this.repo.getWorkflow(workflowId);
    if (!workflow) return;
    if (workflow.status === "completed" || workflow.status === "failed") return;
    await this.setWorkflowStatus(workflowId, "cancelled");
  }
}
