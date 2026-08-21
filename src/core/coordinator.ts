import { Workflow, Step } from "../types";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";
import { EventBus } from "../queue/eventBus";

const DEFAULT_MAX_ATTEMPTS = 3;

/**
 * Owns the DAG-aware dispatch logic: given a workflow's current state,
 * decides which steps are runnable right now, and reacts to a step's
 * completion by re-checking the DAG for newly-unblocked steps.
 *
 * Also owns retry/attempt-limit logic, since a retry (triggered by the
 * reaper after a worker crash) and a first dispatch are the same
 * operation — "put this step on the queue" — just with a different
 * trigger and a check against how many times it's already been tried.
 */
export class DagCoordinator {
  constructor(
    private repo: WorkflowRepository,
    private producer: QueueProducer,
    private maxAttempts: number = DEFAULT_MAX_ATTEMPTS,
    private eventBus?: EventBus
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
  }

  /**
   * Dispatches a single step: increments its attempt counter, and either
   * enqueues it (attempts remaining) or permanently fails it and the
   * workflow (attempts exhausted). Used for both first dispatch and
   * reaper-triggered retries — they're the same operation.
   */
  private async dispatchStep(workflowId: string, stepId: string): Promise<void> {
    const attempt = await this.repo.incrementAttempt(workflowId, stepId);
    if (attempt > this.maxAttempts) {
      await this.repo.updateStepStatus(workflowId, stepId, "failed");
      await this.publishStep(workflowId, stepId, "failed");
      await this.setWorkflowStatus(workflowId, "failed");
      return;
    }
    await this.repo.updateStepStatus(workflowId, stepId, "running");
    await this.publishStep(workflowId, stepId, "running");
    // dispatchedAt lets a worker (or a benchmark) compute exact queue
    // wait time at pickup, without relying on any DB column — steps.updated_at
    // gets overwritten again on completion, so it can't be used for this.
    await this.producer.enqueue({ workflowId, stepId, attempt, dispatchedAt: Date.now() });
  }

  /** Kicks off a workflow: marks it running and dispatches every step with no unmet dependencies. */
  async start(workflow: Workflow): Promise<void> {
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
      await this.dispatchStep(workflow.id, step.id);
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
      (s) => s.status === "pending" || s.status === "running"
    );
    if (!stillOutstanding) {
      await this.setWorkflowStatus(workflowId, "completed");
      return;
    }

    const newlyReady = workflow.steps.filter((s) => this.isReady(s, workflow.steps));
    for (const step of newlyReady) {
      await this.dispatchStep(workflowId, step.id);
    }
  }

  /**
   * Called by the reaper when it detects a step marked "running" whose
   * worker lease has expired — i.e. the worker that claimed it is
   * presumed dead. Re-dispatches through the same attempt-limited path
   * as a first dispatch.
   */
  async retryStep(workflowId: string, stepId: string): Promise<void> {
    const workflow = await this.repo.getWorkflow(workflowId);
    if (workflow?.status === "cancelled") return;
    await this.dispatchStep(workflowId, stepId);
  }

  /**
   * Marks a workflow cancelled. Steps already in flight are not forcibly
   * stopped (a worker mid-execution has no way to be interrupted from
   * outside), but no further steps will be dispatched once this is set —
   * handleStepResult checks for cancellation before continuing the DAG.
   */
  async cancel(workflowId: string): Promise<void> {
    const workflow = await this.repo.getWorkflow(workflowId);
    if (!workflow) return;
    if (workflow.status === "completed" || workflow.status === "failed") return;
    await this.setWorkflowStatus(workflowId, "cancelled");
  }
}
