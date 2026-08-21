import { Workflow, Step } from "../types";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";

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
    private maxAttempts: number = DEFAULT_MAX_ATTEMPTS
  ) {}

  private isReady(step: Step, all: Step[]): boolean {
    if (step.status !== "pending") return false;
    return step.dependsOn.every(
      (depId) => all.find((s) => s.id === depId)?.status === "completed"
    );
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
      await this.repo.updateWorkflowStatus(workflowId, "failed");
      return;
    }
    await this.repo.updateStepStatus(workflowId, stepId, "running");
    await this.producer.enqueue({ workflowId, stepId, attempt });
  }

  /** Kicks off a workflow: marks it running and dispatches every step with no unmet dependencies. */
  async start(workflow: Workflow): Promise<void> {
    if (workflow.steps.length === 0) {
      await this.repo.updateWorkflowStatus(workflow.id, "completed");
      return;
    }

    await this.repo.updateWorkflowStatus(workflow.id, "running");

    const ready = workflow.steps.filter((s) => this.isReady(s, workflow.steps));
    if (ready.length === 0) {
      // No zero-dependency steps to start from — the DAG can never progress
      // (most likely a dependency cycle). Fail fast rather than hang forever.
      await this.repo.updateWorkflowStatus(workflow.id, "failed");
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

    const workflow = await this.repo.getWorkflow(workflowId);
    if (!workflow) return;

    const anyFailed = workflow.steps.some((s) => s.status === "failed");
    if (anyFailed) {
      await this.repo.updateWorkflowStatus(workflowId, "failed");
      return;
    }

    const stillOutstanding = workflow.steps.some(
      (s) => s.status === "pending" || s.status === "running"
    );
    if (!stillOutstanding) {
      await this.repo.updateWorkflowStatus(workflowId, "completed");
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
    await this.dispatchStep(workflowId, stepId);
  }
}
