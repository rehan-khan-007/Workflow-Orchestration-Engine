import { Workflow, Step } from "../types";
import { WorkflowRepository } from "../storage/workflowRepository";
import { QueueProducer } from "../queue/producer";

/**
 * Owns the DAG-aware dispatch logic: given a workflow's current state,
 * decides which steps are runnable right now, and reacts to a step's
 * completion by re-checking the DAG for newly-unblocked steps.
 *
 * This is what actually replaces the old scheduler's sequential
 * "await each step in topo order" loop with real concurrent dispatch —
 * multiple independent steps get enqueued together and picked up by
 * whichever worker is free.
 */
export class DagCoordinator {
  constructor(
    private repo: WorkflowRepository,
    private producer: QueueProducer
  ) {}

  private isReady(step: Step, all: Step[]): boolean {
    if (step.status !== "pending") return false;
    return step.dependsOn.every(
      (depId) => all.find((s) => s.id === depId)?.status === "completed"
    );
  }

  /** Kicks off a workflow: marks it running and enqueues every step with no unmet dependencies. */
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
      await this.repo.updateStepStatus(workflow.id, step.id, "running");
      await this.producer.enqueue({ workflowId: workflow.id, stepId: step.id });
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
      await this.repo.updateStepStatus(workflowId, step.id, "running");
      await this.producer.enqueue({ workflowId, stepId: step.id });
    }
  }
}
