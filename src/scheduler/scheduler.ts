import { Workflow } from "../types";
import { DagCoordinator } from "../core/coordinator";

/**
 * Entry point for kicking off a workflow's execution. The real DAG logic
 * (deciding what's ready, reacting to completions) lives in DagCoordinator
 * since that logic is also invoked by workers as steps complete — this
 * class just exposes the "start a workflow" operation.
 */
export class DagScheduler {
  constructor(private coordinator: DagCoordinator) {}

  async schedule(workflow: Workflow): Promise<void> {
    await this.coordinator.start(workflow);
  }
}
