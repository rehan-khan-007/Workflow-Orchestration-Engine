import { Workflow, Step, WorkflowStatus } from "../types";
import { WorkerPool } from "../worker/pool";

export class DagScheduler {
  private workerPool: WorkerPool;

  constructor(workerCount: number = 4) {
    this.workerPool = new WorkerPool(workerCount);
  }

  async schedule(workflow: Workflow): Promise<void> {
    const sorted = this.topologicalSort(workflow.steps);
    workflow.status = "running";

    for (const step of sorted) {
      const depsMet = step.dependsOn.every((depId) =>
        workflow.steps.find((s) => s.id === depId)?.status === "completed"
      );
      if (!depsMet) {
        workflow.status = "failed";
        return;
      }
      await this.workerPool.executeStep(step, workflow);
    }

    workflow.status = "completed";
  }

  private topologicalSort(steps: Step[]): Step[] {
    const visited = new Set<string>();
    const sorted: Step[] = [];

    function dfs(step: Step, all: Map<string, Step>) {
      if (visited.has(step.id)) return;
      visited.add(step.id);
      for (const depId of step.dependsOn) {
        const dep = all.get(depId);
        if (dep) dfs(dep, all);
      }
      sorted.push(step);
    }

    const map = new Map(steps.map((s) => [s.id, s]));
    for (const step of steps) dfs(step, map);
    return sorted;
  }
}