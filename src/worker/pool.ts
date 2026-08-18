import { EventEmitter } from "events";
import { Workflow, WorkflowStatus, Step } from "../types";

export class WorkerPool extends EventEmitter {
  private workers: Map<string, { busy: boolean; id: string }> = new Map();
  private maxWorkers: number;

  constructor(maxWorkers: number = 4) {
    super();
    this.maxWorkers = maxWorkers;
    for (let i = 0; i < maxWorkers; i++) {
      const id = `worker-${i + 1}`;
      this.workers.set(id, { busy: false, id });
    }
  }

  async executeStep(step: Step, workflow: Workflow): Promise<void> {
    const worker = this.assignWorker();
    if (!worker) {
      this.emit("queue_full", { step, workflow });
      return;
    }

    this.emit("step_started", { step, worker: worker.id, workflow: workflow.id });
    worker.busy = true;

    try {
      await this.runStep(step);
      worker.busy = false;
      this.emit("step_completed", { step, worker: worker.id });
    } catch (err) {
      worker.busy = false;
      this.emit("step_failed", { step, worker: worker.id, error: (err as Error).message });
    }
  }

  private async runStep(step: Step): Promise<void> {
    // Simulate step execution — in production runs actual task logic
    await new Promise((resolve) => setTimeout(resolve, Math.random() * 1000));
  }

  private assignWorker(): { busy: boolean; id: string } | null {
    for (const worker of this.workers.values()) {
      if (!worker.busy) return worker;
    }
    return null;
  }

  availableCount(): number {
    return Array.from(this.workers.values()).filter((w) => !w.busy).length;
  }

  totalCount(): number {
    return this.workers.size;
  }
}