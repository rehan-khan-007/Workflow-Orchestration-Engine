import { Workflow, WorkflowStatus } from "../types";
import { WorkflowRepository } from "../storage/workflowRepository";
import { validateDag } from "./validation";

export class WorkflowEngine {
  constructor(private repo: WorkflowRepository = new WorkflowRepository()) {}

  async createWorkflow(name: string, steps: Workflow["steps"]): Promise<Workflow> {
    const validation = validateDag(steps);
    if (!validation.valid) {
      throw new Error(validation.error);
    }

    const workflow: Workflow = {
      id: crypto.randomUUID(),
      name,
      steps: steps.map((s) => ({ ...s, status: s.status ?? "pending" })),
      status: "pending" as WorkflowStatus,
    };
    return this.repo.createWorkflow(workflow);
  }

  async getWorkflow(id: string): Promise<Workflow | undefined> {
    return this.repo.getWorkflow(id);
  }

  async listWorkflows(): Promise<Workflow[]> {
    return this.repo.listWorkflows();
  }
}
