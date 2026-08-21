import { Workflow, WorkflowStatus } from "../types";
import { WorkflowRepository } from "../storage/workflowRepository";

export class WorkflowEngine {
  constructor(private repo: WorkflowRepository = new WorkflowRepository()) {}

  async createWorkflow(name: string, steps: Workflow["steps"]): Promise<Workflow> {
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
