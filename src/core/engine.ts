import { Workflow, WorkflowStatus } from "../types";

export class WorkflowEngine {
  private workflows: Map<string, Workflow> = new Map();

  async createWorkflow(name: string, steps: Workflow["steps"]): Promise<Workflow> {
    const workflow: Workflow = {
      id: crypto.randomUUID(),
      name,
      steps,
      status: "pending" as WorkflowStatus,
    };
    this.workflows.set(workflow.id, workflow);
    return workflow;
  }

  async getWorkflow(id: string): Promise<Workflow | undefined> {
    return this.workflows.get(id);
  }

  async listWorkflows(): Promise<Workflow[]> {
    return Array.from(this.workflows.values());
  }
}