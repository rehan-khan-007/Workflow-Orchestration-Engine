export interface Workflow {
  id: string;
  name: string;
  steps: Step[];
  status: WorkflowStatus;
}

export type WorkflowStatus = "pending" | "running" | "completed" | "failed";

export interface Step {
  id: string;
  dependsOn: string[];
  status: StepStatus;
}

export type StepStatus = "pending" | "running" | "completed" | "failed";

