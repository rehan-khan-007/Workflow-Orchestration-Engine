export interface Workflow {
  id: string;
  name: string;
  steps: Step[];
  status: WorkflowStatus;
}

export type WorkflowStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export interface Step {
  id: string;
  dependsOn: string[];
  status: StepStatus;
  /** Optional: if the executor doesn't resolve within this many ms, the
   *  attempt is treated as a failure. See worker/runner.ts's withTimeout
   *  for the (best-effort — Node can't truly cancel an in-flight
   *  Promise) implementation and its documented limitation. */
  timeoutMs?: number;
}

export type StepStatus = "pending" | "queued" | "retrying" | "running" | "completed" | "failed";

