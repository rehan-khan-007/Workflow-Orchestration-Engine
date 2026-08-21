import { Step } from "../types";

export type StepExecutor = (step: Step) => Promise<void>;

/**
 * Default step executor. In production this is where real task logic
 * (an HTTP call, a script, a container run, etc.) would go. Kept as a
 * simulated delay for now since actual task execution is outside this
 * engine's scope — the engine's job is scheduling/dispatch, not defining
 * what a "step" does.
 */
export const defaultStepExecutor: StepExecutor = async () => {
  await new Promise((resolve) => setTimeout(resolve, 100 + Math.random() * 300));
};
