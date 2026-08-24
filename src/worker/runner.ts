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

export class StepTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Step timed out after ${timeoutMs}ms`);
    this.name = "StepTimeoutError";
  }
}

/**
 * Races a step's execution against a timeout. If timeoutMs is undefined,
 * runs the executor with no time limit at all.
 *
 * Important limitation: this is a best-effort orchestration-level
 * timeout, not a true cancellation. Node.js has no general mechanism to
 * abort an arbitrary in-flight Promise — if the timeout wins the race,
 * the orchestrator treats the step as failed and moves on, but the
 * original executor call keeps running in the background until it
 * settles on its own. For a real cancellation, the executor itself would
 * need to accept and honor an AbortSignal (e.g. passing one through to
 * `fetch`) — that's a property of individual step implementations, not
 * something this wrapper can impose from the outside.
 */
export async function withTimeout(promise: Promise<void>, timeoutMs?: number): Promise<void> {
  if (!timeoutMs) return promise;
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new StepTimeoutError(timeoutMs)), timeoutMs);
  });
  try {
    await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}
