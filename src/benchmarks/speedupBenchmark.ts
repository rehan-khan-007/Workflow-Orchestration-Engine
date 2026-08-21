import { Step } from "../types";
import { WorkloadSpec } from "./dagGenerator";

/**
 * Topologically sorts a workflow's steps, then executes them strictly
 * one at a time — the same shape as the original (pre-Phase-2) scheduler,
 * which awaited each step in order regardless of which ones were
 * actually independent. This is the honest "no concurrency" baseline
 * speedup is measured against.
 */
async function runSequential(
  workloads: WorkloadSpec[],
  taskDurationMs: () => number
): Promise<number> {
  const start = Date.now();

  for (const workload of workloads) {
    const completed = new Set<string>();
    const remaining = [...workload.steps];

    while (remaining.length > 0) {
      const nextIndex = remaining.findIndex((s) =>
        s.dependsOn.every((dep) => completed.has(dep))
      );
      if (nextIndex === -1) throw new Error("Cycle or malformed DAG in benchmark workload");
      const [step] = remaining.splice(nextIndex, 1);
      await new Promise((r) => setTimeout(r, taskDurationMs()));
      completed.add(step.id);
    }
  }

  return Date.now() - start;
}

export interface SpeedupResult {
  sequentialMs: number;
  parallelMs: number;
  speedupFactor: number;
  latencyReductionPct: number;
}

export async function runSpeedupComparison(
  workloads: WorkloadSpec[],
  taskDurationMs: () => number,
  parallelMs: number
): Promise<SpeedupResult> {
  const sequentialMs = await runSequential(workloads, taskDurationMs);
  const speedupFactor = sequentialMs / parallelMs;
  const latencyReductionPct = (1 - parallelMs / sequentialMs) * 100;

  return { sequentialMs, parallelMs, speedupFactor, latencyReductionPct };
}
