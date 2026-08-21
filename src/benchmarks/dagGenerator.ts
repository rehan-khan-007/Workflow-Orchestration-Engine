import { Step } from "../types";

/**
 * Generates a synthetic layered DAG: `layers` layers of `stepsPerLayer`
 * steps each, where every step in layer N depends on every step in layer
 * N-1. This creates realistic fan-out/fan-in structure — each layer's
 * steps are independent of each other (genuine parallelism opportunity),
 * while layers themselves are strictly ordered.
 */
export function generateLayeredWorkflow(
  layers: number,
  stepsPerLayer: number
): Step[] {
  const steps: Step[] = [];
  let previousLayerIds: string[] = [];

  for (let layer = 0; layer < layers; layer++) {
    const currentLayerIds: string[] = [];
    for (let i = 0; i < stepsPerLayer; i++) {
      const id = `l${layer}-s${i}`;
      steps.push({
        id,
        dependsOn: [...previousLayerIds],
        status: "pending",
      });
      currentLayerIds.push(id);
    }
    previousLayerIds = currentLayerIds;
  }

  return steps;
}

export interface WorkloadSpec {
  name: string;
  steps: Step[];
}

/**
 * Generates `count` workflows with randomized (but seeded-range) layer
 * counts and widths, so the benchmark isn't just running the same shape
 * over and over. Returns the workloads plus the total step count across
 * all of them, so callers can confirm they've hit a target scale.
 */
export function generateWorkloadBatch(
  count: number,
  minLayers = 2,
  maxLayers = 4,
  minWidth = 2,
  maxWidth = 4
): { workloads: WorkloadSpec[]; totalSteps: number } {
  const workloads: WorkloadSpec[] = [];
  let totalSteps = 0;

  for (let i = 0; i < count; i++) {
    const layers = minLayers + Math.floor(Math.random() * (maxLayers - minLayers + 1));
    const width = minWidth + Math.floor(Math.random() * (maxWidth - minWidth + 1));
    const steps = generateLayeredWorkflow(layers, width);
    totalSteps += steps.length;
    workloads.push({ name: `bench-wf-${i}`, steps });
  }

  return { workloads, totalSteps };
}
