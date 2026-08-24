import { Step } from "../types";

export interface ValidationResult {
  valid: boolean;
  error?: string;
}

/**
 * Validates a set of steps before a workflow is ever created — catches
 * malformed DAGs at submission time instead of letting them silently
 * fail (or hang forever) once dispatch actually starts.
 */
export function validateDag(steps: Step[]): ValidationResult {
  const seenIds = new Set<string>();
  for (const step of steps) {
    if (seenIds.has(step.id)) {
      return { valid: false, error: `Duplicate step id: "${step.id}"` };
    }
    seenIds.add(step.id);
  }

  for (const step of steps) {
    if (step.dependsOn.includes(step.id)) {
      return { valid: false, error: `Step "${step.id}" depends on itself` };
    }
    for (const dep of step.dependsOn) {
      if (!seenIds.has(dep)) {
        return {
          valid: false,
          error: `Step "${step.id}" depends on unknown step "${dep}"`,
        };
      }
    }
  }

  const cycleError = detectCycle(steps);
  if (cycleError) return { valid: false, error: cycleError };

  return { valid: true };
}

function detectCycle(steps: Step[]): string | undefined {
  const byId = new Map(steps.map((s) => [s.id, s]));
  const visiting = new Set<string>();
  const visited = new Set<string>();

  function visit(id: string, path: string[]): string | undefined {
    if (visited.has(id)) return undefined;
    if (visiting.has(id)) {
      return `Dependency cycle detected: ${[...path, id].join(" -> ")}`;
    }
    visiting.add(id);
    const step = byId.get(id);
    if (step) {
      for (const dep of step.dependsOn) {
        const err = visit(dep, [...path, id]);
        if (err) return err;
      }
    }
    visiting.delete(id);
    visited.add(id);
    return undefined;
  }

  for (const step of steps) {
    const err = visit(step.id, []);
    if (err) return err;
  }
  return undefined;
}
