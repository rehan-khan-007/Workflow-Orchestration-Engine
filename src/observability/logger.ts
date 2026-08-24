/**
 * A structured logger: every call emits one JSON object per line to
 * stdout. No external logging library — at this project's scale, that's
 * genuinely all structured logging means, and every container log
 * aggregator (CloudWatch, Loki, Datadog, `kubectl logs` piped to `jq`)
 * already expects exactly this format with zero extra configuration.
 *
 * This deliberately logs domain events (a step was dispatched, a workflow
 * completed, a retry was triggered), not HTTP access logs — the goal is
 * tracing the workflow -> step -> attempt -> worker -> result chain, not
 * logging every request.
 */
export interface LogFields {
  event: string;
  workflowId?: string;
  stepId?: string;
  attempt?: number;
  workerId?: string;
  durationMs?: number;
  error?: string;
  [key: string]: unknown;
}

/** Pure formatting — split out from log() so it's directly testable regardless of NODE_ENV. */
export function formatLogEntry(fields: LogFields): string {
  return JSON.stringify({ timestamp: new Date().toISOString(), ...fields });
}

export function log(fields: LogFields): void {
  // Jest sets NODE_ENV=test automatically — suppressing here keeps test
  // output readable. Production/dev runs (where NODE_ENV isn't "test")
  // log normally; this never needs touching per-call-site.
  if (process.env.NODE_ENV === "test") return;
  console.log(formatLogEntry(fields));
}
