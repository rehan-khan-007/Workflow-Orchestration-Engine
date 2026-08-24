import { writeFileSync } from "fs";
import { join } from "path";
import { runThroughputBenchmark, ThroughputResult } from "./throughputBenchmark";
import { closePool } from "../storage/db";

const WORKER_COUNTS = [1, 2, 4, 8];
const WORKFLOW_COUNT = 60; // fixed workload size — only worker count varies between runs
const TASK_DURATION = () => 100 + Math.random() * 200; // same 100-300ms simulated task as the main benchmark

/**
 * Runs the exact same workload (fixed workflow count, fixed task
 * duration distribution) through the real system multiple times, only
 * varying the number of workers — 1, 2, 4, 8 — so the resulting
 * throughput/latency numbers isolate the effect of worker count from
 * everything else. This is what actually demonstrates scaling behavior,
 * as opposed to a single throughput number at one fixed worker count.
 */
async function runScalingExperiment(): Promise<ThroughputResult[]> {
  const results: ThroughputResult[] = [];

  for (const workerCount of WORKER_COUNTS) {
    console.log(`Running with ${workerCount} worker(s)...`);
    const result = await runThroughputBenchmark(WORKFLOW_COUNT, workerCount, TASK_DURATION);
    results.push(result);
    console.log(
      `  ${result.stepsPerMinute.toFixed(0)} steps/min, ` +
        `p50=${result.p50QueueLatencyMs.toFixed(0)}ms p95=${result.p95QueueLatencyMs.toFixed(0)}ms p99=${result.p99QueueLatencyMs.toFixed(0)}ms, ` +
        `utilization=${result.workerUtilizationPct.toFixed(1)}%`
    );
  }

  return results;
}

async function main(): Promise<void> {
  console.log(`Worker Scaling Experiment`);
  console.log(`Fixed workload: ${WORKFLOW_COUNT} workflows, task duration 100-300ms`);
  console.log(`Varying worker count: ${WORKER_COUNTS.join(", ")}\n`);

  const results = await runScalingExperiment();

  console.log(`\n${"Workers".padEnd(8)}${"Throughput/min".padEnd(16)}${"p50".padEnd(8)}${"p95".padEnd(8)}${"p99".padEnd(8)}${"Utilization"}`);
  for (const r of results) {
    console.log(
      `${String(r.workerCount).padEnd(8)}${r.stepsPerMinute.toFixed(0).padEnd(16)}` +
        `${(r.p50QueueLatencyMs.toFixed(0) + "ms").padEnd(8)}${(r.p95QueueLatencyMs.toFixed(0) + "ms").padEnd(8)}${(r.p99QueueLatencyMs.toFixed(0) + "ms").padEnd(8)}` +
        `${r.workerUtilizationPct.toFixed(1)}%`
    );
  }

  const outPath = join(__dirname, "scaling-results.json");
  writeFileSync(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2));
  console.log(`\nFull results written to ${outPath}`);

  await closePool();
}

main().catch((err) => {
  console.error("Scaling experiment failed:", err);
  process.exitCode = 1;
});
