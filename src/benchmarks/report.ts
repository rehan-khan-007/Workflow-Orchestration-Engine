import { writeFileSync } from "fs";
import { join } from "path";
import { generateWorkloadBatch } from "./dagGenerator";
import { runThroughputBenchmark } from "./throughputBenchmark";
import { runSpeedupComparison } from "./speedupBenchmark";
import { runFailureRecoveryBenchmark } from "./failureRecoveryBenchmark";
import { closePool } from "../storage/db";

const WORKFLOW_COUNT = 120; // targets 500+ total steps across the batch
const WORKER_COUNT = 4; // matches the 4-replica Kubernetes worker deployment
const TASK_DURATION = () => 100 + Math.random() * 200; // 100-300ms simulated task
const FAILURE_INJECTION_COUNT = 20;

async function main(): Promise<void> {
  console.log(`Workflow Orchestration Engine — Benchmark Suite`);
  console.log(`Run started: ${new Date().toISOString()}\n`);

  console.log(`[1/3] Throughput / latency / utilization (${WORKFLOW_COUNT} workflows, ${WORKER_COUNT} workers)...`);
  const throughput = await runThroughputBenchmark(WORKFLOW_COUNT, WORKER_COUNT, TASK_DURATION);
  console.log(`  Total steps executed: ${throughput.totalSteps}`);
  console.log(`  Wall clock: ${(throughput.wallClockMs / 1000).toFixed(1)}s`);
  console.log(`  Throughput: ${throughput.stepsPerMinute.toFixed(1)} steps/minute`);
  console.log(`  Avg queue latency: ${throughput.avgQueueLatencyMs.toFixed(1)}ms`);
  console.log(`  p95 queue latency: ${throughput.p95QueueLatencyMs.toFixed(1)}ms`);
  console.log(`  Worker utilization: ${throughput.workerUtilizationPct.toFixed(1)}%\n`);

  console.log(`[2/3] Parallel vs sequential speedup...`);
  const { workloads } = generateWorkloadBatch(30); // smaller batch — sequential is slow by design
  const speedupThroughput = await runThroughputBenchmark(30, WORKER_COUNT, TASK_DURATION);
  const speedup = await runSpeedupComparison(workloads, TASK_DURATION, speedupThroughput.wallClockMs);
  console.log(`  Sequential: ${(speedup.sequentialMs / 1000).toFixed(1)}s`);
  console.log(`  Parallel:   ${(speedup.parallelMs / 1000).toFixed(1)}s`);
  console.log(`  Speedup: ${speedup.speedupFactor.toFixed(2)}x`);
  console.log(`  Latency reduction: ${speedup.latencyReductionPct.toFixed(1)}%\n`);

  console.log(`[3/3] Failure recovery (${FAILURE_INJECTION_COUNT} injected worker crashes)...`);
  const recovery = await runFailureRecoveryBenchmark(FAILURE_INJECTION_COUNT, 10000);
  console.log(`  Recovered within 10s: ${recovery.recoveredWithinThreshold}/${recovery.injectedFailures} (${recovery.recoveryRatePct.toFixed(1)}%)`);
  console.log(`  Avg recovery time: ${recovery.avgRecoveryMs.toFixed(0)}ms`);
  console.log(`  p95 recovery time: ${recovery.p95RecoveryMs.toFixed(0)}ms\n`);

  const report = {
    generatedAt: new Date().toISOString(),
    throughput,
    speedup: { ...speedup, workloadCount: 30 },
    failureRecovery: recovery,
  };

  const outPath = join(__dirname, "results.json");
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(`Full results written to ${outPath}`);

  await closePool();
}

main().catch((err) => {
  console.error("Benchmark run failed:", err);
  process.exitCode = 1;
});
