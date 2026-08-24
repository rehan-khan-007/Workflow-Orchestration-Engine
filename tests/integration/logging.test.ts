import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkerPool } from "../../src/worker/pool";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { getPool, closePool } from "../../src/storage/db";

/**
 * log() is suppressed under NODE_ENV=test (see logger.ts) to keep the
 * rest of the suite's output clean. This test deliberately overrides
 * NODE_ENV for its own duration to prove the real end-to-end trace
 * actually works — the same workflowId genuinely threading through
 * every stage: dispatch -> a specific worker's execution -> completion
 * -> the workflow itself finishing. That's the actual claim being
 * tested, not just that individual log calls produce valid JSON (the
 * logger unit tests already cover that in isolation).
 */
describe("Structured logging: real end-to-end trace (Redis + Postgres backed)", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-logging-${Date.now()}`;
  let producer: QueueProducer;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let pool: WorkerPool;
  let consoleSpy: jest.SpyInstance;
  const originalNodeEnv = process.env.NODE_ENV;

  beforeAll(() => {
    producer = new QueueProducer(queueName);
    coordinator = new DagCoordinator(repo, producer, 3);
    scheduler = new DagScheduler(coordinator);
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      await new Promise((r) => setTimeout(r, 100));
    });
    pool.start();
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
    process.env.NODE_ENV = "development"; // un-suppress log() for this test only
    consoleSpy = jest.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
    process.env.NODE_ENV = originalNodeEnv;
  });

  afterAll(async () => {
    await pool.stop();
    await producer.close();
    await closePool();
  });

  function loggedEvents(): any[] {
    return consoleSpy.mock.calls
      .map((call) => {
        try {
          return JSON.parse(call[0]);
        } catch {
          return null;
        }
      })
      .filter(Boolean);
  }

  it("traces a single workflowId through dispatch, worker execution, and completion", async () => {
    const wf = await engine.createWorkflow("logging-trace-test", [
      { id: "only-step", dependsOn: [], status: "pending" },
    ]);
    await scheduler.schedule(wf);

    const deadline = Date.now() + 5000;
    let finished;
    while (Date.now() < deadline) {
      finished = await repo.getWorkflow(wf.id);
      if (finished!.status === "completed") break;
      await new Promise((r) => setTimeout(r, 50));
    }
    expect(finished!.status).toBe("completed");

    const events = loggedEvents().filter((e) => e.workflowId === wf.id);
    const eventNames = events.map((e) => e.event);

    // The full chain, in the order this system actually produces it.
    expect(eventNames).toContain("workflow_started");
    expect(eventNames).toContain("step_dispatched");
    expect(eventNames).toContain("step_execution_started");
    expect(eventNames).toContain("step_execution_finished");
    expect(eventNames).toContain("step_completed");
    expect(eventNames).toContain("workflow_completed");

    // Every step-level event refers to the same step and carries a
    // workerId — proving the "attempt -> worker" part of the trace, not
    // just "some event happened for this workflow".
    const startedEvent = events.find((e) => e.event === "step_execution_started");
    const finishedEvent = events.find((e) => e.event === "step_execution_finished");
    expect(startedEvent.stepId).toBe("only-step");
    expect(finishedEvent.stepId).toBe("only-step");
    expect(startedEvent.workerId).toBeDefined();
    expect(finishedEvent.workerId).toBe(startedEvent.workerId);
    expect(finishedEvent.durationMs).toBeGreaterThanOrEqual(0);
    expect(finishedEvent.result).toBe("completed");

    // workflow_completed carries a duration too, computed from the
    // workflow's actual creation time, not a placeholder.
    const workflowCompletedEvent = events.find((e) => e.event === "workflow_completed");
    expect(workflowCompletedEvent.durationMs).toBeGreaterThanOrEqual(0);
  }, 10000);

  it("a dead-lettered step's failure is traceable with its error message attached", async () => {
    // Dedicated queue + coordinator, separate from the shared healthy
    // pool above — otherwise both pools race for the same queue item,
    // and the healthy pool can "steal" and succeed a step this test
    // specifically needs to fail.
    const failQueueName = `test-logging-fail-${Date.now()}`;
    const failProducer = new QueueProducer(failQueueName);
    const failCoordinator = new DagCoordinator(repo, failProducer, 3);
    const failScheduler = new DagScheduler(failCoordinator);
    const failingPool = new WorkerPool(failQueueName, failCoordinator, repo, 1, async () => {
      throw new Error("simulated task failure for logging trace");
    });
    failingPool.start();

    const wf = await engine.createWorkflow("logging-failure-trace", [
      { id: "always-fails", dependsOn: [], status: "pending" },
    ]);
    await failScheduler.schedule(wf);

    const deadline = Date.now() + 5000;
    let finished;
    while (Date.now() < deadline) {
      finished = await repo.getWorkflow(wf.id);
      if (finished!.status === "failed") break;
      await new Promise((r) => setTimeout(r, 50));
    }
    await failingPool.stop();
    await failProducer.close();
    expect(finished!.status).toBe("failed");

    const events = loggedEvents().filter((e) => e.workflowId === wf.id);
    const deadLetterEvent = events.find((e) => e.event === "dead_letter_recorded");
    expect(deadLetterEvent).toBeDefined();
    expect(deadLetterEvent.stepId).toBe("always-fails");
    expect(deadLetterEvent.error).toMatch(/simulated task failure/);
  }, 10000);
});
