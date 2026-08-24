import { DagCoordinator } from "../../src/core/coordinator";
import { Workflow, Step } from "../../src/types";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";

/**
 * An in-memory fake standing in for WorkflowRepository — only implements
 * the subset of methods DagCoordinator actually calls. Lets these tests
 * run in milliseconds with no Postgres/Redis involved, since what's
 * being tested here is pure DAG dispatch logic, not persistence.
 */
class FakeRepository {
  private workflows = new Map<string, Workflow>();
  private attempts = new Map<string, number>();

  seed(workflow: Workflow): void {
    this.workflows.set(workflow.id, JSON.parse(JSON.stringify(workflow)));
  }

  async getWorkflow(id: string): Promise<Workflow | undefined> {
    const wf = this.workflows.get(id);
    return wf ? JSON.parse(JSON.stringify(wf)) : undefined;
  }

  async updateWorkflowStatus(id: string, status: Workflow["status"]): Promise<void> {
    const wf = this.workflows.get(id);
    if (wf) wf.status = status;
  }

  async updateStepStatus(workflowId: string, stepId: string, status: Step["status"]): Promise<void> {
    const wf = this.workflows.get(workflowId);
    const step = wf?.steps.find((s) => s.id === stepId);
    if (step) step.status = status;
  }

  async incrementAttempt(workflowId: string, stepId: string): Promise<number> {
    const key = `${workflowId}:${stepId}`;
    const next = (this.attempts.get(key) ?? 0) + 1;
    this.attempts.set(key, next);
    return next;
  }

  getLastError = jest.fn().mockResolvedValue(undefined);
  recordDeadLetter = jest.fn().mockResolvedValue(undefined);
  getAttemptCount = jest.fn().mockResolvedValue(1);
}

/** Fake producer that just records what was enqueued, for assertions. */
class FakeProducer {
  enqueued: { stepId: string }[] = [];
  async enqueue(payload: Record<string, unknown>): Promise<void> {
    this.enqueued.push({ stepId: payload.stepId as string });
  }
}

function makeWorkflow(id: string, steps: Step[], status: Workflow["status"] = "pending"): Workflow {
  return { id, name: "test", steps, status };
}

function step(id: string, dependsOn: string[] = [], status: Step["status"] = "pending"): Step {
  return { id, dependsOn, status };
}

/**
 * baseBackoffMs/maxBackoffMs default to 0 here — disabling retry backoff
 * — so all these logic-focused tests behave synchronously (a retry
 * dispatches immediately, same as before backoff existed). Backoff
 * timing itself has its own dedicated tests below, using small nonzero
 * delays there specifically.
 */
function setup(maxAttempts = 3, baseBackoffMs = 0, maxBackoffMs = 0) {
  const repo = new FakeRepository();
  const producer = new FakeProducer();
  const coordinator = new DagCoordinator(
    repo as unknown as WorkflowRepository,
    producer as unknown as QueueProducer,
    maxAttempts,
    undefined,
    baseBackoffMs,
    maxBackoffMs
  );
  return { repo, producer, coordinator };
}

describe("DagCoordinator (in-memory, no DB/Redis)", () => {
  it("marks an empty workflow completed immediately without dispatching anything", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow("wf1", []);
    repo.seed(wf);

    await coordinator.start(wf);

    expect((await repo.getWorkflow("wf1"))!.status).toBe("completed");
    expect(producer.enqueued).toHaveLength(0);
  });

  it("dispatches every zero-dependency step on start", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a"), step("b"), step("c", ["a"])]);
    repo.seed(wf);

    await coordinator.start(wf);

    expect(producer.enqueued.map((e) => e.stepId).sort()).toEqual(["a", "b"]);
    expect((await repo.getWorkflow("wf1"))!.status).toBe("running");
  });

  it("fails the workflow immediately if no step has zero dependencies (cycle)", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a", ["b"]), step("b", ["a"])]);
    repo.seed(wf);

    await coordinator.start(wf);

    expect((await repo.getWorkflow("wf1"))!.status).toBe("failed");
    expect(producer.enqueued).toHaveLength(0);
  });

  it("dispatches a single dependent step once its only dependency completes", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [step("a", [], "completed"), step("b", ["a"], "pending")],
      "running"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "a", true);

    expect(producer.enqueued.map((e) => e.stepId)).toEqual(["b"]);
  });

  it("does not dispatch a diamond's join step until BOTH branches complete", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [
        step("a", [], "completed"),
        step("b", ["a"], "running"),
        step("c", ["a"], "running"),
        step("d", ["b", "c"], "pending"),
      ],
      "running"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "b", true);
    expect(producer.enqueued.map((e) => e.stepId)).toEqual([]); // c still running

    await coordinator.handleStepResult("wf1", "c", true);
    expect(producer.enqueued.map((e) => e.stepId)).toEqual(["d"]);
  });

  it("dispatches multiple newly-ready steps together after a fan-in point", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [
        step("a", [], "running"),
        step("b", ["a"], "pending"),
        step("c", ["a"], "pending"),
      ],
      "running"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "a", true);

    expect(producer.enqueued.map((e) => e.stepId).sort()).toEqual(["b", "c"]);
  });

  it("marks the workflow completed once the last outstanding step finishes", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [step("a", [], "completed"), step("b", ["a"], "running")],
      "running"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "b", true);

    expect((await repo.getWorkflow("wf1"))!.status).toBe("completed");
  });

  it("marks the workflow failed when a step fails, without dispatching its dependents", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [step("a", [], "running"), step("b", ["a"], "pending")],
      "running"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "a", false);

    expect((await repo.getWorkflow("wf1"))!.status).toBe("failed");
    expect(producer.enqueued).toHaveLength(0);
  });

  it("does not resurrect a cancelled workflow when an in-flight step reports success", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [step("a", [], "running"), step("b", ["a"], "pending")],
      "cancelled"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "a", true);

    expect((await repo.getWorkflow("wf1"))!.status).toBe("cancelled");
    expect(producer.enqueued).toHaveLength(0);
  });

  it("permanently fails a step once its attempt count exceeds maxAttempts, and records it as a dead letter", async () => {
    const { repo, producer, coordinator } = setup(2); // maxAttempts = 2
    const wf = makeWorkflow("wf1", [step("a")], "running");
    repo.seed(wf);

    await coordinator.retryStep("wf1", "a"); // attempt 1 — under limit
    await coordinator.retryStep("wf1", "a"); // attempt 2 — under limit
    await coordinator.retryStep("wf1", "a"); // attempt 3 — exceeds limit

    const finalWf = await repo.getWorkflow("wf1");
    expect(finalWf!.steps[0].status).toBe("failed");
    expect(finalWf!.status).toBe("failed");
    expect(repo.recordDeadLetter).toHaveBeenCalledWith("wf1", "a", 2, undefined);
  });

  it("retryStep is a no-op on an already-cancelled workflow", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a")], "cancelled");
    repo.seed(wf);

    await coordinator.retryStep("wf1", "a");

    expect(producer.enqueued).toHaveLength(0);
  });

  it("cancel() marks a running workflow cancelled", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a")], "running");
    repo.seed(wf);

    await coordinator.cancel("wf1");

    expect((await repo.getWorkflow("wf1"))!.status).toBe("cancelled");
  });

  it("cancel() does not downgrade an already-completed workflow", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a", [], "completed")], "completed");
    repo.seed(wf);

    await coordinator.cancel("wf1");

    expect((await repo.getWorkflow("wf1"))!.status).toBe("completed");
  });

  it("cancel() does not downgrade an already-failed workflow", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a", [], "failed")], "failed");
    repo.seed(wf);

    await coordinator.cancel("wf1");

    expect((await repo.getWorkflow("wf1"))!.status).toBe("failed");
  });

  it("dispatches a deep linear chain one step at a time, in order", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow("wf1", [
      step("s1"),
      step("s2", ["s1"]),
      step("s3", ["s2"]),
      step("s4", ["s3"]),
      step("s5", ["s4"]),
    ]);
    repo.seed(wf);

    await coordinator.start(wf);
    expect(producer.enqueued.map((e) => e.stepId)).toEqual(["s1"]);

    for (const [prev, next] of [["s1", "s2"], ["s2", "s3"], ["s3", "s4"], ["s4", "s5"]]) {
      await repo.updateStepStatus("wf1", prev, "completed");
      await coordinator.handleStepResult("wf1", prev, true);
    }
    expect(producer.enqueued.map((e) => e.stepId)).toEqual(["s1", "s2", "s3", "s4", "s5"]);
  });

  it("dispatches a wide fan-out of 10 independent steps all at once", async () => {
    const { repo, producer, coordinator } = setup();
    const steps = Array.from({ length: 10 }, (_, i) => step(`s${i}`));
    const wf = makeWorkflow("wf1", steps);
    repo.seed(wf);

    await coordinator.start(wf);

    expect(producer.enqueued).toHaveLength(10);
  });

  it("does not re-dispatch a step that's already completed", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [step("a", [], "completed"), step("b", ["a"], "completed")],
      "running"
    );
    repo.seed(wf);

    // Some sibling of "a" finishing shouldn't cause "a" or "b" (already
    // completed) to be dispatched again.
    await repo.updateStepStatus("wf1", "c", "completed"); // no-op, "c" doesn't exist
    await coordinator.handleStepResult("wf1", "a", true);

    expect(producer.enqueued).toHaveLength(0);
  });

  it("start() marks the workflow running and dispatches its step as queued (not yet running)", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a")]);
    repo.seed(wf);

    await coordinator.start(wf);

    const finalWf = await repo.getWorkflow("wf1");
    expect(finalWf!.status).toBe("running");
    // "queued", not "running" — dispatch onto the queue and a worker
    // actually picking it up are different moments; markRunning() is
    // what transitions it further, and that's the worker pool's job.
    expect(finalWf!.steps[0].status).toBe("queued");
  });

  it("markRunning() transitions a queued step to running", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a", [], "queued")], "running");
    repo.seed(wf);

    await coordinator.markRunning("wf1", "a");

    expect((await repo.getWorkflow("wf1"))!.steps[0].status).toBe("running");
  });

  it("cancel() works on a workflow that hasn't been started yet (still pending)", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a")], "pending");
    repo.seed(wf);

    await coordinator.cancel("wf1");

    expect((await repo.getWorkflow("wf1"))!.status).toBe("cancelled");
  });

  it("cancel() on an already-cancelled workflow is a harmless no-op", async () => {
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a")], "cancelled");
    repo.seed(wf);

    await coordinator.cancel("wf1");

    expect((await repo.getWorkflow("wf1"))!.status).toBe("cancelled");
    expect(producer.enqueued).toHaveLength(0);
  });

  it("a duplicate handleStepResult call for the same step does not double-dispatch its dependent", async () => {
    // Regression test: if a step's completion were somehow reported
    // twice (e.g. a duplicate message), the dependent step must only be
    // dispatched once — the second call sees the dependent is no longer
    // "pending" (already queued from the first call) and skips it.
    const { repo, producer, coordinator } = setup();
    const wf = makeWorkflow(
      "wf1",
      [step("a", [], "running"), step("b", ["a"], "pending")],
      "running"
    );
    repo.seed(wf);

    await coordinator.handleStepResult("wf1", "a", true);
    await coordinator.handleStepResult("wf1", "a", true); // duplicate report

    expect(producer.enqueued.filter((e) => e.stepId === "b")).toHaveLength(1);
  });
});

describe("DagCoordinator retry backoff (real small delays, no DB/Redis)", () => {
  it("a retry (attempt > 1) enters 'retrying' status immediately, then 'queued' only after the backoff delay elapses", async () => {
    const { repo, producer, coordinator } = setup(3, 100, 100); // 100ms backoff
    const wf = makeWorkflow("wf1", [step("a")], "running");
    repo.seed(wf);

    await coordinator.retryStep("wf1", "a"); // attempt 1, no backoff — immediate
    expect((await repo.getWorkflow("wf1"))!.steps[0].status).toBe("queued");
    expect(producer.enqueued).toHaveLength(1);

    await coordinator.retryStep("wf1", "a"); // attempt 2 — this one backs off
    // Right after the call returns, the step should be waiting, not
    // re-enqueued yet — the backoff delay hasn't elapsed.
    expect((await repo.getWorkflow("wf1"))!.steps[0].status).toBe("retrying");
    expect(producer.enqueued).toHaveLength(1); // still just the first

    await new Promise((r) => setTimeout(r, 200)); // past the 100ms delay

    expect((await repo.getWorkflow("wf1"))!.steps[0].status).toBe("queued");
    expect(producer.enqueued).toHaveLength(2);
  });

  it("a backoff-delayed retry does not re-enqueue into a workflow that was cancelled during the wait", async () => {
    const { repo, producer, coordinator } = setup(3, 100, 100);
    const wf = makeWorkflow("wf1", [step("a")], "running");
    repo.seed(wf);

    await coordinator.retryStep("wf1", "a"); // attempt 1, immediate
    await coordinator.retryStep("wf1", "a"); // attempt 2, backs off 100ms

    await coordinator.cancel("wf1"); // cancel while the retry is waiting

    await new Promise((r) => setTimeout(r, 200)); // let the backoff timer fire

    expect(producer.enqueued).toHaveLength(1); // only the first attempt ever went out
    expect((await repo.getWorkflow("wf1"))!.status).toBe("cancelled");
  });
});
