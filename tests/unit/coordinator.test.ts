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

function setup(maxAttempts = 3) {
  const repo = new FakeRepository();
  const producer = new FakeProducer();
  const coordinator = new DagCoordinator(
    repo as unknown as WorkflowRepository,
    producer as unknown as QueueProducer,
    maxAttempts
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

  it("permanently fails a step once its attempt count exceeds maxAttempts", async () => {
    const { repo, producer, coordinator } = setup(2); // maxAttempts = 2
    const wf = makeWorkflow("wf1", [step("a")], "running");
    repo.seed(wf);

    await coordinator.retryStep("wf1", "a"); // attempt 1 — under limit
    await coordinator.retryStep("wf1", "a"); // attempt 2 — under limit
    await coordinator.retryStep("wf1", "a"); // attempt 3 — exceeds limit

    const finalWf = await repo.getWorkflow("wf1");
    expect(finalWf!.steps[0].status).toBe("failed");
    expect(finalWf!.status).toBe("failed");
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

  it("start() marks workflow running before any step is dispatched", async () => {
    const { repo, coordinator } = setup();
    const wf = makeWorkflow("wf1", [step("a")]);
    repo.seed(wf);

    await coordinator.start(wf);

    const finalWf = await repo.getWorkflow("wf1");
    expect(finalWf!.status).toBe("running");
    expect(finalWf!.steps[0].status).toBe("running");
  });
});
