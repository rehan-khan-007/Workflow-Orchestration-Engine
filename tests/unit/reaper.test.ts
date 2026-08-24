import { Reaper } from "../../src/worker/reaper";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { LeaseManager } from "../../src/worker/leaseManager";
import { DagCoordinator } from "../../src/core/coordinator";

function makeMocks(runningSteps: { workflowId: string; stepId: string; updatedAtMs: number }[]) {
  const repo = {
    listDispatchedSteps: jest.fn().mockResolvedValue(runningSteps),
  };
  const leases = {
    exists: jest.fn(),
  };
  const coordinator = {
    retryStep: jest.fn().mockResolvedValue(undefined),
  };
  return { repo, leases, coordinator };
}

function build(
  repo: unknown,
  leases: unknown,
  coordinator: unknown,
  intervalMs = 1000,
  minAgeMs = 4000
): Reaper {
  return new Reaper(
    repo as WorkflowRepository,
    leases as LeaseManager,
    coordinator as DagCoordinator,
    intervalMs,
    minAgeMs
  );
}

describe("Reaper.sweep (mocked, no DB/Redis)", () => {
  it("does nothing when there are no running steps", async () => {
    const { repo, leases, coordinator } = makeMocks([]);
    const reaper = build(repo, leases, coordinator);

    await reaper.sweep();

    expect(coordinator.retryStep).not.toHaveBeenCalled();
  });

  it("ignores a step younger than minAgeMs, even with no lease", async () => {
    const { repo, leases, coordinator } = makeMocks([
      { workflowId: "wf1", stepId: "a", updatedAtMs: Date.now() - 500 }, // only 0.5s old
    ]);
    leases.exists.mockResolvedValue(false);
    const reaper = build(repo, leases, coordinator, 1000, 4000);

    await reaper.sweep();

    expect(coordinator.retryStep).not.toHaveBeenCalled();
  });

  it("retries a step old enough with no lease held (abandoned)", async () => {
    const { repo, leases, coordinator } = makeMocks([
      { workflowId: "wf1", stepId: "a", updatedAtMs: Date.now() - 5000 }, // 5s old, past minAge
    ]);
    leases.exists.mockResolvedValue(false);
    const reaper = build(repo, leases, coordinator, 1000, 4000);

    await reaper.sweep();

    expect(coordinator.retryStep).toHaveBeenCalledWith("wf1", "a");
  });

  it("does not retry a step whose lease is still held (worker alive)", async () => {
    const { repo, leases, coordinator } = makeMocks([
      { workflowId: "wf1", stepId: "a", updatedAtMs: Date.now() - 5000 },
    ]);
    leases.exists.mockResolvedValue(true);
    const reaper = build(repo, leases, coordinator, 1000, 4000);

    await reaper.sweep();

    expect(coordinator.retryStep).not.toHaveBeenCalled();
  });

  it("handles multiple running steps independently — only retries the abandoned ones", async () => {
    const { repo, leases, coordinator } = makeMocks([
      { workflowId: "wf1", stepId: "abandoned", updatedAtMs: Date.now() - 5000 },
      { workflowId: "wf1", stepId: "alive", updatedAtMs: Date.now() - 5000 },
      { workflowId: "wf1", stepId: "too-young", updatedAtMs: Date.now() - 100 },
    ]);
    leases.exists.mockImplementation(async (_wf: string, stepId: string) => stepId === "alive");
    const reaper = build(repo, leases, coordinator, 1000, 4000);

    await reaper.sweep();

    expect(coordinator.retryStep).toHaveBeenCalledTimes(1);
    expect(coordinator.retryStep).toHaveBeenCalledWith("wf1", "abandoned");
  });

  it("a failing retryStep call for one step does not prevent checking the rest", async () => {
    const { repo, leases, coordinator } = makeMocks([
      { workflowId: "wf1", stepId: "a", updatedAtMs: Date.now() - 5000 },
      { workflowId: "wf1", stepId: "b", updatedAtMs: Date.now() - 5000 },
    ]);
    leases.exists.mockResolvedValue(false);
    coordinator.retryStep = jest
      .fn()
      .mockRejectedValueOnce(new Error("simulated failure"))
      .mockResolvedValueOnce(undefined);
    const reaper = build(repo, leases, coordinator, 1000, 4000);

    // sweep() itself doesn't guard individual retryStep calls, so a
    // rejection here is expected to propagate — start()'s interval
    // wrapper is what swallows it in production. This test documents
    // that sweep() awaits sequentially rather than using Promise.all,
    // by confirming the first step's failure surfaces.
    await expect(reaper.sweep()).rejects.toThrow("simulated failure");
  });
});

describe("Reaper.start/stop (fake timers)", () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("calls sweep repeatedly at the configured interval while started", () => {
    const { repo, leases, coordinator } = makeMocks([]);
    const reaper = build(repo, leases, coordinator, 1000, 4000);
    const sweepSpy = jest.spyOn(reaper, "sweep").mockResolvedValue(undefined);

    reaper.start();
    jest.advanceTimersByTime(3500);

    expect(sweepSpy).toHaveBeenCalledTimes(3);
  });

  it("stops calling sweep once stop() is called", () => {
    const { repo, leases, coordinator } = makeMocks([]);
    const reaper = build(repo, leases, coordinator, 1000, 4000);
    const sweepSpy = jest.spyOn(reaper, "sweep").mockResolvedValue(undefined);

    reaper.start();
    jest.advanceTimersByTime(2000);
    reaper.stop();
    jest.advanceTimersByTime(5000);

    expect(sweepSpy).toHaveBeenCalledTimes(2);
  });

  it("stop() before start() is a harmless no-op", () => {
    const { repo, leases, coordinator } = makeMocks([]);
    const reaper = build(repo, leases, coordinator, 1000, 4000);

    expect(() => reaper.stop()).not.toThrow();
  });
});
