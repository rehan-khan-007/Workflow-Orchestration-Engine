import { formatLogEntry, log, LogFields } from "../../src/observability/logger";

describe("formatLogEntry", () => {
  it("produces valid JSON", () => {
    const entry = formatLogEntry({ event: "workflow_started", workflowId: "wf1" });
    expect(() => JSON.parse(entry)).not.toThrow();
  });

  it("includes every field passed in", () => {
    const fields: LogFields = {
      event: "step_execution_finished",
      workflowId: "wf1",
      stepId: "a",
      attempt: 2,
      workerId: "worker-1",
      durationMs: 482,
      result: "completed",
    };
    const parsed = JSON.parse(formatLogEntry(fields));
    expect(parsed.event).toBe("step_execution_finished");
    expect(parsed.workflowId).toBe("wf1");
    expect(parsed.stepId).toBe("a");
    expect(parsed.attempt).toBe(2);
    expect(parsed.workerId).toBe("worker-1");
    expect(parsed.durationMs).toBe(482);
    expect(parsed.result).toBe("completed");
  });

  it("adds an ISO 8601 timestamp automatically", () => {
    const parsed = JSON.parse(formatLogEntry({ event: "workflow_started" }));
    expect(parsed.timestamp).toBeDefined();
    expect(() => new Date(parsed.timestamp).toISOString()).not.toThrow();
    expect(new Date(parsed.timestamp).toISOString()).toBe(parsed.timestamp);
  });

  it("omits fields that were never provided, rather than emitting them as null/undefined", () => {
    const parsed = JSON.parse(formatLogEntry({ event: "workflow_started", workflowId: "wf1" }));
    expect("stepId" in parsed).toBe(false);
    expect("workerId" in parsed).toBe(false);
  });
});

describe("log", () => {
  const originalNodeEnv = process.env.NODE_ENV;
  let consoleSpy: jest.SpyInstance;

  beforeEach(() => {
    consoleSpy = jest.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
    process.env.NODE_ENV = originalNodeEnv;
  });

  it("is suppressed under NODE_ENV=test (keeps jest's own output clean)", () => {
    process.env.NODE_ENV = "test";
    log({ event: "workflow_started", workflowId: "wf1" });
    expect(consoleSpy).not.toHaveBeenCalled();
  });

  it("actually logs when NODE_ENV is not 'test' (dev/production behavior)", () => {
    process.env.NODE_ENV = "production";
    log({ event: "workflow_started", workflowId: "wf1" });
    expect(consoleSpy).toHaveBeenCalledTimes(1);
    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.event).toBe("workflow_started");
    expect(logged.workflowId).toBe("wf1");
  });
});
