import { WorkflowEngine } from "../../src/core/engine";
import { getPool, closePool } from "../../src/storage/db";

// These tests hit a real Postgres instance (see .env.example / docker-compose.yml).
// Run `npm run migrate` once before running tests.
describe("WorkflowEngine (Postgres-backed)", () => {
  const engine = new WorkflowEngine();

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
  });

  afterAll(async () => {
    await closePool();
  });

  it("should create a workflow and persist it", async () => {
    const wf = await engine.createWorkflow("test", []);
    expect(wf.id).toBeDefined();
    expect(wf.name).toBe("test");
    expect(wf.status).toBe("pending");
  });

  it("should retrieve a workflow by id after creation", async () => {
    const created = await engine.createWorkflow("test", []);
    const fetched = await engine.getWorkflow(created.id);
    expect(fetched).toBeDefined();
    expect(fetched!.name).toBe("test");
  });

  it("should persist steps with their dependencies", async () => {
    const created = await engine.createWorkflow("with-steps", [
      { id: "a", dependsOn: [], status: "pending" },
      { id: "b", dependsOn: ["a"], status: "pending" },
    ]);
    const fetched = await engine.getWorkflow(created.id);
    expect(fetched!.steps).toHaveLength(2);
    const stepB = fetched!.steps.find((s) => s.id === "b");
    expect(stepB!.dependsOn).toEqual(["a"]);
  });

  it("should list all workflows", async () => {
    await engine.createWorkflow("wf1", []);
    await engine.createWorkflow("wf2", []);
    const list = await engine.listWorkflows();
    expect(list.length).toBe(2);
  });

  it("should survive being re-fetched via a fresh WorkflowEngine instance", async () => {
    // Proves state lives in Postgres, not in engine's memory — the old
    // in-memory Map implementation would fail this test.
    const created = await engine.createWorkflow("durable", []);
    const freshEngine = new WorkflowEngine();
    const fetched = await freshEngine.getWorkflow(created.id);
    expect(fetched).toBeDefined();
    expect(fetched!.name).toBe("durable");
  });
});
