import { WorkflowEngine } from "../../src/core/engine";

describe("WorkflowEngine", () => {
  it("should create a workflow", async () => {
    const engine = new WorkflowEngine();
    const wf = await engine.createWorkflow("test", []);
    expect(wf.id).toBeDefined();
    expect(wf.name).toBe("test");
    expect(wf.status).toBe("pending");
  });

  it("should retrieve a workflow by id", async () => {
    const engine = new WorkflowEngine();
    const created = await engine.createWorkflow("test", []);
    const fetched = await engine.getWorkflow(created.id);
    expect(fetched).toBeDefined();
    expect(fetched!.name).toBe("test");
  });

  it("should list all workflows", async () => {
    const engine = new WorkflowEngine();
    await engine.createWorkflow("wf1", []);
    await engine.createWorkflow("wf2", []);
    const list = await engine.listWorkflows();
    expect(list.length).toBe(2);
  });
});