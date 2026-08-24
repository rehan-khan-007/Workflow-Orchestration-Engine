import { AddressInfo } from "net";
import { Server } from "http";
import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkerPool } from "../../src/worker/pool";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { EventBus } from "../../src/queue/eventBus";
import { createApp } from "../../src/api/server";
import { getPool, closePool } from "../../src/storage/db";
import Redis from "ioredis";

// Full round trip through real HTTP: this test starts an actual Express
// server on an ephemeral port and a real worker pool, and drives
// everything through fetch() the same way a real client would — no
// mocking of the HTTP layer or the engine underneath it.
describe("REST API (Redis + Postgres + real HTTP backed)", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-api-${Date.now()}`;
  let producer: QueueProducer;
  let eventBus: EventBus;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let pool: WorkerPool;
  let server: Server;
  let baseUrl: string;
  let redisRaw: Redis;

  beforeAll(async () => {
    producer = new QueueProducer(queueName);
    eventBus = new EventBus();
    coordinator = new DagCoordinator(repo, producer, 3, eventBus);
    scheduler = new DagScheduler(coordinator);
    pool = new WorkerPool(queueName, coordinator, repo, 3, async () => {
      await new Promise((r) => setTimeout(r, 150));
    });
    pool.start();

    const app = createApp(engine, scheduler, coordinator, eventBus);
    server = app.listen(0);
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const { port } = server.address() as AddressInfo;
    baseUrl = `http://localhost:${port}`;

    redisRaw = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
    await redisRaw.del(`${queueName}:queue`);
  });

  afterAll(async () => {
    await pool.stop();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await producer.close();
    await eventBus.close();
    await redisRaw.quit();
    await closePool();
  });

  async function waitForStatus(id: string, status: string, timeoutMs = 5000): Promise<any> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const res = await fetch(`${baseUrl}/workflows/${id}`);
      const wf = (await res.json()) as any;
      if (wf.status === status) return wf;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`Timed out waiting for ${id} to reach ${status}`);
  }

  it("creates a workflow via POST and it executes to completion", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "api-test",
        steps: [
          { id: "a", dependsOn: [], status: "pending" },
          { id: "b", dependsOn: ["a"], status: "pending" },
        ],
      }),
    });
    expect(res.status).toBe(201);
    const created = await res.json() as any;
    expect(created.status).toBe("running");

    const finished = await waitForStatus(created.id, "completed");
    expect(finished.steps.every((s: any) => s.status === "completed")).toBe(true);
  });

  it("rejects a malformed workflow submission with 400", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "missing-steps" }),
    });
    expect(res.status).toBe(400);
  });

  it("rejects a workflow with duplicate step ids with 400", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "dup-ids",
        steps: [
          { id: "a", dependsOn: [], status: "pending" },
          { id: "a", dependsOn: [], status: "pending" },
        ],
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as any;
    expect(body.error).toMatch(/duplicate/i);
  });

  it("rejects a workflow with a dangling dependency reference with 400", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "dangling-dep",
        steps: [{ id: "a", dependsOn: ["ghost"], status: "pending" }],
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as any;
    expect(body.error).toMatch(/unknown step/i);
  });

  it("rejects a workflow containing a dependency cycle with 400", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "cyclic",
        steps: [
          { id: "a", dependsOn: ["b"], status: "pending" },
          { id: "b", dependsOn: ["a"], status: "pending" },
        ],
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as any;
    expect(body.error).toMatch(/cycle/i);
  });

  it("returns 404 for a workflow that doesn't exist", async () => {
    const res = await fetch(`${baseUrl}/workflows/does-not-exist`);
    expect(res.status).toBe(404);
  });

  it("lists workflows via GET /workflows", async () => {
    await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "list-test-1", steps: [] }),
    });
    await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "list-test-2", steps: [] }),
    });

    const res = await fetch(`${baseUrl}/workflows`);
    const list = await res.json() as any;
    expect(list.length).toBe(2);
  });

  it("cancels a running workflow and it never reaches completed", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "cancel-test",
        steps: [
          { id: "slow", dependsOn: [], status: "pending" },
          { id: "next", dependsOn: ["slow"], status: "pending" },
        ],
      }),
    });
    const created = await res.json() as any;

    const cancelRes = await fetch(`${baseUrl}/workflows/${created.id}/cancel`, { method: "POST" });
    expect(cancelRes.status).toBe(200);
    const cancelled = await cancelRes.json() as any;
    expect(cancelled.status).toBe("cancelled");

    // Give the in-flight "slow" step time to finish — it should NOT
    // trigger "next" to be dispatched, since the workflow is cancelled.
    await new Promise((r) => setTimeout(r, 500));
    const after = await (await fetch(`${baseUrl}/workflows/${created.id}`)).json() as any;
    expect(after.status).toBe("cancelled");
    expect(after.steps.find((s: any) => s.id === "next").status).toBe("pending");
  });

  it("streams live status updates via SSE until the workflow finishes", async () => {
    const created = await (
      await fetch(`${baseUrl}/workflows`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "sse-test",
          steps: [{ id: "only", dependsOn: [], status: "pending" }],
        }),
      })
    ).json() as any;

    const streamRes = await fetch(`${baseUrl}/workflows/${created.id}/stream`);
    expect(streamRes.headers.get("content-type")).toContain("text/event-stream");

    const reader = streamRes.body!.getReader();
    const decoder = new TextDecoder();
    let raw = "";
    const events: any[] = [];
    const deadline = Date.now() + 5000;

    while (Date.now() < deadline) {
      const { done, value } = await reader.read();
      if (done) break;
      raw += decoder.decode(value, { stream: true });
      let boundary;
      while ((boundary = raw.indexOf("\n\n")) !== -1) {
        const chunk = raw.slice(0, boundary);
        raw = raw.slice(boundary + 2);
        const line = chunk.split("\n").find((l) => l.startsWith("data: "));
        if (line) events.push(JSON.parse(line.slice("data: ".length)));
      }
      if (events.some((e) => e.type === "workflow_status" && e.status === "completed")) break;
    }

    expect(events[0].type).toBe("snapshot");
    expect(events.some((e) => e.type === "step_status" && e.status === "completed")).toBe(true);
    expect(events.some((e) => e.type === "workflow_status" && e.status === "completed")).toBe(true);
  }, 10000);
});
