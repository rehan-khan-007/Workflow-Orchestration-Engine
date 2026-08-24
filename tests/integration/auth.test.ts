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

describe("API key authentication", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-auth-${Date.now()}`;
  const apiKey = "test-secret-key";
  let producer: QueueProducer;
  let eventBus: EventBus;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let pool: WorkerPool;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    producer = new QueueProducer(queueName);
    eventBus = new EventBus();
    coordinator = new DagCoordinator(repo, producer, 3, eventBus);
    scheduler = new DagScheduler(coordinator);
    pool = new WorkerPool(queueName, coordinator, repo, 2, async () => {
      await new Promise((r) => setTimeout(r, 50));
    });
    pool.start();

    // apiKey passed here — this app instance requires it, unlike every
    // other test file's app (which pass no key, matching the default
    // local/dev behavior already proven backward-compatible elsewhere).
    const app = createApp(engine, scheduler, coordinator, eventBus, repo, apiKey);
    server = app.listen(0);
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const { port } = server.address() as AddressInfo;
    baseUrl = `http://localhost:${port}`;
  });

  beforeEach(async () => {
    await getPool().query("TRUNCATE workflows CASCADE");
  });

  afterAll(async () => {
    await pool.stop();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await producer.close();
    await eventBus.close();
  });

  it("rejects a request with no Authorization header at all", async () => {
    const res = await fetch(`${baseUrl}/workflows`);
    expect(res.status).toBe(401);
  });

  it("rejects a request with the wrong API key", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      headers: { Authorization: "Bearer wrong-key" },
    });
    expect(res.status).toBe(401);
  });

  it("rejects a request with a malformed Authorization header (missing 'Bearer ' prefix)", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      headers: { Authorization: apiKey },
    });
    expect(res.status).toBe(401);
  });

  it("accepts a request with the correct API key", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(res.status).toBe(200);
  });

  it("enforces auth on POST /workflows too, not just GET routes", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "should-be-rejected", steps: [] }),
    });
    expect(res.status).toBe(401);
  });

  it("does not require auth on GET /metrics, even when an API key is configured", async () => {
    // Prometheus scrapers don't send app-level auth headers by default —
    // /metrics is deliberately exempted, see server.ts.
    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(200);
  });

  it("a correctly-authenticated request can create and complete a real workflow", async () => {
    const created = await (
      await fetch(`${baseUrl}/workflows`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          name: "authenticated-workflow",
          steps: [{ id: "a", dependsOn: [], status: "pending" }],
        }),
      })
    ).json() as any;
    expect(created.status).toBe("running");

    const deadline = Date.now() + 5000;
    let finalStatus: string | undefined;
    while (Date.now() < deadline) {
      const res = await fetch(`${baseUrl}/workflows/${created.id}`, {
        headers: { Authorization: `Bearer ${apiKey}` },
      });
      const wf = (await res.json()) as any;
      if (wf.status === "completed") {
        finalStatus = wf.status;
        break;
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    expect(finalStatus).toBe("completed");
  }, 10000);
});

describe("API with no API key configured (default, backward-compatible)", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-noauth-${Date.now()}`;
  let producer: QueueProducer;
  let eventBus: EventBus;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    producer = new QueueProducer(queueName);
    eventBus = new EventBus();
    coordinator = new DagCoordinator(repo, producer, 3, eventBus);
    scheduler = new DagScheduler(coordinator);
    // No apiKey argument — auth disabled, the default.
    const app = createApp(engine, scheduler, coordinator, eventBus, repo);
    server = app.listen(0);
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const { port } = server.address() as AddressInfo;
    baseUrl = `http://localhost:${port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await producer.close();
    await eventBus.close();
  });

  it("accepts requests with no Authorization header when no API key is configured", async () => {
    const res = await fetch(`${baseUrl}/workflows`);
    expect(res.status).toBe(200);
  });
});

describe("Rate limiting", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-ratelimit-${Date.now()}`;
  let producer: QueueProducer;
  let eventBus: EventBus;
  let coordinator: DagCoordinator;
  let scheduler: DagScheduler;
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    producer = new QueueProducer(queueName);
    eventBus = new EventBus();
    coordinator = new DagCoordinator(repo, producer, 3, eventBus);
    scheduler = new DagScheduler(coordinator);
    // A deliberately tiny limit (3 requests per minute) so the test can
    // actually trigger a 429 quickly, rather than needing 100+ real
    // requests against the production default.
    const app = createApp(engine, scheduler, coordinator, eventBus, repo, undefined, {
      windowMs: 60_000,
      max: 3,
    });
    server = app.listen(0);
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const { port } = server.address() as AddressInfo;
    baseUrl = `http://localhost:${port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await producer.close();
    await eventBus.close();
    await closePool();
  });

  it("allows requests up to the configured limit, then rejects further ones with 429", async () => {
    const results: number[] = [];
    for (let i = 0; i < 5; i++) {
      const res = await fetch(`${baseUrl}/workflows`);
      results.push(res.status);
    }
    expect(results.slice(0, 3)).toEqual([200, 200, 200]);
    expect(results.slice(3)).toEqual([429, 429]);
  });

  it("does not rate-limit GET /metrics, even after the main limit is exhausted", async () => {
    // The previous test already exhausted this app instance's limit —
    // /metrics should still respond normally, since it's exempted.
    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(200);
  });
});
