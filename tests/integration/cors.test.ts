import { AddressInfo } from "net";
import { Server } from "http";
import { WorkflowEngine } from "../../src/core/engine";
import { DagCoordinator } from "../../src/core/coordinator";
import { DagScheduler } from "../../src/scheduler/scheduler";
import { WorkflowRepository } from "../../src/storage/workflowRepository";
import { QueueProducer } from "../../src/queue/producer";
import { EventBus } from "../../src/queue/eventBus";
import { createApp } from "../../src/api/server";
import { closePool } from "../../src/storage/db";

describe("CORS", () => {
  const repo = new WorkflowRepository();
  const engine = new WorkflowEngine(repo);
  const queueName = `test-cors-${Date.now()}`;
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
    await closePool();
  });

  it("sets Access-Control-Allow-Origin on a real GET response, from a simulated foreign origin", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      headers: { Origin: "https://example-demo-page.com" },
    });
    expect(res.headers.get("access-control-allow-origin")).toBe("*");
  });

  it("responds to a CORS preflight OPTIONS request with 204 and the right allow-headers", async () => {
    const res = await fetch(`${baseUrl}/workflows`, {
      method: "OPTIONS",
      headers: {
        Origin: "https://example-demo-page.com",
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "Content-Type, Authorization",
      },
    });
    expect(res.status).toBe(204);
    expect(res.headers.get("access-control-allow-methods")).toContain("POST");
    expect(res.headers.get("access-control-allow-headers")).toContain("Authorization");
  });

  it("also sets the CORS header on /metrics, which sits outside the auth/rate-limit middleware", async () => {
    // /metrics is registered before requireApiKey/rateLimit in server.ts;
    // confirming CORS still applies there too, since a demo dashboard
    // might legitimately want to read it cross-origin as well.
    const res = await fetch(`${baseUrl}/metrics`, {
      headers: { Origin: "https://example-demo-page.com" },
    });
    expect(res.headers.get("access-control-allow-origin")).toBe("*");
  });
});
