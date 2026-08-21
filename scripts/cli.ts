/**
 * CLI for the Workflow Orchestration Engine's REST API.
 *
 * Usage:
 *   npm run cli -- create <workflow.json>
 *   npm run cli -- list
 *   npm run cli -- get <workflowId>
 *   npm run cli -- cancel <workflowId>
 *   npm run cli -- watch <workflowId>
 *
 * Talks to the API at API_URL (default http://localhost:3000) — the API
 * process must be running separately (`npm run start:api`).
 */
import { readFileSync } from "fs";

const API_URL = process.env.API_URL || "http://localhost:3000";

async function create(filePath: string): Promise<void> {
  const body = readFileSync(filePath, "utf-8");
  const res = await fetch(`${API_URL}/workflows`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  const data = await res.json();
  if (!res.ok) {
    console.error(`Error: ${JSON.stringify(data)}`);
    process.exitCode = 1;
    return;
  }
  console.log(JSON.stringify(data, null, 2));
}

async function list(): Promise<void> {
  const res = await fetch(`${API_URL}/workflows`);
  const data = await res.json();
  console.log(JSON.stringify(data, null, 2));
}

async function get(id: string): Promise<void> {
  const res = await fetch(`${API_URL}/workflows/${id}`);
  const data = await res.json();
  if (!res.ok) {
    console.error(`Error: ${JSON.stringify(data)}`);
    process.exitCode = 1;
    return;
  }
  console.log(JSON.stringify(data, null, 2));
}

async function cancel(id: string): Promise<void> {
  const res = await fetch(`${API_URL}/workflows/${id}/cancel`, { method: "POST" });
  const data = await res.json();
  if (!res.ok) {
    console.error(`Error: ${JSON.stringify(data)}`);
    process.exitCode = 1;
    return;
  }
  console.log(JSON.stringify(data, null, 2));
}

async function watch(id: string): Promise<void> {
  const res = await fetch(`${API_URL}/workflows/${id}/stream`);
  if (!res.ok || !res.body) {
    console.error(`Error: could not open stream (status ${res.status})`);
    process.exitCode = 1;
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let boundary: number;
    while ((boundary = buffer.indexOf("\n\n")) !== -1) {
      const rawEvent = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const line = rawEvent.split("\n").find((l) => l.startsWith("data: "));
      if (line) {
        const payload = JSON.parse(line.slice("data: ".length));
        console.log(JSON.stringify(payload));
      }
    }
  }
}

async function main(): Promise<void> {
  const [command, arg] = process.argv.slice(2);

  switch (command) {
    case "create":
      if (!arg) return usage("create requires a path to a workflow JSON file");
      return create(arg);
    case "list":
      return list();
    case "get":
      if (!arg) return usage("get requires a workflow id");
      return get(arg);
    case "cancel":
      if (!arg) return usage("cancel requires a workflow id");
      return cancel(arg);
    case "watch":
      if (!arg) return usage("watch requires a workflow id");
      return watch(arg);
    default:
      return usage(`Unknown command: ${command ?? "(none)"}`);
  }
}

function usage(message: string): void {
  console.error(message);
  console.error("Usage: npm run cli -- <create|list|get|cancel|watch> [arg]");
  process.exitCode = 1;
}

main();
