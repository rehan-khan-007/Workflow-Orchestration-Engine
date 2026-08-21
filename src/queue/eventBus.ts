import Redis from "ioredis";

export interface WorkflowEvent {
  type: "step_status" | "workflow_status";
  workflowId: string;
  stepId?: string;
  status: string;
  at: string;
}

/**
 * Thin wrapper around Redis Pub/Sub for live status updates. The
 * coordinator publishes an event every time a step or workflow's status
 * changes; the API's SSE endpoint subscribes per-workflow and streams
 * those events straight to connected clients.
 */
export class EventBus {
  private publisher: Redis;

  constructor(private redisUrl: string = process.env.REDIS_URL || "redis://localhost:6379") {
    this.publisher = new Redis(redisUrl);
  }

  private channel(workflowId: string): string {
    return `workflow:${workflowId}:events`;
  }

  async publish(event: WorkflowEvent): Promise<void> {
    await this.publisher.publish(this.channel(event.workflowId), JSON.stringify(event));
  }

  /**
   * Subscribes to a single workflow's events. Returns the dedicated Redis
   * connection used for the subscription — the caller is responsible for
   * calling .quit() on it when done (e.g. when the SSE client disconnects),
   * since a subscribed connection can't be reused for other commands.
   */
  subscribe(workflowId: string, onEvent: (event: WorkflowEvent) => void): Redis {
    const sub = new Redis(this.redisUrl);
    sub.subscribe(this.channel(workflowId));
    sub.on("message", (_channel, message) => {
      try {
        onEvent(JSON.parse(message) as WorkflowEvent);
      } catch {
        // malformed message — ignore rather than crash the subscriber
      }
    });
    return sub;
  }

  async close(): Promise<void> {
    if (this.publisher.status !== "end") await this.publisher.quit();
  }
}
