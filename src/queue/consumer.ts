import Redis from "ioredis";
import { EventEmitter } from "events";

export type QueueHandler = (payload: Record<string, unknown>) => Promise<void>;

export class QueueConsumer extends EventEmitter {
  private redis: Redis;
  private running = false;

  constructor(private queueName: string) {
    super();
    this.redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  }

  async start(handler: QueueHandler): Promise<void> {
    this.running = true;
    while (this.running) {
      const result = await this.redis.brpop(`${this.queueName}:queue`, 2);
      if (result) {
        const payload = JSON.parse(result[1]);
        try {
          await handler(payload);
          this.emit("processed", payload);
        } catch (err) {
          this.emit("failed", { payload, error: (err as Error).message });
        }
      }
    }
  }

  stop(): void {
    this.running = false;
  }

  async close(): Promise<void> {
    this.stop();
    await this.redis.quit();
  }
}