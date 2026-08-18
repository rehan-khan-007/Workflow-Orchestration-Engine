import Redis from "ioredis";

export class QueueProducer {
  private redis: Redis;

  constructor(private queueName: string) {
    this.redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
  }

  async enqueue(payload: Record<string, unknown>): Promise<void> {
    await this.redis.lpush(`${this.queueName}:queue`, JSON.stringify(payload));
  }

  async enqueueMany(payloads: Record<string, unknown>[]): Promise<void> {
    const pipeline = this.redis.pipeline();
    for (const p of payloads) {
      pipeline.lpush(`${this.queueName}:queue`, JSON.stringify(p));
    }
    await pipeline.exec();
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}