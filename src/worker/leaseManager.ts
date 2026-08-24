import Redis from "ioredis";

/**
 * A lease is a Redis key with a TTL, proving "worker X is actively
 * executing step Y right now." As long as the worker keeps renewing it
 * (heartbeating), the lease stays alive. If the worker crashes, renewal
 * stops and the key expires on its own — Redis does the failure detection
 * for us, with no polling of the worker itself required.
 */
export class LeaseManager {
  private redis: Redis;

  constructor(redisUrl: string = process.env.REDIS_URL || "redis://localhost:6379") {
    this.redis = new Redis(redisUrl);
  }

  private key(workflowId: string, stepId: string): string {
    return `lease:${workflowId}:${stepId}`;
  }

  /** Acquires the lease if nobody else currently holds it. Returns false if already held. */
  async acquire(
    workflowId: string,
    stepId: string,
    workerId: string,
    ttlMs: number
  ): Promise<boolean> {
    const result = await this.redis.set(
      this.key(workflowId, stepId),
      workerId,
      "PX",
      ttlMs,
      "NX"
    );
    return result === "OK";
  }

  /**
   * Extends the lease's TTL — but only if this worker still owns it.
   * Atomic (a single Lua script, not a separate GET then PEXPIRE) to
   * close a real race: without this check, a worker whose lease already
   * expired and was reclaimed by a different worker (retrying the same
   * step after presumed abandonment) could renew what is now someone
   * else's lease, artificially extending a stale claim. Returns false if
   * the caller no longer owns the lease — callers can use that to notice
   * they've lost ownership mid-execution.
   */
  async renew(
    workflowId: string,
    stepId: string,
    workerId: string,
    ttlMs: number
  ): Promise<boolean> {
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
    const result = await this.redis.eval(script, 1, this.key(workflowId, stepId), workerId, ttlMs);
    return result === 1;
  }

  /** Releases the lease, only if it's still held (avoids deleting a lease someone else just acquired). */
  async release(workflowId: string, stepId: string, workerId: string): Promise<void> {
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    await this.redis.eval(script, 1, this.key(workflowId, stepId), workerId);
  }

  /** Whether a lease currently exists (used by the reaper to check for abandonment). */
  async exists(workflowId: string, stepId: string): Promise<boolean> {
    const result = await this.redis.exists(this.key(workflowId, stepId));
    return result === 1;
  }

  async close(): Promise<void> {
    if (this.redis.status === "end") return;
    await this.redis.quit();
  }
}
