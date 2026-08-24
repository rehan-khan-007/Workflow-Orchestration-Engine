import { randomUUID } from "crypto";
import { LeaseManager } from "../../src/worker/leaseManager";

// Each test uses its own globally-unique workflow/step ids (via
// randomUUID, not a simple counter) — a counter resets to 0 on every
// fresh test process, so two `npx jest` runs within the same few
// seconds could reuse the same key while an old TTL from the previous
// run was still alive in Redis. A random id can't collide across runs.
function uniqueIds(): { wf: string; step: string } {
  const id = randomUUID();
  return { wf: `wf-${id}`, step: `step-${id}` };
}

describe("LeaseManager (real Redis)", () => {
  let leases: LeaseManager;

  beforeEach(() => {
    leases = new LeaseManager();
  });

  afterEach(async () => {
    await leases.close();
  });

  it("acquires a lease when none is currently held", async () => {
    const { wf, step } = uniqueIds();
    const acquired = await leases.acquire(wf, step, "worker-a", 5000);
    expect(acquired).toBe(true);
  });

  it("fails to acquire a lease already held by another worker", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 5000);
    const second = await leases.acquire(wf, step, "worker-b", 5000);
    expect(second).toBe(false);
  });

  it("allows acquiring again after the lease is released", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 5000);
    await leases.release(wf, step, "worker-a");
    const reacquired = await leases.acquire(wf, step, "worker-b", 5000);
    expect(reacquired).toBe(true);
  });

  it("does not release a lease when called by a non-owning worker", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 5000);
    await leases.release(wf, step, "worker-b"); // wrong owner
    const stillHeld = await leases.exists(wf, step);
    expect(stillHeld).toBe(true);
  });

  it("removes the lease when released by its actual owner", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 5000);
    await leases.release(wf, step, "worker-a");
    const stillHeld = await leases.exists(wf, step);
    expect(stillHeld).toBe(false);
  });

  it("reports no lease exists before one is acquired", async () => {
    const { wf, step } = uniqueIds();
    const exists = await leases.exists(wf, step);
    expect(exists).toBe(false);
  });

  it("reports a lease exists immediately after acquiring", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 5000);
    const exists = await leases.exists(wf, step);
    expect(exists).toBe(true);
  });

  it("lease expires on its own after the TTL elapses with no renewal", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 300);
    await new Promise((r) => setTimeout(r, 500));
    const exists = await leases.exists(wf, step);
    expect(exists).toBe(false);
  }, 3000);

  it("renew extends the lease past its original expiry", async () => {
    const { wf, step } = uniqueIds();
    await leases.acquire(wf, step, "worker-a", 400);
    await new Promise((r) => setTimeout(r, 250));
    await leases.renew(wf, step, 400);
    await new Promise((r) => setTimeout(r, 250));
    // Original TTL (400ms) would have expired by now (500ms elapsed),
    // but the renewal at 250ms should have pushed it further out.
    const exists = await leases.exists(wf, step);
    expect(exists).toBe(true);
  }, 3000);

  it("leases for different steps are independent of each other", async () => {
    const { wf } = uniqueIds();
    await leases.acquire(wf, "step-x", "worker-a", 5000);
    const otherAcquired = await leases.acquire(wf, "step-y", "worker-b", 5000);
    expect(otherAcquired).toBe(true);
  });
});
