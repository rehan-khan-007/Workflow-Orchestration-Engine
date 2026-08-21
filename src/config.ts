/**
 * Shared constants between the API process and worker process. Both are
 * meant to run independently (and, from Phase 5 on, in separate
 * containers/pods) but must agree on the queue name and pub/sub channel
 * prefix to actually talk to each other via Redis.
 */
export const QUEUE_NAME = process.env.QUEUE_NAME || "workflow-steps";
export const API_PORT = Number(process.env.PORT) || 3000;
export const WORKER_POOL_SIZE = Number(process.env.WORKER_POOL_SIZE) || 4;
