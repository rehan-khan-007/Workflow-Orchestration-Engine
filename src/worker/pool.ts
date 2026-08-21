import { QueueConsumer } from "../queue/consumer";
import { DagCoordinator } from "../core/coordinator";
import { StepExecutor, defaultStepExecutor } from "./runner";

interface StepQueuePayload {
  workflowId: string;
  stepId: string;
}

/**
 * A pool of `size` independent Redis consumers, all listening on the same
 * queue. Redis's BRPOP hands each queued item to whichever consumer asks
 * next, so N consumers running concurrently gives real parallel execution
 * of independent steps — this replaces the old in-memory fake pool that
 * just faked concurrency with setTimeout and never touched Redis.
 */
export class WorkerPool {
  private consumers: QueueConsumer[] = [];

  constructor(
    private queueName: string,
    private coordinator: DagCoordinator,
    private size: number = 4,
    private executor: StepExecutor = defaultStepExecutor
  ) {}

  start(): void {
    for (let i = 0; i < this.size; i++) {
      const consumer = new QueueConsumer(this.queueName);
      // Fire-and-forget: start() runs its own internal loop until stop()/close().
      consumer.start(async (payload) => {
        const { workflowId, stepId } = payload as unknown as StepQueuePayload;
        try {
          await this.executor({ id: stepId, dependsOn: [], status: "running" });
          await this.coordinator.handleStepResult(workflowId, stepId, true);
        } catch {
          await this.coordinator.handleStepResult(workflowId, stepId, false);
        }
      });
      this.consumers.push(consumer);
    }
  }

  async stop(): Promise<void> {
    await Promise.all(this.consumers.map((c) => c.close()));
    this.consumers = [];
  }
}
