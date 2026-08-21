import express, { Express, NextFunction, Request, Response } from "express";
import { WorkflowEngine } from "../core/engine";
import { DagCoordinator } from "../core/coordinator";
import { DagScheduler } from "../scheduler/scheduler";
import { EventBus, WorkflowEvent } from "../queue/eventBus";
import { Step, Workflow } from "../types";

type AsyncHandler = (req: Request, res: Response) => Promise<void>;

/**
 * Wraps an async route handler so a rejected promise reaches Express's
 * error handling instead of hanging the request forever — Express 4
 * does not do this automatically for async handlers.
 */
function asyncRoute(handler: AsyncHandler) {
  return (req: Request, res: Response, next: NextFunction) => {
    handler(req, res).catch(next);
  };
}

/**
 * Builds the Express app without starting it listening — kept separate
 * from src/api/index.ts so tests can spin the app up on an ephemeral
 * port without going through the CLI entrypoint.
 */
export function createApp(
  engine: WorkflowEngine,
  scheduler: DagScheduler,
  coordinator: DagCoordinator,
  eventBus: EventBus
): Express {
  const app = express();
  app.use(express.json());

  /**
   * Looks up a workflow by id, treating any lookup failure — including
   * Postgres rejecting a malformed id that isn't valid UUID syntax — the
   * same as "not found". From the API consumer's point of view a bad id
   * and a nonexistent id should both just be a 404, not a 500.
   */
  async function findWorkflowOr404(id: string, res: Response): Promise<Workflow | undefined> {
    let workflow: Workflow | undefined;
    try {
      workflow = await engine.getWorkflow(id);
    } catch {
      workflow = undefined;
    }
    if (!workflow) {
      res.status(404).json({ error: `No workflow with id ${id}` });
    }
    return workflow;
  }

  app.post(
    "/workflows",
    asyncRoute(async (req, res) => {
      const { name, steps } = req.body as { name?: string; steps?: Step[] };
      if (!name || !Array.isArray(steps)) {
        res
          .status(400)
          .json({ error: "Request body must include 'name' (string) and 'steps' (array)." });
        return;
      }
      const workflow = await engine.createWorkflow(name, steps);
      await scheduler.schedule(workflow);
      const started = await engine.getWorkflow(workflow.id);
      res.status(201).json(started);
    })
  );

  app.get(
    "/workflows",
    asyncRoute(async (_req, res) => {
      const workflows = await engine.listWorkflows();
      res.json(workflows);
    })
  );

  app.get(
    "/workflows/:id",
    asyncRoute(async (req, res) => {
      const workflow = await findWorkflowOr404(req.params.id, res);
      if (!workflow) return;
      res.json(workflow);
    })
  );

  app.post(
    "/workflows/:id/cancel",
    asyncRoute(async (req, res) => {
      const workflow = await findWorkflowOr404(req.params.id, res);
      if (!workflow) return;
      await coordinator.cancel(req.params.id);
      const updated = await engine.getWorkflow(req.params.id);
      res.json(updated);
    })
  );

  // Server-Sent Events: live step/workflow status updates for one workflow,
  // fed by the same Redis Pub/Sub channel the coordinator publishes to.
  app.get(
    "/workflows/:id/stream",
    asyncRoute(async (req, res) => {
      const workflowId = req.params.id;
      const workflow = await findWorkflowOr404(workflowId, res);
      if (!workflow) return;

      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      const send = (event: WorkflowEvent | { type: "snapshot"; workflow: Workflow }) => {
        res.write(`data: ${JSON.stringify(event)}\n\n`);
      };

      send({ type: "snapshot", workflow });

      const sub = eventBus.subscribe(workflowId, (event) => {
        send(event);
        if (event.type === "workflow_status" && event.status !== "running") {
          // Terminal state reached — close the stream, nothing more will happen.
          sub.quit().catch(() => {});
          res.end();
        }
      });

      req.on("close", () => {
        sub.quit().catch(() => {});
      });
    })
  );

  // Fallback error handler: anything that reaches here is unexpected
  // (not the "malformed id" case above, which is handled as 404).
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}
