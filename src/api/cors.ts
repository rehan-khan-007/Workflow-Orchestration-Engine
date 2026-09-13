import { NextFunction, Request, Response } from "express";

/**
 * Minimal CORS middleware — allows any origin to call this API.
 *
 * No new dependency (the `cors` package) for something this small;
 * same reasoning as hand-rolling the structured logger. Deliberately
 * permissive (`*`) because this API has no cookie-based session to
 * protect against CSRF — auth (when enabled) is a bearer token the
 * browser never sends automatically, so an open CORS policy doesn't
 * weaken it. This is the right tradeoff for a local/demo tool, not a
 * multi-tenant production service with user sessions.
 */
export function corsMiddleware(req: Request, res: Response, next: NextFunction): void {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }
  next();
}
