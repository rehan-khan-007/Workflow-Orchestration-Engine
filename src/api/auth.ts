import { NextFunction, Request, RequestHandler, Response } from "express";

/**
 * Builds an auth middleware requiring a matching
 * `Authorization: Bearer <apiKey>` header on every request.
 *
 * If apiKey is undefined, returns a no-op middleware — auth is entirely
 * disabled. This is the deliberate default for local dev/demo use; set
 * API_KEY to actually require it. See src/config.ts for the reasoning.
 */
export function requireApiKey(apiKey: string | undefined): RequestHandler {
  if (!apiKey) {
    return (_req: Request, _res: Response, next: NextFunction) => next();
  }

  return (req: Request, res: Response, next: NextFunction) => {
    const header = req.header("Authorization");
    const expected = `Bearer ${apiKey}`;
    // Fallback: the browser's built-in EventSource API (used by
    // GET /workflows/:id/stream) cannot set custom headers at all — this
    // is a real, unavoidable browser API limitation, not a design choice
    // here. A `?api_key=` query param is the standard workaround for
    // exactly this problem in long-lived streaming connections. Real
    // tradeoff, stated plainly: a key passed this way can end up in
    // server access logs or browser history, which a header never
    // would. Acceptable for this project's threat model (a single-
    // operator demo/portfolio tool, not a multi-tenant service with
    // untrusted users); would need reconsidering for anything handling
    // real secrets at scale.
    const queryKey = typeof req.query.api_key === "string" ? req.query.api_key : undefined;
    if (header === expected || queryKey === apiKey) {
      next();
      return;
    }
    res.status(401).json({ error: "Missing or invalid API key" });
  };
}
