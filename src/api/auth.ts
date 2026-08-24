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
    if (header !== expected) {
      res.status(401).json({ error: "Missing or invalid API key" });
      return;
    }
    next();
  };
}
