# Topic: API, auth, rate limiting, and deployment

Covers: `src/api/`, `docker/`, `docker-compose.yml`, `k8s/`.

## REST API routes (`src/api/server.ts`)

Exact routes, from direct source inspection:
- `POST /workflows` — create + start a workflow
- `GET /workflows` — list all
- `GET /workflows/:id` — single workflow status
- `POST /workflows/:id/cancel`
- `GET /workflows/:id/stream` — Server-Sent Events, backed by Redis
  Pub/Sub (`src/queue/eventBus.ts`)
- `GET /dead-letters`
- `GET /metrics` — Prometheus text format, deliberately outside auth
  and rate limiting (see below)

## Auth (`src/api/auth.ts`)

`requireApiKey(apiKey: string | undefined): RequestHandler`:
- If `apiKey` is `undefined` (i.e., `API_KEY` env var unset), returns a
  no-op middleware — **auth is fully disabled**. This is the default.
- If set, every request must carry `Authorization: Bearer <apiKey>`
  exactly (checked as `header === \`Bearer ${apiKey}\``) or gets
  `401 { error: "Missing or invalid API key" }`.
- Applied via `app.use(requireApiKey(apiKey))` in `createApp()` — global
  middleware, but registered **after** the `/metrics` route handler is
  already defined earlier in the function, so `/metrics` never passes
  through it.

No OAuth, no per-key scoping, no key rotation, no RBAC — a single
shared secret model, sufficient for a single-operator tool, not a
multi-tenant product.

## Rate limiting (`src/api/server.ts`)

`express-rate-limit`, configured via `createApp()`'s optional
`rateLimitOptions` parameter:
- Default: `windowMs: 60_000` (1 minute), `max: 100` requests per IP.
- `standardHeaders: true`, `legacyHeaders: false`.
- On limit exceeded: `429 { error: "Too many requests, please try
  again later." }`.
- Applied globally via `app.use(limiter)`, registered before
  `requireApiKey` — but, like auth, `/metrics`'s route handler is
  defined earlier in the function and is not behind this middleware
  either.

**Verified in this investigation**: `tests/integration/auth.test.ts`
includes a test that deliberately configures `max: 3` for a dedicated
test app instance and confirms the 4th and 5th requests get `429`
while the first three get `200` — this is a real behavioral test, not
just a config value asserted to exist.

## Docker (`docker/Dockerfile`)

Multi-stage build, 4 stages total:
1. `builder` (`node:22-slim`) — compiles TypeScript.
2. `deps` (`node:22-slim`) — production-only `node_modules` (no
   devDependencies).
3. `api` (`node:22-slim`) — runtime image for the API process.
4. `worker` (`node:22-slim`) — runtime image for the worker process.

Built independently:
```bash
docker build --target api    -t woe-api    .
docker build --target worker -t woe-worker .
```

`docker-compose.yml` services (confirmed via direct grep): `redis`,
`postgres`, `migrate` (one-shot job, not a long-running service),
`api`, `worker`.

## Kubernetes (`k8s/*.yaml`)

5 manifest files, confirmed `Deployment` resources and their exact
`metadata.name` values:
| File | Deployment name |
|---|---|
| `01-postgres.yaml` | `postgres` |
| `02-redis.yaml` | `redis` |
| `04-api.yaml` | `woe-api` |
| `05-worker.yaml` | `woe-worker` |

`03-migrate-job.yaml` is a `Job`, not a `Deployment` (one-shot schema
migration, consistent with the docker-compose `migrate` service).
`00-namespace-config.yaml` holds the namespace + ConfigMap
(`WORKER_METRICS_PORT` is set here per MASTER.md's config reference).

**Not independently re-verified in this investigation**: whether these
manifests still `kubectl apply` cleanly against a live cluster today.
The engineering log and README describe this having been done
successfully against Minikube during development (including a live
`kubectl scale --replicas=6` demonstration), but that was not
re-executed as part of producing this handoff doc.

## Local dev port remaps

`.env.example` in this snapshot points at `localhost:5433` (Postgres)
and `localhost:6380` (Redis) — non-default ports, chosen specifically
to avoid colliding with another local project's containers running on
the standard 5432/6379 on the same developer machine. This is a
developer-machine convenience, not a requirement of the application
itself — any reachable Postgres/Redis works, set via `DATABASE_URL` /
`REDIS_URL`.
