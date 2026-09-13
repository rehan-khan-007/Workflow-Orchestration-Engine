# Topic: API, auth, rate limiting, and deployment

Covers: `src/api/`, `docker/`, `docker-compose.yml`, `k8s/`, `demo/`.

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
  exactly (checked as `header === \`Bearer ${apiKey}\``), **or** a
  `?api_key=<apiKey>` query parameter matching exactly — either is
  accepted. Neither present/matching → `401 { error: "Missing or
  invalid API key" }`.
- The query-param path exists specifically because the browser's
  built-in `EventSource` API (used by `demo/index.html` for
  `GET /workflows/:id/stream`) cannot set custom headers at all — this
  is a real browser API limitation, not a weaker auth scheme added by
  accident. Tradeoff, stated directly in the source comment: a key
  passed as a query param can end up in server access logs or browser
  history, which a header never would. Judged acceptable for this
  project's threat model (single-operator tool, no untrusted multi-
  tenant users) — would need reconsidering for a service handling real
  secrets at scale.
- Applied via `app.use(requireApiKey(apiKey))` in `createApp()` — global
  middleware, but registered **after** the `/metrics` route handler is
  already defined earlier in the function, so `/metrics` never passes
  through it.

No OAuth, no per-key scoping, no key rotation, no RBAC — a single
shared secret model, sufficient for a single-operator tool, not a
multi-tenant product.

## CORS (`src/api/cors.ts`)

- Hand-rolled, not the `cors` npm package — same reasoning as
  hand-rolling the structured logger (`src/observability/logger.ts`):
  small enough that a dependency isn't worth it.
- Sets `Access-Control-Allow-Origin: *` on every response (including
  `/metrics`, verified directly — CORS is applied via
  `app.use(corsMiddleware)` before any route is registered), plus
  `Access-Control-Allow-Methods: GET, POST, OPTIONS` and
  `Access-Control-Allow-Headers: Content-Type, Authorization`, and
  responds `204` directly to `OPTIONS` preflight requests.
- **Did not exist before this API had a browser-based consumer**
  (`demo/index.html`). A page opened as a local file — or served from
  anywhere other than the API's own origin — would have every
  `fetch()` call silently blocked by the browser without this.
- Deliberately permissive (`*`, not an allowlist): this API has no
  cookie-based session for CORS to protect against CSRF on, and the
  bearer-token auth (when `API_KEY` is set) is never sent
  automatically by a browser the way a cookie would be — so an open
  origin policy doesn't weaken that auth. This reasoning is specific to
  this API's auth model; it would not automatically transfer to a
  cookie-authenticated service.
- **Verified in this investigation**: `tests/integration/cors.test.ts`
  (3 tests) — confirms the header on a real cross-origin-simulated GET,
  confirms the `OPTIONS` preflight response and its headers, and
  confirms `/metrics` gets the header too despite being registered
  before the auth/rate-limit middleware.

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

## Live demo page (`demo/index.html`)

- Single self-contained HTML/CSS/JS file — no build step, no external
  dependencies, no framework. Opened directly in a browser (`file://`
  or served any other way).
- Not a recording or mock: it `POST`s a real workflow to the API base
  URL entered in the page, then opens a real `EventSource` against
  `GET /workflows/:id/stream` and renders the live event stream as an
  SVG DAG graph (nodes colored/pulsing by real step status) plus a raw
  event-log panel.
- DAG layout is computed client-side: each step's depth = 1 + max(depth
  of its dependencies), 0 if none — a simple layered layout, not an
  external graph-layout library.
- Three built-in preset workloads (linear chain, diamond, wide fan-out)
  plus a raw-JSON editor for custom DAGs, matching the exact
  `{ name, steps: [{ id, dependsOn }] }` shape `POST /workflows`
  expects.
- Requires the two additions above (CORS, `?api_key=` fallback) to
  function at all against an API on a different origin or with
  `API_KEY` set — this page is *why* those two exist in this codebase,
  not an unrelated feature that happens to use them.
- **Verification boundary, stated plainly**: the page's actual visual
  rendering in a browser cannot be verified by an AI investigating this
  repo — only a human opening it can confirm that. What *can* be (and
  was) verified without a browser: every HTTP/SSE call the page's
  JavaScript makes was independently reproduced with `curl` against a
  real running API + worker pair, confirming the exact response/event
  JSON shapes the page's code parses. The two verification methods are
  complementary, not redundant — one confirms the network contract is
  correct, the other confirms the page actually renders it correctly.
