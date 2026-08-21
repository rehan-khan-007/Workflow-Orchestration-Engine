# Multi-stage build producing two runtime images (api, worker) from one
# shared compile step. Build each with:
#   docker build --target api    -t woe-api    .
#   docker build --target worker -t woe-worker .

# ---- Stage 1: compile TypeScript ----
FROM node:22-slim AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npm run build
RUN cp src/storage/schema.sql dist/storage/schema.sql

# ---- Stage 2: production-only node_modules (no devDependencies) ----
FROM node:22-slim AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ---- Stage 3a: API runtime ----
FROM node:22-slim AS api
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY package*.json ./
EXPOSE 3000
CMD ["node", "dist/api/index.js"]

# ---- Stage 3b: worker runtime ----
FROM node:22-slim AS worker
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY package*.json ./
CMD ["node", "dist/worker/main.js"]
