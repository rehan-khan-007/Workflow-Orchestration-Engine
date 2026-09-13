import { Pool } from "pg";

let pool: Pool | null = null;

/**
 * Returns a shared connection pool. Lazily created so importing this module
 * doesn't open a connection until something actually queries the DB.
 */
export function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString:
        process.env.DATABASE_URL ||
        "postgresql://app:app@localhost:5432/workflow_engine",
      // Managed cloud Postgres (Neon, Supabase, RDS, etc.) requires TLS;
      // local dev Postgres neither needs nor supports it. Driven by an
      // explicit env var rather than sniffing the connection string for a
      // hostname pattern — an explicit toggle is a documented choice, not
      // fragile string-matching that breaks for a provider we didn't
      // anticipate. `rejectUnauthorized: false` skips server-certificate
      // verification (the connection is still encrypted in transit) — a
      // reasonable simplification for a demo, not something to carry into
      // anything handling real sensitive data without revisiting.
      ssl: process.env.DATABASE_SSL === "true" ? { rejectUnauthorized: false } : undefined,
    });
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
