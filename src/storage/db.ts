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
