import { readFileSync } from "fs";
import { join } from "path";
import { getPool, closePool } from "./db";

async function migrate(): Promise<void> {
  const sql = readFileSync(join(__dirname, "schema.sql"), "utf-8");
  const pool = getPool();
  await pool.query(sql);
  console.log("Migration applied successfully.");
}

migrate()
  .catch((err) => {
    console.error("Migration failed:", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
