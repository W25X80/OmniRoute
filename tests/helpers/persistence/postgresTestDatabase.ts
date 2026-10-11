import { readFile } from "node:fs/promises";
import { createPostgresDatabase } from "../../../src/lib/db/postgres/connect.ts";
import type { PostgresOptions } from "../../../src/lib/db/postgres/config.ts";

const port = Number(process.env.OMNIROUTE_TEST_POSTGRES_PORT);
const caFile = process.env.OMNIROUTE_TEST_POSTGRES_CA;
export const postgresTestsEnabled =
  Number.isInteger(port) && port > 0 && port < 65_536 && Boolean(caFile);

export async function createTestPostgresDatabase(options: Partial<PostgresOptions> = {}) {
  return createPostgresDatabase(await testPostgresOptions(options), { warn() {} });
}

export async function testPostgresOptions(
  options: Partial<PostgresOptions> = {}
): Promise<PostgresOptions> {
  if (!postgresTestsEnabled) throw new Error("Isolated PostgreSQL test server is not configured");
  return {
    host: "localhost",
    port,
    database: "omniroute_test",
    user: "omniroute_test",
    password: "omniroute_test",
    tls: { mode: "verify-full", ca: await readFile(caFile!, "utf8") },
    ...options,
  };
}
