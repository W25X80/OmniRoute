import type { Logger } from "pino";
import {
  parsePostgresConfiguration,
  postgresPoolConfiguration,
  type PostgresOptions,
} from "./config";
import { PostgresDatabase } from "./database";

/** Opt-in factory; importing this module neither loads pg nor opens a connection. */
export async function createPostgresDatabase(
  options: PostgresOptions,
  logger: Pick<Logger, "warn">
): Promise<PostgresDatabase> {
  const config = parsePostgresConfiguration(options);
  const { Pool } = await import("pg");
  return new PostgresDatabase(new Pool(postgresPoolConfiguration(config)), config, logger);
}
