import { readFile } from "node:fs/promises";
import { parsePostgresConfiguration, type PostgresConfiguration } from "./config";

type Environment = Readonly<Record<string, string | undefined>>;

function integer(env: Environment, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined) return fallback;
  if (!/^\d+$/.test(raw)) throw new Error("Invalid PostgreSQL environment configuration");
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) throw new Error("Invalid PostgreSQL environment configuration");
  return value;
}

/** Reads explicit process configuration at boot; never discovers a local database or credentials. */
export async function readPostgresEnvironment(
  env: Environment = process.env
): Promise<PostgresConfiguration> {
  const tlsMode = env.OMNIROUTE_POSTGRES_TLS ?? "verify-full";
  if (tlsMode !== "verify-full" && tlsMode !== "disable-local") {
    throw new Error("Invalid PostgreSQL TLS configuration");
  }
  let ca: string | undefined;
  if (env.OMNIROUTE_POSTGRES_CA_FILE !== undefined) {
    if (!env.OMNIROUTE_POSTGRES_CA_FILE || tlsMode !== "verify-full") {
      throw new Error("Invalid PostgreSQL CA configuration");
    }
    try {
      ca = await readFile(env.OMNIROUTE_POSTGRES_CA_FILE, "utf8");
    } catch {
      throw new Error("PostgreSQL CA file could not be read");
    }
  }
  const connectionTimeoutMs = integer(env, "OMNIROUTE_POSTGRES_CONNECTION_TIMEOUT_MS", 3000);
  return parsePostgresConfiguration({
    host: env.OMNIROUTE_POSTGRES_HOST ?? "",
    port: integer(env, "OMNIROUTE_POSTGRES_PORT", 5432),
    database: env.OMNIROUTE_POSTGRES_DATABASE ?? "",
    user: env.OMNIROUTE_POSTGRES_USER ?? "",
    password: env.OMNIROUTE_POSTGRES_PASSWORD ?? "",
    tls: tlsMode === "verify-full" ? { mode: tlsMode, ...(ca ? { ca } : {}) } : { mode: tlsMode },
    maxConnections: integer(env, "OMNIROUTE_POSTGRES_MAX_CONNECTIONS", 16),
    connectionTimeoutMs,
    maxQueuedOperations: integer(env, "OMNIROUTE_POSTGRES_MAX_QUEUED_OPERATIONS", 256),
    admissionWaitTimeoutMs: integer(
      env,
      "OMNIROUTE_POSTGRES_ADMISSION_WAIT_TIMEOUT_MS",
      connectionTimeoutMs
    ),
    lockTimeoutMs: integer(env, "OMNIROUTE_POSTGRES_LOCK_TIMEOUT_MS", 1000),
    statementTimeoutMs: integer(env, "OMNIROUTE_POSTGRES_STATEMENT_TIMEOUT_MS", 5000),
    queryTimeoutMs: integer(env, "OMNIROUTE_POSTGRES_QUERY_TIMEOUT_MS", 6000),
    transactionTimeoutMs: integer(env, "OMNIROUTE_POSTGRES_TRANSACTION_TIMEOUT_MS", 15000),
    idleInTransactionTimeoutMs: integer(
      env,
      "OMNIROUTE_POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS",
      5000
    ),
  });
}
