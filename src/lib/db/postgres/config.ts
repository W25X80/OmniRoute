import type { PoolConfig } from "pg";
import { z } from "zod";

const milliseconds = z.number().int().min(1).max(300_000);
const configurationSchema = z
  .object({
    host: z
      .string()
      .min(1)
      .regex(/^[a-zA-Z0-9.:-]+$/),
    port: z.number().int().min(1).max(65_535).default(5432),
    database: z.string().min(1),
    user: z.string().min(1),
    password: z.string().min(1),
    tls: z.discriminatedUnion("mode", [
      z.object({ mode: z.literal("verify-full"), ca: z.string().min(1).optional() }).strict(),
      z.object({ mode: z.literal("disable-local") }).strict(),
    ]),
    maxConnections: z.number().int().min(1).max(100).default(4),
    maxQueuedOperations: z.number().int().min(0).max(1024).default(0),
    admissionWaitTimeoutMs: milliseconds.optional(),
    connectionTimeoutMs: milliseconds.default(3_000),
    statementTimeoutMs: milliseconds.default(5_000),
    lockTimeoutMs: milliseconds.default(1_000),
    queryTimeoutMs: milliseconds.default(6_000),
    transactionTimeoutMs: milliseconds.default(15_000),
    idleInTransactionTimeoutMs: milliseconds.default(5_000),
  })
  .strict()
  .superRefine((config, ctx) => {
    if (
      config.tls.mode === "disable-local" &&
      config.host !== "127.0.0.1" &&
      config.host !== "::1"
    ) {
      ctx.addIssue({ code: "custom", message: "Plaintext requires a loopback IP address" });
    }
    if (
      config.lockTimeoutMs >= config.statementTimeoutMs ||
      config.statementTimeoutMs >= config.queryTimeoutMs ||
      config.queryTimeoutMs >= config.transactionTimeoutMs ||
      config.idleInTransactionTimeoutMs > config.transactionTimeoutMs
    ) {
      ctx.addIssue({ code: "custom", message: "Invalid timeout ordering" });
    }
  })
  .transform((config) => ({
    ...config,
    admissionWaitTimeoutMs: config.admissionWaitTimeoutMs ?? config.connectionTimeoutMs,
  }));

export type PostgresOptions = z.input<typeof configurationSchema>;
export type PostgresConfiguration = z.output<typeof configurationSchema>;

export function parsePostgresConfiguration(options: PostgresOptions): PostgresConfiguration {
  const parsed = configurationSchema.safeParse(options);
  if (!parsed.success) {
    // Zod issues may contain rejected credentials or connection strings.
    throw new Error("Invalid PostgreSQL configuration");
  }
  return parsed.data;
}

export function postgresPoolConfiguration(config: PostgresConfiguration): PoolConfig {
  return {
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    // No connectionString: its parameters can override node-postgres's ssl object.
    ssl:
      config.tls.mode === "verify-full"
        ? { rejectUnauthorized: true, ...(config.tls.ca ? { ca: config.tls.ca } : {}) }
        : false,
    application_name: "omniroute",
    max: config.maxConnections,
    connectionTimeoutMillis: config.connectionTimeoutMs,
    statement_timeout: config.statementTimeoutMs,
    lock_timeout: config.lockTimeoutMs,
    query_timeout: config.queryTimeoutMs,
    idle_in_transaction_session_timeout: config.idleInTransactionTimeoutMs,
    // PostgreSQL 17+. Bound abandoned server transactions independently of client timers.
    options: `-c transaction_timeout=${config.transactionTimeoutMs} -c timezone=UTC`,
    idleTimeoutMillis: 10_000,
    maxLifetimeSeconds: 300,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
  };
}
