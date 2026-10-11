import { getSelectedPostgresDatabase, getSelectedControlPlane } from "../controlPlaneContext";
import type { PostgresDatabase, PostgresTransaction } from "./database";

export function getPostgresUsageDatabase(): PostgresDatabase | undefined {
  return getSelectedPostgresDatabase();
}

/** Binds fixed repository SQL; this does not translate SQL or expose a driver to callers. */
export function bindUsageParameters(
  text: string,
  parameters: Record<string, unknown>
): { text: string; values: unknown[] } {
  const names: string[] = [];
  const bound = text.replace(/@([a-zA-Z_][a-zA-Z0-9_]*)/g, (_, name: string) => {
    if (!Object.hasOwn(parameters, name)) throw new Error("Missing usage query parameter");
    let position = names.indexOf(name);
    if (position === -1) {
      names.push(name);
      position = names.length - 1;
    }
    return `$${position + 1}`;
  });
  return { text: bound, values: names.map((name) => parameters[name]) };
}

export function safeUsageNumber(value: unknown): number {
  const number = Number(value);
  if (!Number.isFinite(number) || Math.abs(number) > Number.MAX_SAFE_INTEGER) {
    throw new Error("Usage value exceeds the supported numeric range");
  }
  return number;
}

export function normalizeUsageRow<Row>(row: Row, fields: readonly string[]): Row {
  const mapped = { ...row } as Record<string, unknown>;
  for (const field of fields) {
    if (mapped[field] !== null && mapped[field] !== undefined) {
      mapped[field] = safeUsageNumber(mapped[field]);
    }
  }
  return mapped as Row;
}

export async function queryUsageRows<Row>(
  database: PostgresDatabase,
  text: string,
  values: unknown[] = [],
  numericFields: readonly string[] = []
): Promise<Row[]> {
  return database.transaction(async (transaction) => {
    const result = await transaction.query<Row>(text, values);
    return result.rows.map((row) => normalizeUsageRow(row, numericFields));
  });
}

export async function queryNamedUsageRows<Row>(
  database: PostgresDatabase,
  text: string,
  parameters: Record<string, unknown>,
  numericFields: readonly string[] = []
): Promise<Row[]> {
  const bound = bindUsageParameters(text, parameters);
  return queryUsageRows<Row>(database, bound.text, bound.values, numericFields);
}

export async function lockUsage(transaction: PostgresTransaction): Promise<void> {
  await transaction.query("SELECT pg_advisory_xact_lock(18765, 19)");
}

export const USAGE_NUMERIC_FIELDS = [
  "count",
  "cnt",
  "total",
  "today",
  "errors",
  "cached",
  "avg_duration",
  "n",
  "totalRequests",
  "totalSuccesses",
  "successfulRequests",
  "requests",
  "successes",
  "latencySamples",
  "avgLatencyMs",
  "avg_latency_ms",
  "totalTokens",
  "totalTokensIn",
  "totalTokensOut",
  "promptTokens",
  "completionTokens",
  "cacheReadTokens",
  "cacheCreationTokens",
  "reasoningTokens",
  "storedCost",
  "isAggregated",
  "days",
  "with_requested",
  "fallback_eligible",
  "fallbacks",
  "inputTokens",
  "outputTokens",
  "successCount",
  "cacheRead",
  "cacheCreation",
  "reasoning",
  "input",
  "output",
  "used",
  "uniqueModels",
  "uniqueAccounts",
  "uniqueApiKeys",
  "tokens_input",
  "tokens_output",
  "tokens_cache_read",
  "tokens_cache_creation",
  "tokens_reasoning",
  "latency_ms",
  "ttft_ms",
  "id",
  "cost",
  "total_input_tokens",
  "total_output_tokens",
  "total_requests",
  "total_cost",
  "requestCount",
  "entryCount",
  "totalCost",
  "rowId",
  "budget_reset_at",
  "last_budget_reset_at",
  "warning_emitted_at",
  "warning_period_start",
];

/** Factories bind methods to their admitted operation; retaining one cannot escape a revoked scope. */
export function bindSelectedUsageRepository<T extends object>(repository: T): T {
  const binding = getSelectedControlPlane();
  if (!binding) throw new Error("PostgreSQL usage owner is not selected");
  return new Proxy(repository, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function"
        ? (...args: unknown[]) => {
            binding.assertReady();
            return value.apply(target, args);
          }
        : value;
    },
  });
}
