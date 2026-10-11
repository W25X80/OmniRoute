import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { UsageEntry, UsageHistoryFilter } from "../../usage/usageHistory";
import {
  resolveUsageAccountIdentity,
  resolveOrphanedUsageAccountIdentity,
} from "../../usage/accountIdentity";
import {
  getLoggedInputTokens,
  getLoggedOutputTokens,
  getPromptCacheReadTokens,
  getPromptCacheCreationTokens,
  getReasoningTokens,
} from "../../usage/tokenAccounting";
import { normalizeServiceTier } from "../../usage/usageHistory/helpers";
import { resolveProviderId } from "@/shared/constants/providers";
import type { PostgresDatabase } from "./database";
import {
  lockUsage,
  queryUsageRows,
  safeUsageNumber,
  bindSelectedUsageRepository,
} from "./usageQueries";

export interface UsageSaveReceipt {
  id: number;
  inserted: boolean;
}
const USAGE_NUMBERS = [
  "id",
  "tokens_input",
  "tokens_output",
  "tokens_cache_read",
  "tokens_cache_creation",
  "tokens_reasoning",
  "success",
  "latency_ms",
  "ttft_ms",
  "account_label_priority",
];

function normalizeTimestamp(value?: string): string {
  const timestamp = value ? new Date(value) : new Date();
  if (!Number.isFinite(timestamp.getTime())) throw new Error("Invalid usage timestamp");
  return timestamp.toISOString();
}

function parseCursor(cursor: string): { timestamp: string; id: number } {
  if (!cursor.startsWith("usage:"))
    return { timestamp: normalizeTimestamp(cursor), id: Number.MAX_SAFE_INTEGER };
  const decoded: unknown = JSON.parse(Buffer.from(cursor.slice(6), "base64url").toString("utf8"));
  if (
    !Array.isArray(decoded) ||
    decoded.length !== 2 ||
    typeof decoded[0] !== "string" ||
    !Number.isSafeInteger(decoded[1]) ||
    decoded[1] < 0
  ) {
    throw new Error("Invalid usage cursor");
  }
  return { timestamp: normalizeTimestamp(decoded[0]), id: decoded[1] as number };
}

export class PostgresUsageHistoryRepository {
  constructor(private readonly database: PostgresDatabase) {}

  async save(entry: UsageEntry): Promise<UsageSaveReceipt> {
    const timestamp = normalizeTimestamp(entry.timestamp);
    const provider = entry.provider ? resolveProviderId(entry.provider) : null;
    const input = getLoggedInputTokens(entry.tokens);
    const output = getLoggedOutputTokens(entry.tokens);
    return this.database.transaction(async (transaction) => {
      await lockUsage(transaction);
      const connection = entry.connectionId
        ? (
            await transaction.query<Record<string, unknown>>(
              "SELECT * FROM omniroute.provider_connections WHERE id=$1",
              [entry.connectionId]
            )
          ).rows[0]
        : undefined;
      const account = connection
        ? resolveUsageAccountIdentity(connection)
        : resolveOrphanedUsageAccountIdentity(entry.provider, entry.connectionId);
      const identity = [
        timestamp,
        provider,
        entry.model || null,
        entry.connectionId || null,
        entry.apiKeyId || null,
        input,
        output,
      ];
      const existing = (
        await transaction.query<{
          id: string;
          endpoint: string | null;
          cpa_auth_index: string | null;
        }>(
          `SELECT id, endpoint, cpa_auth_index FROM omniroute.usage_history
          WHERE timestamp=$1 AND COALESCE(provider,'')=COALESCE($2,'')
          AND COALESCE(model,'')=COALESCE($3,'') AND COALESCE(connection_id,'')=COALESCE($4,'')
          AND COALESCE(api_key_id,'')=COALESCE($5,'') AND tokens_input=$6 AND tokens_output=$7
          ORDER BY id DESC LIMIT 1`,
          identity
        )
      ).rows[0];
      if (existing) {
        await transaction.query(
          `UPDATE omniroute.usage_history SET endpoint=COALESCE(NULLIF(endpoint,''), $2),
            cpa_auth_index=COALESCE(NULLIF(cpa_auth_index,''), $3) WHERE id=$1`,
          [existing.id, entry.endpoint || null, entry.cpaAuthIndex || null]
        );
        return { id: safeUsageNumber(existing.id), inserted: false };
      }
      const tier = normalizeServiceTier(entry.serviceTier ?? entry.service_tier);
      const result = await transaction.query<{ id: string }>(
        `INSERT INTO omniroute.usage_history(provider,model,connection_id,account_key,account_label,
          account_label_priority,api_key_id,api_key_name,tokens_input,tokens_output,tokens_cache_read,
          tokens_cache_creation,tokens_reasoning,service_tier,status,success,latency_ms,ttft_ms,error_code,
          combo_strategy,endpoint,cpa_auth_index,timestamp)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
          RETURNING id`,
        [
          provider,
          entry.model || null,
          entry.connectionId || null,
          account.accountKey,
          account.accountLabel,
          account.accountLabelPriority,
          entry.apiKeyId || null,
          entry.apiKeyName || null,
          input,
          output,
          getPromptCacheReadTokens(entry.tokens),
          getPromptCacheCreationTokens(entry.tokens),
          getReasoningTokens(entry.tokens),
          tier,
          entry.status || null,
          entry.success === false ? 0 : 1,
          Number.isFinite(Number(entry.latencyMs)) ? Number(entry.latencyMs) : 0,
          Number.isFinite(Number(entry.timeToFirstTokenMs))
            ? Number(entry.timeToFirstTokenMs)
            : Number.isFinite(Number(entry.latencyMs))
              ? Number(entry.latencyMs)
              : 0,
          entry.errorCode || null,
          entry.comboStrategy || entry.combo_strategy || null,
          entry.endpoint || null,
          entry.cpaAuthIndex || null,
          timestamp,
        ]
      );
      return { id: safeUsageNumber(result.rows[0].id), inserted: true };
    });
  }

  async page(
    since?: string | null,
    limit = 10000,
    cursor?: string | null
  ): Promise<{ rows: Record<string, unknown>[]; nextCursor: string | null }> {
    const maximum = Number.isSafeInteger(limit) && limit > 0 ? Math.min(limit, 10000) : 10000;
    const values: unknown[] = [];
    const conditions: string[] = [];
    if (since) {
      values.push(normalizeTimestamp(since));
      conditions.push(`timestamp >= $${values.length}`);
    }
    if (cursor) {
      const after = parseCursor(cursor);
      values.push(after.timestamp, after.id);
      conditions.push(`(timestamp,id) > ($${values.length - 1},$${values.length}::bigint)`);
    }
    values.push(maximum);
    const rows = await queryUsageRows<Record<string, unknown>>(
      this.database,
      `SELECT * FROM omniroute.usage_history ${conditions.length ? "WHERE " + conditions.join(" AND ") : ""}
        ORDER BY timestamp ASC,id ASC LIMIT $${values.length}`,
      values,
      USAGE_NUMBERS
    );
    const last = rows.at(-1);
    return {
      rows,
      nextCursor:
        rows.length === maximum && last
          ? "usage:" + Buffer.from(JSON.stringify([last.timestamp, last.id])).toString("base64url")
          : null,
    };
  }

  async history(filter: UsageHistoryFilter = {}): Promise<Record<string, unknown>[]> {
    const conditions: string[] = [];
    const values: unknown[] = [];
    const add = (column: string, operator: string, value: unknown) => {
      values.push(value);
      conditions.push(`${column} ${operator} $${values.length}`);
    };
    if (filter.provider) add("provider", "=", filter.provider);
    if (filter.model) add("model", "=", filter.model);
    if (filter.startDate) add("timestamp", ">=", new Date(filter.startDate).toISOString());
    if (filter.endDate) add("timestamp", "<=", new Date(filter.endDate).toISOString());
    return queryUsageRows(
      this.database,
      `SELECT * FROM omniroute.usage_history
      ${conditions.length ? "WHERE " + conditions.join(" AND ") : ""} ORDER BY timestamp ASC,id ASC`,
      values,
      USAGE_NUMBERS
    );
  }

  async latency(
    options: { provider?: string; model?: string },
    since: string,
    maxRows: number
  ): Promise<Record<string, unknown>[]> {
    const values: unknown[] = [since];
    const conditions = ["timestamp >= $1", "provider IS NOT NULL", "model IS NOT NULL"];
    if (options.provider) {
      values.push(options.provider);
      conditions.push(`provider=$${values.length}`);
    }
    if (options.model) {
      values.push(options.model);
      conditions.push(`model=$${values.length}`);
    }
    values.push(maxRows);
    return queryUsageRows(
      this.database,
      `SELECT provider,model,success,latency_ms,ttft_ms,tokens_output,tokens_reasoning
      FROM omniroute.usage_history WHERE ${conditions.join(" AND ")} ORDER BY timestamp DESC,id DESC LIMIT $${values.length}`,
      values,
      USAGE_NUMBERS
    );
  }

  async recentLogs(limit: number): Promise<Record<string, unknown>[]> {
    return queryUsageRows(
      this.database,
      `SELECT timestamp,model,provider,account,tokens_in,tokens_out,status
      FROM omniroute.call_logs ORDER BY timestamp DESC,id DESC LIMIT $1`,
      [limit],
      ["tokens_in", "tokens_out", "status"]
    );
  }
}

export function getSelectedUsageHistoryRepository(): PostgresUsageHistoryRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresUsageHistoryRepository(database))
    : undefined;
}
