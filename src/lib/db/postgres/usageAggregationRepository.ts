import { createHash } from "node:crypto";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { PostgresDatabase } from "./database";
import {
  lockUsage,
  normalizeUsageRow,
  queryUsageRows,
  safeUsageNumber,
  bindSelectedUsageRepository,
} from "./usageQueries";

export interface UsageArchiveRow {
  id: number;
  provider: string | null;
  model: string | null;
  timestamp: string;
  service_tier: string | null;
  tokens_input: number;
  tokens_output: number;
  tokens_cache_read: number;
  tokens_cache_creation: number;
  tokens_reasoning: number;
}
export interface UsageCleanupReceipt {
  processed: number;
  inserted: number;
  deleted: number;
  errors: number;
}
export class UsageArchiveChangedError extends Error {
  constructor() {
    super("Usage archive snapshot changed; no rows were deleted");
    this.name = "UsageArchiveChangedError";
  }
}
const NUMBERS = [
  "id",
  "tokens_input",
  "tokens_output",
  "tokens_cache_read",
  "tokens_cache_creation",
  "tokens_reasoning",
];
const COLUMNS =
  "id,provider,model,timestamp,service_tier,tokens_input,tokens_output,tokens_cache_read,tokens_cache_creation,tokens_reasoning";
function digest(rows: UsageArchiveRow[]): string {
  return createHash("sha256").update(JSON.stringify(rows)).digest("hex");
}

export class PostgresUsageAggregationRepository {
  constructor(private readonly database: PostgresDatabase) {}
  async prune(
    before: string,
    price: (row: UsageArchiveRow) => Promise<number>
  ): Promise<UsageCleanupReceipt> {
    const cutoff = new Date(before);
    if (!Number.isFinite(cutoff.getTime())) throw new Error("Invalid usage retention cutoff");
    const receipt: UsageCleanupReceipt = { processed: 0, inserted: 0, deleted: 0, errors: 0 };
    // Each batch prices at most 500 rows outside a transaction and deletes exactly that snapshot.
    for (let batch = 0; batch < 100; batch++) {
      const rows = await queryUsageRows<UsageArchiveRow>(
        this.database,
        `SELECT ${COLUMNS} FROM omniroute.usage_history
    WHERE timestamp<$1 ORDER BY id LIMIT 500`,
        [cutoff.toISOString()],
        NUMBERS
      );
      if (!rows.length) break;
      const fingerprint = digest(rows);
      const summaries = new Map<
        string,
        {
          provider: string;
          model: string;
          date: string;
          requests: number;
          input: number;
          output: number;
          cost: number;
        }
      >();
      for (const row of rows) {
        const provider = row.provider?.toLowerCase() || "unknown",
          model = row.model?.toLowerCase() || "unknown",
          date = new Date(row.timestamp).toISOString().slice(0, 10);
        const key = JSON.stringify([provider, model, date]);
        const summary = summaries.get(key) ?? {
          provider,
          model,
          date,
          requests: 0,
          input: 0,
          output: 0,
          cost: 0,
        };
        summary.requests++;
        summary.input += row.tokens_input ?? 0;
        summary.output += row.tokens_output ?? 0;
        summary.cost += await price(row);
        summaries.set(key, summary);
      }
      const inserted = await this.database.transaction(async (transaction) => {
        await lockUsage(transaction);
        const current = (
          await transaction.query<UsageArchiveRow>(
            `SELECT ${COLUMNS} FROM omniroute.usage_history
     WHERE id=ANY($1::bigint[]) ORDER BY id FOR UPDATE`,
            [rows.map((row) => row.id)]
          )
        ).rows.map((row) => normalizeUsageRow(row, NUMBERS));
        if (digest(current) !== fingerprint) throw new UsageArchiveChangedError();
        for (const summary of summaries.values()) {
          safeUsageNumber(summary.input);
          safeUsageNumber(summary.output);
          safeUsageNumber(summary.cost);
          if (summary.cost < 0) throw new Error("Invalid archived usage cost");
          await transaction.query(
            `INSERT INTO omniroute.native_usage_archive
      (provider,model,date,total_requests,total_input_tokens,total_output_tokens,total_cost)
      VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(provider,model,date) DO UPDATE SET
      total_requests=omniroute.native_usage_archive.total_requests+excluded.total_requests,
      total_input_tokens=omniroute.native_usage_archive.total_input_tokens+excluded.total_input_tokens,
      total_output_tokens=omniroute.native_usage_archive.total_output_tokens+excluded.total_output_tokens,
      total_cost=omniroute.native_usage_archive.total_cost+excluded.total_cost`,
            [
              summary.provider,
              summary.model,
              summary.date,
              summary.requests,
              summary.input,
              summary.output,
              summary.cost,
            ]
          );
        }
        const deleted = await transaction.query(
          "DELETE FROM omniroute.usage_history WHERE id=ANY($1::bigint[])",
          [rows.map((row) => row.id)]
        );
        if (safeUsageNumber(deleted.rowCount) !== rows.length) throw new UsageArchiveChangedError();
        return summaries.size;
      });
      receipt.processed += rows.length;
      receipt.deleted += rows.length;
      receipt.inserted += inserted;
      if (rows.length < 500) break;
    }
    return receipt;
  }
  async rollupQuota(
    period: "daily" | "hourly",
    from: string,
    to: string
  ): Promise<{ processed: number; inserted: number; errors: number }> {
    const column = period === "daily" ? "date" : "date_hour";
    const format = period === "daily" ? "YYYY-MM-DD" : "YYYY-MM-DD HH24:00:00";
    const window =
      period === "daily"
        ? `to_char(created_at::timestamptz AT TIME ZONE 'UTC','${format}') >= $1 AND to_char(created_at::timestamptz AT TIME ZONE 'UTC','${format}') <= $2`
        : "created_at >= $1 AND created_at <= $2";
    const result = await this.database.transaction((transaction) =>
      transaction.query(
        `INSERT INTO omniroute.${period}_usage_summary
   (provider,model,${column},total_requests,total_input_tokens,total_output_tokens,total_cost)
   SELECT provider,COALESCE(raw_data::jsonb->>'model','unknown'),to_char(created_at::timestamptz AT TIME ZONE 'UTC','${format}'),
    COUNT(*),COALESCE(SUM(trunc((raw_data::jsonb->>'input_tokens')::numeric)::bigint),0),
    COALESCE(SUM(trunc((raw_data::jsonb->>'output_tokens')::numeric)::bigint),0),COALESCE(SUM((raw_data::jsonb->>'cost')::double precision),0)
   FROM omniroute.quota_snapshots WHERE ${window}
   GROUP BY provider,COALESCE(raw_data::jsonb->>'model','unknown'),to_char(created_at::timestamptz AT TIME ZONE 'UTC','${format}')
   ON CONFLICT(provider,model,${column}) DO UPDATE SET total_requests=excluded.total_requests,
    total_input_tokens=excluded.total_input_tokens,total_output_tokens=excluded.total_output_tokens,total_cost=excluded.total_cost`,
        [from, to]
      )
    );
    return { processed: result.rowCount ?? 0, inserted: result.rowCount ?? 0, errors: 0 };
  }
}
export function getSelectedUsageAggregationRepository():
  PostgresUsageAggregationRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresUsageAggregationRepository(database))
    : undefined;
}
