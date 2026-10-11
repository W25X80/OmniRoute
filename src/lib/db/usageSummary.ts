import { getPostgresUsageDatabase, queryUsageRows, safeUsageNumber } from "./postgres/usageQueries";
import { getDbInstance } from "./core.ts";
import type { SqliteAdapter } from "./adapters/types.ts";

/**
 * Total input+output tokens for the current calendar month, across BOTH storage legs:
 *
 * 1. `daily_usage_summary` — the rolled-up aggregate (filled only by the retention
 *    cleanup path rolling up rows OLDER than `retention.usageHistory`, default 365 days),
 *    so the current month's rows never appear here until ~a year later (or the operator
 *    lowers retention).
 * 2. `usage_history` — the live per-request rows (written by `saveRequestUsage` with
 *    `tokens_input`/`tokens_output` and an ISO `timestamp`), which hold the current month's
 *    actual usage.
 *
 * #10381: reading only leg 1 always returned ~0 for the current month. No double-count:
 * a row is rolled into `daily_usage_summary` only immediately before being deleted from
 * `usage_history` by the same cleanup step. The analytics layer uses the same
 * reconciliation in `buildUnifiedSource`.
 */
export function sumUsageTokensThisMonth(db: SqliteAdapter = getDbInstance()): number {
  try {
    const row = db
      .prepare(
        `SELECT
           COALESCE((
             SELECT SUM(tokens_input + tokens_output)
             FROM usage_history
             WHERE timestamp >= strftime('%Y-%m-01T00:00:00.000Z','now')
               AND timestamp < strftime('%Y-%m-01T00:00:00.000Z','now','+1 month')
           ), 0) +
           COALESCE((
             SELECT SUM(total_input_tokens + total_output_tokens)
             FROM daily_usage_summary
             WHERE date >= strftime('%Y-%m-01', 'now')
           ), 0) AS used`
      )
      .get() as { used: number } | undefined;
    return row?.used ?? 0;
  } catch {
    return 0; // table may not exist yet on a fresh install — treat as 0 used
  }
}

export async function sumUsageTokensThisMonthAsync(): Promise<number> {
  const database = getPostgresUsageDatabase();
  if (!database) return sumUsageTokensThisMonth();
  const now = new Date(),
    start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)),
    end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));
  const row = (
    await queryUsageRows<{ used: string }>(
      database,
      `SELECT COALESCE((SELECT SUM(tokens_input+tokens_output)
  FROM omniroute.usage_history WHERE timestamp >= $1 AND timestamp < $2),0)+
  COALESCE((SELECT SUM(total_input_tokens+total_output_tokens) FROM omniroute.daily_usage_summary WHERE date >= $3
   AND NOT EXISTS (SELECT 1 FROM omniroute.native_usage_archive archived
    WHERE archived.provider=LOWER(daily_usage_summary.provider) AND archived.model=LOWER(daily_usage_summary.model)
     AND archived.date=daily_usage_summary.date)
   AND NOT EXISTS (SELECT 1 FROM omniroute.usage_history raw
    WHERE LOWER(raw.provider)=LOWER(daily_usage_summary.provider) AND LOWER(raw.model)=LOWER(daily_usage_summary.model)
     AND to_char(raw.timestamp::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD')=daily_usage_summary.date)),0)+
  COALESCE((SELECT SUM(total_input_tokens+total_output_tokens) FROM omniroute.native_usage_archive WHERE date >= $3),0) AS used`,
      [start.toISOString(), end.toISOString(), start.toISOString().slice(0, 10)]
    )
  )[0];
  return safeUsageNumber(row.used);
}
