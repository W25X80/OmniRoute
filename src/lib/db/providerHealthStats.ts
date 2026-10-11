import { toNumber } from "@/shared/utils/numeric";
import { getDbInstance } from "./core";
import { getPostgresUsageDatabase } from "./postgres/usageQueries";
import { queryProviderHealthTargetStats } from "./postgres/providerHealthStatsRepository";

type JsonRecord = Record<string, unknown>;

export interface CallLogTargetStats {
  provider: string;
  connectionId: string | null;
  model: string;
  requests: number;
  successes: number;
  avgLatencyMs: number | null;
  lastRequestAt: string | null;
  lastErrorAt: string | null;
  lastStatus: number | null;
  lastErrorStatus: number | null;
}

function toString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function queryProviderHealthTargetStatsSqlite(
  cutoff: string,
  providerFilter: string | null
): JsonRecord[] {
  const db = getDbInstance();
  const providerClause = providerFilter ? "AND c.provider = @provider" : "";
  const params = providerFilter ? { cutoff, provider: providerFilter } : { cutoff };
  const rows = db
    .prepare(
      `WITH log_targets AS (
        SELECT
          c.provider,
          COALESCE(c.connection_id, '') as connectionId,
          COALESCE(c.model, c.requested_model, 'unknown') as model,
          c.status,
          c.duration,
          c.timestamp,
          c.id,
          c.error_summary
        FROM call_logs c
        WHERE c.provider IS NOT NULL
          AND c.provider != '-'
          AND c.timestamp >= @cutoff
          ${providerClause}
      ), ranked AS (
        SELECT
          provider,
          connectionId,
          model,
          status,
          duration,
          timestamp,
          CASE
            WHEN (status IS NOT NULL AND (status < 200 OR status >= 400))
              OR error_summary IS NOT NULL
            THEN 1
            ELSE 0
          END as isError,
          ROW_NUMBER() OVER (
            PARTITION BY provider, connectionId, model
            ORDER BY timestamp DESC, id DESC
          ) as latestRank,
          ROW_NUMBER() OVER (
            PARTITION BY provider, connectionId, model,
              CASE
                WHEN (status IS NOT NULL AND (status < 200 OR status >= 400))
                  OR error_summary IS NOT NULL
                THEN 1
                ELSE 0
              END
            ORDER BY timestamp DESC, id DESC
          ) as errorRank
        FROM log_targets
      )
      SELECT
        provider,
        connectionId,
        model,
        COUNT(*) as requests,
        SUM(CASE WHEN status >= 200 AND status < 400 THEN 1 ELSE 0 END) as successes,
        ROUND(AVG(duration)) as avgLatencyMs,
        MAX(timestamp) as lastRequestAt,
        MAX(
          CASE
            WHEN isError = 1
            THEN timestamp
            ELSE NULL
          END
        ) as lastErrorAt,
        MAX(CASE WHEN latestRank = 1 THEN status ELSE NULL END) as lastStatus,
        MAX(CASE WHEN isError = 1 AND errorRank = 1 THEN status ELSE NULL END) as lastErrorStatus
      FROM ranked
      GROUP BY provider, connectionId, model`
    )
    .all(params) as JsonRecord[];

  return rows;
}

/** Windowed call health by raw provider, connection and model; retains historical accounts. */
export async function getProviderHealthTargetStatsAsync(
  cutoff: string,
  providerFilter: string | null
): Promise<CallLogTargetStats[]> {
  const database = getPostgresUsageDatabase();
  const rows = database
    ? await queryProviderHealthTargetStats(database, cutoff, providerFilter)
    : queryProviderHealthTargetStatsSqlite(cutoff, providerFilter);
  return rows.map((row) => {
    const connectionId = toString(row.connectionId);
    return {
      provider: toString(row.provider) || "unknown",
      connectionId: connectionId || null,
      model: toString(row.model) || "unknown",
      requests: toNumber(row.requests),
      successes: toNumber(row.successes),
      avgLatencyMs: row.avgLatencyMs == null ? null : toNumber(row.avgLatencyMs),
      lastRequestAt: toString(row.lastRequestAt),
      lastErrorAt: toString(row.lastErrorAt),
      lastStatus: row.lastStatus == null ? null : toNumber(row.lastStatus),
      lastErrorStatus: row.lastErrorStatus == null ? null : toNumber(row.lastErrorStatus),
    };
  });
}
