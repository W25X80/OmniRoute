import type { PostgresDatabase } from "./database";
import { queryUsageRows } from "./usageQueries";

/** Uses the source status/null/error predicates and text window, without topology filtering. */
export async function queryProviderHealthTargetStats(
  database: PostgresDatabase,
  cutoff: string,
  providerFilter: string | null
): Promise<Record<string, unknown>[]> {
  const providerClause = providerFilter ? "AND c.provider = $2" : "";
  return queryUsageRows(
    database,
    `WITH log_targets AS (
        SELECT
          c.provider,
          COALESCE(c.connection_id, '') as "connectionId",
          COALESCE(c.model, c.requested_model, 'unknown') as model,
          c.status,
          c.duration,
          c.timestamp,
          c.id,
          c.error_summary
        FROM omniroute.call_logs c
        WHERE c.provider IS NOT NULL
          AND c.provider != '-'
          AND c.timestamp >= $1
          ${providerClause}
      ), ranked AS (
        SELECT
          provider,
          "connectionId",
          model,
          status,
          duration,
          timestamp,
          CASE
            WHEN (status IS NOT NULL AND (status < 200 OR status >= 400))
              OR error_summary IS NOT NULL
            THEN 1
            ELSE 0
          END as "isError",
          ROW_NUMBER() OVER (
            PARTITION BY provider, "connectionId", model
            ORDER BY timestamp COLLATE "C" DESC, id COLLATE "C" DESC
          ) as "latestRank",
          ROW_NUMBER() OVER (
            PARTITION BY provider, "connectionId", model,
              CASE
                WHEN (status IS NOT NULL AND (status < 200 OR status >= 400))
                  OR error_summary IS NOT NULL
                THEN 1
                ELSE 0
              END
            ORDER BY timestamp COLLATE "C" DESC, id COLLATE "C" DESC
          ) as "errorRank"
        FROM log_targets
      )
      SELECT
        provider,
        "connectionId",
        model,
        COUNT(*) as requests,
        SUM(CASE WHEN status >= 200 AND status < 400 THEN 1 ELSE 0 END) as successes,
        ROUND(AVG(duration)::numeric) as "avgLatencyMs",
        MAX(timestamp) as "lastRequestAt",
        MAX(
          CASE
            WHEN "isError" = 1
            THEN timestamp
            ELSE NULL
          END
        ) as "lastErrorAt",
        MAX(CASE WHEN "latestRank" = 1 THEN status ELSE NULL END) as "lastStatus",
        MAX(CASE WHEN "isError" = 1 AND "errorRank" = 1 THEN status ELSE NULL END) as "lastErrorStatus"
      FROM ranked
      GROUP BY provider, "connectionId", model`,
    providerFilter ? [cutoff, providerFilter] : [cutoff]
  );
}
