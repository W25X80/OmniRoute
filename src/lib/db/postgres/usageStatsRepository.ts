import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { PostgresDatabase } from "./database";
import { queryUsageRows, safeUsageNumber, bindSelectedUsageRepository } from "./usageQueries";
const NUMBERS = [
  "request_count",
  "tokens_input",
  "tokens_output",
  "tokens_cache_read",
  "tokens_cache_creation",
  "tokens_reasoning",
  "cost_tokens_input",
  "cost_tokens_output",
  "cost_tokens_cache_read",
  "cost_tokens_cache_creation",
  "cost_tokens_reasoning",
  "stored_cost",
  "input",
  "output",
  "cacheRead",
  "cacheCreation",
  "reasoning",
  "requests",
];
function buildUsageSourceSql(aggregationEnabled: boolean) {
  if (!aggregationEnabled) {
    return `
      SELECT
        provider,
        model,
        timestamp,
        connection_id,
        api_key_id,
        api_key_name,
        COALESCE(tokens_input, 0) as tokens_input,
        COALESCE(tokens_output, 0) as tokens_output,
        COALESCE(tokens_cache_read, 0) as tokens_cache_read,
        COALESCE(tokens_cache_creation, 0) as tokens_cache_creation,
        COALESCE(tokens_reasoning, 0) as tokens_reasoning,
        COALESCE(tokens_input, 0) as cost_tokens_input,
        COALESCE(tokens_output, 0) as cost_tokens_output,
        COALESCE(tokens_cache_read, 0) as cost_tokens_cache_read,
        COALESCE(tokens_cache_creation, 0) as cost_tokens_cache_creation,
        COALESCE(tokens_reasoning, 0) as cost_tokens_reasoning,
        0.0 as stored_cost,
        COALESCE(service_tier, 'standard') as service_tier,
        1 as request_count
      FROM omniroute.usage_history
      UNION ALL SELECT provider,model,date || 'T12:00:00.000Z',NULL,NULL,NULL,total_input_tokens,total_output_tokens,
       0,0,0,0,0,0,0,0,total_cost,'standard',total_requests FROM omniroute.native_usage_archive
    `;
  }

  return `
    SELECT
      provider,
      model,
      timestamp,
      connection_id,
      api_key_id,
      api_key_name,
      COALESCE(tokens_input, 0) as tokens_input,
      COALESCE(tokens_output, 0) as tokens_output,
      COALESCE(tokens_cache_read, 0) as tokens_cache_read,
      COALESCE(tokens_cache_creation, 0) as tokens_cache_creation,
      COALESCE(tokens_reasoning, 0) as tokens_reasoning,
      COALESCE(tokens_input, 0) as cost_tokens_input,
      COALESCE(tokens_output, 0) as cost_tokens_output,
      COALESCE(tokens_cache_read, 0) as cost_tokens_cache_read,
      COALESCE(tokens_cache_creation, 0) as cost_tokens_cache_creation,
      COALESCE(tokens_reasoning, 0) as cost_tokens_reasoning,
      0.0 as stored_cost,
      COALESCE(service_tier, 'standard') as service_tier,
      1 as request_count
    FROM omniroute.usage_history

    UNION ALL

    SELECT
      provider,
      model,
      date || 'T12:00:00.000Z' as timestamp,
      NULL as connection_id,
      NULL as api_key_id,
      NULL as api_key_name,
      COALESCE(total_input_tokens, 0) as tokens_input,
      COALESCE(total_output_tokens, 0) as tokens_output,
      0 as tokens_cache_read,
      0 as tokens_cache_creation,
      0 as tokens_reasoning,
      0 as cost_tokens_input,
      0 as cost_tokens_output,
      0 as cost_tokens_cache_read,
      0 as cost_tokens_cache_creation,
      0 as cost_tokens_reasoning,
      COALESCE(total_cost, 0.0) as stored_cost,
      'standard' as service_tier,
      COALESCE(total_requests, 0) as request_count
    FROM omniroute.daily_usage_summary
    WHERE date < $1
      AND NOT EXISTS (SELECT 1 FROM omniroute.native_usage_archive archived
        WHERE archived.provider=LOWER(daily_usage_summary.provider)
          AND archived.model=LOWER(daily_usage_summary.model)
          AND archived.date=daily_usage_summary.date)
      AND NOT EXISTS (SELECT 1 FROM omniroute.usage_history raw
        WHERE LOWER(raw.provider)=LOWER(daily_usage_summary.provider)
          AND LOWER(raw.model)=LOWER(daily_usage_summary.model)
          AND to_char(raw.timestamp::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD')=daily_usage_summary.date)
    UNION ALL SELECT provider,model,date || 'T12:00:00.000Z',NULL,NULL,NULL,total_input_tokens,total_output_tokens,
     0,0,0,0,0,0,0,0,total_cost,'standard',total_requests FROM omniroute.native_usage_archive
  `;
}

const AGGREGATE_FIELDS = `
  SUM(request_count) as request_count,
  COALESCE(SUM(tokens_input), 0) as tokens_input,
  COALESCE(SUM(tokens_output), 0) as tokens_output,
  COALESCE(SUM(tokens_cache_read), 0) as tokens_cache_read,
  COALESCE(SUM(tokens_cache_creation), 0) as tokens_cache_creation,
  COALESCE(SUM(tokens_reasoning), 0) as tokens_reasoning,
  COALESCE(SUM(cost_tokens_input), 0) as cost_tokens_input,
  COALESCE(SUM(cost_tokens_output), 0) as cost_tokens_output,
  COALESCE(SUM(cost_tokens_cache_read), 0) as cost_tokens_cache_read,
  COALESCE(SUM(cost_tokens_cache_creation), 0) as cost_tokens_cache_creation,
  COALESCE(SUM(cost_tokens_reasoning), 0) as cost_tokens_reasoning,
  COALESCE(SUM(stored_cost), 0.0) as stored_cost,
  MAX(timestamp) as last_used
`;

export class PostgresUsageStatsRepository {
  constructor(private readonly database: PostgresDatabase) {}
  async monthlyTokens(provider: string, connectionId: string, monthStart: string): Promise<number> {
    return safeUsageNumber(
      (
        await queryUsageRows<{ total: string }>(
          this.database,
          `SELECT COALESCE(SUM(tokens_input),0)+COALESCE(SUM(tokens_output),0)+
   COALESCE(SUM(tokens_cache_read),0)+COALESCE(SUM(tokens_cache_creation),0)+COALESCE(SUM(tokens_reasoning),0) AS total
   FROM omniroute.usage_history WHERE provider=$1 AND connection_id=$2 AND timestamp >= $3`,
          [provider, connectionId, monthStart]
        )
      )[0].total
    );
  }
  async connectionSpend(
    provider: string,
    connectionId: string
  ): Promise<Record<string, unknown>[]> {
    return queryUsageRows(
      this.database,
      `SELECT model,COALESCE(SUM(tokens_input),0) AS input,COALESCE(SUM(tokens_output),0) AS output,
   COALESCE(SUM(tokens_cache_read),0) AS "cacheRead",COALESCE(SUM(tokens_cache_creation),0) AS "cacheCreation",
   COALESCE(SUM(tokens_reasoning),0) AS reasoning,COUNT(*) AS requests FROM omniroute.usage_history
   WHERE connection_id=$1 AND provider=$2 AND success=1 GROUP BY model`,
      [connectionId, provider],
      NUMBERS
    );
  }
  async aggregates(
    group: "model" | "account" | "key",
    enabled: boolean,
    cutoff: string | null
  ): Promise<Record<string, unknown>[]> {
    const columns = {
      model: "provider,model,service_tier",
      account: "provider,model,connection_id,service_tier",
      key: "provider,model,api_key_id,api_key_name,service_tier",
    }[group];
    const filter =
      group === "account"
        ? "WHERE connection_id IS NOT NULL AND connection_id != ''"
        : group === "key"
          ? "WHERE NULLIF(api_key_id,'') IS NOT NULL OR NULLIF(api_key_name,'') IS NOT NULL"
          : "";
    return queryUsageRows(
      this.database,
      `WITH usage_source AS (${buildUsageSourceSql(enabled)})
   SELECT ${columns},${AGGREGATE_FIELDS} FROM usage_source ${filter} GROUP BY ${columns}`,
      enabled ? [cutoff] : [],
      NUMBERS
    );
  }
  async recent(since: string, until: string): Promise<Record<string, unknown>[]> {
    return queryUsageRows(
      this.database,
      `SELECT to_char(timestamp::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:00.000"Z"') AS minute,
   provider,model,COALESCE(service_tier,'standard') AS service_tier,COUNT(*) AS request_count,
   COALESCE(SUM(tokens_input),0) AS tokens_input,COALESCE(SUM(tokens_output),0) AS tokens_output,
   COALESCE(SUM(tokens_cache_read),0) AS tokens_cache_read,COALESCE(SUM(tokens_cache_creation),0) AS tokens_cache_creation,
   COALESCE(SUM(tokens_reasoning),0) AS tokens_reasoning FROM omniroute.usage_history
   WHERE timestamp >= $1 AND timestamp <= $2 GROUP BY minute,provider,model,service_tier`,
      [since, until],
      NUMBERS
    );
  }
}
export function getSelectedUsageStatsRepository(): PostgresUsageStatsRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresUsageStatsRepository(database))
    : undefined;
}
