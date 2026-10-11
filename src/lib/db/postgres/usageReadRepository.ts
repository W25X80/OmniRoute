import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { PostgresDatabase } from "./database";
import {
  queryNamedUsageRows,
  queryUsageRows,
  bindSelectedUsageRepository,
  safeUsageNumber,
} from "./usageQueries";
const NUMBERS = [
  "id",
  "rowId",
  "timestamp",
  "cost",
  "requests",
  "requestCount",
  "entryCount",
  "totalCost",
  "promptTokens",
  "completionTokens",
  "cacheReadTokens",
  "cacheCreationTokens",
  "reasoningTokens",
  "totalTokens",
  "inputTokens",
  "outputTokens",
  "successCount",
  "avgLatencyMs",
  "lastStatusCode",
  "totalRequests",
];
const USAGE_COLUMNS = `NULLIF(api_key_id,'') AS "apiKeyId",NULLIF(api_key_name,'') AS "apiKeyName",
 LOWER(provider) AS provider,LOWER(model) AS model,COALESCE(NULLIF(service_tier,''),'standard') AS "serviceTier"`;
export class PostgresUsageReadRepository {
  constructor(private readonly database: PostgresDatabase) {}
  async antigravityLocalTokenTotal(
    provider: "antigravity" | "agy",
    connectionId: string,
    model: string,
    windowStart: string,
    windowEnd: string
  ): Promise<number> {
    const row = (
      await queryUsageRows<{ tokens: unknown }>(
        this.database,
        `SELECT COALESCE(SUM(COALESCE(tokens_input,0)+COALESCE(tokens_output,0)+
          COALESCE(tokens_reasoning,0)),0) AS tokens FROM omniroute.usage_history
         WHERE provider=$1 AND connection_id=$2 AND model=$3 AND success=1
           AND timestamp>=$4 AND timestamp<$5`,
        [provider, connectionId, model, windowStart, windowEnd]
      )
    )[0];
    const raw = row?.tokens;
    if (!(typeof raw === "number" || (typeof raw === "string" && /^-?\d+$/.test(raw)))) {
      throw new Error("Invalid Antigravity local usage total");
    }
    const tokens = safeUsageNumber(raw);
    if (!Number.isSafeInteger(tokens)) throw new Error("Invalid Antigravity local usage total");
    return tokens;
  }
  async windowAggregates(
    whereSql: string,
    params: Record<string, unknown>
  ): Promise<Record<string, unknown>[]> {
    return queryNamedUsageRows(
      this.database,
      `SELECT ${USAGE_COLUMNS},COUNT(*) AS requests,
   COALESCE(SUM(tokens_input),0) AS "promptTokens",COALESCE(SUM(tokens_output),0) AS "completionTokens",
   COALESCE(SUM(tokens_cache_read),0) AS "cacheReadTokens",COALESCE(SUM(tokens_cache_creation),0) AS "cacheCreationTokens",
   COALESCE(SUM(tokens_reasoning),0) AS "reasoningTokens",COALESCE(SUM(tokens_input+tokens_output),0) AS "totalTokens",
   MAX(timestamp) AS "lastUsed" FROM omniroute.usage_history WHERE ${whereSql}
   GROUP BY NULLIF(api_key_id,''),NULLIF(api_key_name,''),LOWER(provider),LOWER(model),COALESCE(NULLIF(service_tier,''),'standard')
   ORDER BY "totalTokens" DESC`,
      params,
      NUMBERS.filter((name) => name !== "timestamp")
    );
  }
  async windowCounts(
    whereSql: string,
    params: Record<string, unknown>
  ): Promise<Record<string, unknown>[]> {
    return queryNamedUsageRows(
      this.database,
      `SELECT COALESCE(NULLIF(api_key_id,''),'') AS "apiKeyId",COUNT(*) AS "requestCount"
   FROM omniroute.usage_history WHERE ${whereSql} GROUP BY COALESCE(NULLIF(api_key_id,''),'')`,
      params,
      NUMBERS
    );
  }
  async windowDetails(
    whereSql: string,
    params: Record<string, unknown>,
    apiKeyId: string
  ): Promise<Record<string, unknown>[]> {
    return queryNamedUsageRows(
      this.database,
      `SELECT id,${USAGE_COLUMNS},COALESCE(tokens_input,0) AS "promptTokens",
   COALESCE(tokens_output,0) AS "completionTokens",COALESCE(tokens_cache_read,0) AS "cacheReadTokens",
   COALESCE(tokens_cache_creation,0) AS "cacheCreationTokens",COALESCE(tokens_reasoning,0) AS "reasoningTokens",
   COALESCE(tokens_input+tokens_output,0) AS "totalTokens",timestamp FROM omniroute.usage_history
   WHERE ${whereSql} AND api_key_id=@apiKeyId ORDER BY timestamp,id`,
      { ...params, apiKeyId },
      NUMBERS.filter((name) => name !== "timestamp")
    );
  }
  async recordedSummaries(
    keys: string[],
    since: number,
    until: number
  ): Promise<Record<string, unknown>[]> {
    return queryUsageRows<Record<string, unknown>>(
      this.database,
      `SELECT api_key_id AS "apiKeyId",COALESCE(SUM(cost),0) AS "totalCost",COUNT(*) AS "entryCount"
   FROM omniroute.domain_cost_history WHERE api_key_id=ANY($1::text[]) AND timestamp>=$2 AND timestamp<=$3 GROUP BY api_key_id`,
      [keys, since, until],
      NUMBERS
    );
  }
  async recordedCosts(
    keys: string[],
    since: number,
    until: number
  ): Promise<Record<string, unknown>[]> {
    return queryUsageRows<Record<string, unknown>>(
      this.database,
      `SELECT id AS "rowId",api_key_id AS "apiKeyId",timestamp,cost FROM omniroute.domain_cost_history
   WHERE api_key_id=ANY($1::text[]) AND timestamp>=$2 AND timestamp<=$3 ORDER BY api_key_id,timestamp,id`,
      [keys, since, until],
      NUMBERS
    );
  }
  async keyTokenTotals(key: string, since: string): Promise<Record<string, unknown>> {
    return (
      await queryUsageRows<Record<string, unknown>>(
        this.database,
        `SELECT COALESCE(SUM(tokens_input),0) AS "inputTokens",COALESCE(SUM(tokens_output),0) AS "outputTokens",
   COALESCE(SUM(tokens_cache_read),0) AS "cacheReadTokens",COALESCE(SUM(tokens_cache_creation),0) AS "cacheCreationTokens",
   COALESCE(SUM(tokens_reasoning),0) AS "reasoningTokens" FROM omniroute.usage_history WHERE api_key_id=$1 AND timestamp>=$2`,
        [key, since],
        NUMBERS
      )
    )[0];
  }
  async cacheHealth(
    since: string,
    model?: string,
    limit = 10000
  ): Promise<Record<string, unknown>[]> {
    return queryUsageRows<Record<string, unknown>>(
      this.database,
      `SELECT model,requested_model,tokens_cache_read,tokens_cache_creation,timestamp FROM omniroute.call_logs
   WHERE timestamp>=$1 AND status=200 AND (tokens_cache_read IS NOT NULL OR tokens_cache_creation IS NOT NULL)
   ${model ? "AND (model=$3 OR requested_model=$3)" : ""} ORDER BY timestamp DESC,id DESC LIMIT $2`,
      model ? [since, limit, model] : [since, limit],
      ["tokens_cache_read", "tokens_cache_creation"]
    );
  }
  async comboUsage(combo: string, since: string): Promise<Record<string, unknown>[]> {
    return queryUsageRows<Record<string, unknown>>(
      this.database,
      `SELECT model,COUNT(*) AS requests,SUM(COALESCE(tokens_in,0)+COALESCE(tokens_out,0)) AS "totalTokens"
   FROM omniroute.call_logs WHERE combo_name=$1 AND timestamp>=$2 GROUP BY model`,
      [combo, since],
      NUMBERS
    );
  }
  async comboPerformance(
    combo: string,
    since: string
  ): Promise<Record<string, unknown> | undefined> {
    return (
      await queryUsageRows<Record<string, unknown>>(
        this.database,
        `SELECT COUNT(*) AS "totalRequests",SUM(CASE WHEN status>=200 AND status<400 THEN 1 ELSE 0 END) AS "successCount",
   AVG(duration) AS "avgLatencyMs" FROM omniroute.call_logs WHERE combo_name=$1 AND timestamp>=$2`,
        [combo, since],
        NUMBERS
      )
    )[0];
  }
  async comboTargets(combo: string, since: string): Promise<Record<string, unknown>[]> {
    return queryUsageRows<Record<string, unknown>>(
      this.database,
      `WITH target_logs AS (SELECT id,COALESCE(NULLIF(combo_execution_key,''),NULLIF(combo_step_id,'')) AS "executionKey",
   NULLIF(combo_step_id,'') AS "stepId",status,duration,timestamp FROM omniroute.call_logs WHERE combo_name=$1 AND timestamp>=$2
   AND COALESCE(NULLIF(combo_execution_key,''),NULLIF(combo_step_id,'')) IS NOT NULL),
   aggregate_metrics AS (SELECT "executionKey",MAX("stepId") AS "stepId",COUNT(*) AS requests,
   SUM(CASE WHEN status>=200 AND status<400 THEN 1 ELSE 0 END) AS "successCount",AVG(duration) AS "avgLatencyMs",MAX(timestamp) AS "lastUsedAt"
   FROM target_logs GROUP BY "executionKey"),latest_metrics AS (SELECT DISTINCT ON("executionKey") "executionKey","stepId",status AS "lastStatusCode"
   FROM target_logs ORDER BY "executionKey",timestamp DESC,id DESC)
   SELECT aggregate_metrics."executionKey",COALESCE(latest_metrics."stepId",aggregate_metrics."stepId") AS "stepId",
    aggregate_metrics.requests,aggregate_metrics."successCount",aggregate_metrics."avgLatencyMs",latest_metrics."lastStatusCode",aggregate_metrics."lastUsedAt"
   FROM aggregate_metrics LEFT JOIN latest_metrics USING("executionKey") ORDER BY aggregate_metrics."executionKey"`,
      [combo, since],
      NUMBERS
    );
  }
}
export function getSelectedUsageReadRepository(): PostgresUsageReadRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresUsageReadRepository(database))
    : undefined;
}
