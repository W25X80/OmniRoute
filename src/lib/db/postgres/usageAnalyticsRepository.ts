import type { PostgresDatabase } from "./database";
import { queryNamedUsageRows, queryUsageRows, USAGE_NUMERIC_FIELDS } from "./usageQueries";
import type { AnalyticsParams } from "../usageAnalytics/sources";
export interface UsageSummaryRow {
  totalRequests: number;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  uniqueModels: number;
  uniqueAccounts: number;
  uniqueApiKeys: number;
  successfulRequests: number;
  avgLatencyMs: number;
  firstRequest: string;
  lastRequest: string;
}
export interface DailyUsageRow {
  date: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
}
export interface DailyCostRow {
  date: string;
  provider: string;
  model: string;
  serviceTier: string;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  storedCost: number;
  isAggregated: number;
}
export interface HeatmapRow {
  date: string;
  totalTokens: number;
}
export interface ModelUsageRow {
  model: string;
  provider: string;
  serviceTier: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  avgLatencyMs: number;
  successfulRequests: number;
  lastUsed: string;
  storedCost: number;
  isAggregated: number;
}
export interface ProviderCostRow {
  provider: string;
  model: string;
  serviceTier: string;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  storedCost: number;
  isAggregated: number;
}
export interface ProviderUsageRow {
  provider: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  avgLatencyMs: number;
  successfulRequests: number;
}
export interface AccountCostRow {
  accountKey: string;
  provider: string;
  model: string;
  serviceTier: string;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
}
export interface AccountUsageRow {
  accountKey: string;
  account: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  avgLatencyMs: number;
  lastUsed: string;
}
export interface ApiKeyUsageRow {
  apiKeyId: string | null;
  apiKeyGroupKey: string;
  provider: string;
  model: string;
  serviceTier: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  totalTokens: number;
}
export interface ServiceTierUsageRow {
  serviceTier: string;
  provider: string;
  model: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  storedCost: number;
  isAggregated: number;
}
export interface ApiKeyMetadataRow {
  apiKeyId: string | null;
  apiKeyName: string | null;
  apiKeyGroupKey: string;
  lastUsed: string;
}
export interface WeeklyPatternRow {
  dayOfWeek: string;
  days: number;
  requests: number;
  totalTokens: number;
}
export interface PresetCostModelRow {
  model: string;
  provider: string;
  serviceTier: string;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  storedCost: number;
  isAggregated: number;
}
export interface EndpointUsageRow {
  endpoint: string;
  provider: string;
  model: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  avgLatencyMs: number;
  successfulRequests: number;
  lastUsed: string;
}
export interface EndpointUsageParams {
  sinceIso?: string | null;
  untilIso?: string | null;
}
export interface ProviderDailyUsageRow {
  date: string;
  provider: string;
  requests: number;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
}
export async function getUsageSummary(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<UsageSummaryRow> {
  const row = (
    await queryNamedUsageRows(
      database,
      `
      SELECT
        COALESCE(SUM(requests), 0) as "totalRequests",
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens",
        COUNT(DISTINCT model) as "uniqueModels",
        COUNT(DISTINCT COALESCE(NULLIF(account_key, ''), NULLIF(connection_id, ''))) as "uniqueAccounts",
        COUNT(DISTINCT COALESCE(NULLIF(api_key_id, ''), NULLIF(api_key_name, ''))) as "uniqueApiKeys",
        COALESCE(SUM(CASE WHEN success = 1 THEN requests ELSE 0 END), 0) as "successfulRequests",
        COALESCE(AVG(latency_ms), 0) as "avgLatencyMs",
        COALESCE(MIN(timestamp), '') as "firstRequest",
        COALESCE(MAX(timestamp), '') as "lastRequest"
      FROM ${unifiedSource} AS _u
    `,
      params,
      USAGE_NUMERIC_FIELDS
    )
  )[0] as UsageSummaryRow | undefined;
  return (
    row ?? {
      totalRequests: 0,
      promptTokens: 0,
      completionTokens: 0,
      totalTokens: 0,
      uniqueModels: 0,
      uniqueAccounts: 0,
      uniqueApiKeys: 0,
      successfulRequests: 0,
      avgLatencyMs: 0,
      firstRequest: "",
      lastRequest: "",
    }
  );
}

export async function getDailyUsage(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<DailyUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD') as date,
        COALESCE(SUM(requests), 0) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens"
      FROM ${unifiedSource} AS _u
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD')
      ORDER BY date ASC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as DailyUsageRow[];
}

export async function getDailyCostRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<DailyCostRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD') as date,
        LOWER(provider) as provider,
        LOWER(model) as model,
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(stored_cost), 0.0) as "storedCost",
        MAX(is_aggregated) as "isAggregated"
      FROM ${unifiedSource} AS _u
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), LOWER(provider), LOWER(model), "serviceTier"
      ORDER BY date ASC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as DailyCostRow[];
}

export async function getHeatmapRows(
  database: PostgresDatabase,
  heatmapConditions: string[],
  params: AnalyticsParams
): Promise<HeatmapRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD') as date,
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens"
      FROM omniroute.usage_history
      WHERE ${heatmapConditions.join(" AND ")}
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD')
      ORDER BY date ASC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as HeatmapRow[];
}

export async function getModelUsageRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<ModelUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        LOWER(model) as model,
        LOWER(provider) as provider,
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        COALESCE(SUM(requests), 0) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens",
        COALESCE(AVG(latency_ms), 0) as "avgLatencyMs",
        COALESCE(SUM(CASE WHEN success = 1 THEN requests ELSE 0 END), 0) as "successfulRequests",
        COALESCE(MAX(timestamp), '') as "lastUsed",
        COALESCE(SUM(stored_cost), 0.0) as "storedCost",
        MAX(is_aggregated) as "isAggregated"
      FROM ${unifiedSource} AS _u
      -- Keep cost inputs separated by day. Historical provider rows do not
      -- always use one cache-token convention, and computeCostFromPricing's
      -- non-cached-input clamp is intentionally non-linear across those rows.
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), LOWER(model), LOWER(provider), "serviceTier"
      ORDER BY requests DESC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ModelUsageRow[];
}

export async function getProviderCostRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<ProviderCostRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        LOWER(provider) as provider,
        LOWER(model) as model,
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(stored_cost), 0.0) as "storedCost",
        MAX(is_aggregated) as "isAggregated"
      FROM ${unifiedSource} AS _u
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), LOWER(provider), LOWER(model), "serviceTier"
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ProviderCostRow[];
}

export async function getProviderUsageRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<ProviderUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        LOWER(provider) as provider,
        COALESCE(SUM(requests), 0) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens",
        COALESCE(AVG(latency_ms), 0) as "avgLatencyMs",
        COALESCE(SUM(CASE WHEN success = 1 THEN requests ELSE 0 END), 0) as "successfulRequests"
      FROM ${unifiedSource} AS _u
      GROUP BY LOWER(provider)
      ORDER BY requests DESC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ProviderUsageRow[];
}

export async function getAccountCostRows(
  database: PostgresDatabase,
  whereClause: string,
  params: AnalyticsParams
): Promise<AccountCostRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      WITH account_events AS (
        SELECT
          COALESCE(
            NULLIF(usage_history.account_key, ''),
            'connection:' || COALESCE(LOWER(usage_history.provider), 'unknown') || ':' || COALESCE(NULLIF(TRIM(usage_history.connection_id), ''), 'unknown')
          ) as resolved_account_key,
          usage_history.provider,
          usage_history.model,
          usage_history.service_tier,
          usage_history.timestamp,
          usage_history.tokens_input,
          usage_history.tokens_output,
          usage_history.tokens_cache_read,
          usage_history.tokens_cache_creation,
          usage_history.tokens_reasoning
        FROM omniroute.usage_history
        ${whereClause}
      )
      SELECT
        account_events.resolved_account_key as "accountKey",
        LOWER(account_events.provider) as provider,
        LOWER(account_events.model) as model,
        COALESCE(NULLIF(account_events.service_tier, ''), 'standard') as "serviceTier",
        COALESCE(SUM(account_events.tokens_input), 0) as "promptTokens",
        COALESCE(SUM(account_events.tokens_output), 0) as "completionTokens",
        COALESCE(SUM(account_events.tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(account_events.tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(account_events.tokens_reasoning), 0) as "reasoningTokens"
      FROM account_events
      GROUP BY to_char(account_events.timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), "accountKey", LOWER(account_events.provider), LOWER(account_events.model), "serviceTier"
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as AccountCostRow[];
}

export async function getAccountUsageRows(
  database: PostgresDatabase,
  whereClause: string,
  params: AnalyticsParams
): Promise<AccountUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      WITH account_events AS (
        SELECT
          usage_history.*,
          COALESCE(NULLIF(usage_history.account_key, ''), 'connection:' || COALESCE(LOWER(usage_history.provider), 'unknown') || ':' || COALESCE(NULLIF(TRIM(usage_history.connection_id), ''), 'unknown')) as resolved_account_key
        FROM omniroute.usage_history
        ${whereClause}
      ),
      stable_account_keys AS (
        SELECT DISTINCT account_key
        FROM account_events
        WHERE account_key > ''
      ),
      stable_labels AS (
        SELECT
          stable_account_keys.account_key,
          (
            SELECT TRIM(usage_history.account_label)
            FROM omniroute.usage_history
            WHERE usage_history.account_key = stable_account_keys.account_key
              AND NULLIF(TRIM(usage_history.account_label), '') IS NOT NULL
            ORDER BY COALESCE(usage_history.account_label_priority, 0) DESC,
                     usage_history.timestamp DESC,
                     usage_history.id DESC
            LIMIT 1
          ) as account_label
        FROM stable_account_keys
      ),
      legacy_labels AS (
        SELECT account_key, account_label
        FROM (
          SELECT
            account_events.resolved_account_key as account_key,
            TRIM(account_events.account_label) as account_label,
            ROW_NUMBER() OVER (
              PARTITION BY account_events.resolved_account_key
              ORDER BY COALESCE(account_events.account_label_priority, 0) DESC,
                       account_events.timestamp DESC,
                       account_events.id DESC
            ) as label_rank
          FROM account_events
          WHERE (account_events.account_key IS NULL OR account_events.account_key = '')
            AND NULLIF(TRIM(account_events.account_label), '') IS NOT NULL
        )
        WHERE label_rank = 1
      ),
      selected_labels AS (
        SELECT account_key, account_label FROM stable_labels
        UNION ALL
        SELECT account_key, account_label FROM legacy_labels
      )
      SELECT
        account_events.resolved_account_key as "accountKey",
        COALESCE(NULLIF(TRIM(MAX(selected_labels.account_label)), ''), NULLIF(TRIM(MIN(account_events.connection_id)), ''), 'unknown') as account,
        COUNT(account_events.id) as requests,
        COALESCE(SUM(account_events.tokens_input), 0) as "promptTokens",
        COALESCE(SUM(account_events.tokens_output), 0) as "completionTokens",
        COALESCE(SUM(account_events.tokens_input + account_events.tokens_output), 0) as "totalTokens",
        COALESCE(AVG(account_events.latency_ms), 0) as "avgLatencyMs",
        COALESCE(MAX(account_events.timestamp), '') as "lastUsed"
      FROM account_events
      LEFT JOIN selected_labels
        ON selected_labels.account_key = account_events.resolved_account_key
      GROUP BY "accountKey"
      ORDER BY requests DESC
      LIMIT 50
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as AccountUsageRow[];
}

export async function getApiKeyUsageRows(
  database: PostgresDatabase,
  apiKeyWhereClause: string,
  params: AnalyticsParams
): Promise<ApiKeyUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        NULLIF(api_key_id, '') as "apiKeyId",
        COALESCE(NULLIF(api_key_id, ''), NULLIF(api_key_name, ''), 'unknown') as "apiKeyGroupKey",
        LOWER(provider) as provider,
        LOWER(model) as model,
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        COUNT(*) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens"
      FROM omniroute.usage_history
      ${apiKeyWhereClause}
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), COALESCE(NULLIF(api_key_id, ''), NULLIF(api_key_name, ''), 'unknown'), NULLIF(api_key_id, ''), LOWER(provider), LOWER(model), "serviceTier"
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ApiKeyUsageRow[];
}

export async function getServiceTierUsageRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<ServiceTierUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        LOWER(provider) as provider,
        LOWER(model) as model,
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        COALESCE(SUM(requests), 0) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens",
        COALESCE(SUM(stored_cost), 0.0) as "storedCost",
        MAX(is_aggregated) as "isAggregated"
      FROM ${unifiedSource} AS _u
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), "serviceTier", LOWER(provider), LOWER(model)
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ServiceTierUsageRow[];
}

export async function getApiKeyMetadataRows(
  database: PostgresDatabase,
  apiKeyWhereClause: string,
  params: AnalyticsParams
): Promise<ApiKeyMetadataRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        NULLIF(api_key_id, '') as "apiKeyId",
        NULLIF(api_key_name, '') as "apiKeyName",
        COALESCE(NULLIF(api_key_id, ''), NULLIF(api_key_name, ''), 'unknown') as "apiKeyGroupKey",
        MAX(timestamp) as "lastUsed"
      FROM omniroute.usage_history
      ${apiKeyWhereClause}
      GROUP BY NULLIF(api_key_id, ''), NULLIF(api_key_name, '')
      ORDER BY "lastUsed" DESC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ApiKeyMetadataRow[];
}

export async function getWeeklyPatternRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<WeeklyPatternRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        "dayOfWeek",
        COUNT(*) as days,
        COALESCE(SUM(requests), 0) as requests,
        COALESCE(SUM("totalTokens"), 0) as "totalTokens"
      FROM (
        SELECT
          to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD') as date,
          EXTRACT(DOW FROM timestamp::timestamptz AT TIME ZONE 'UTC')::integer::text as "dayOfWeek",
          COALESCE(SUM(requests), 0) as requests,
          COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens"
        FROM ${unifiedSource} AS _u
        GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), EXTRACT(DOW FROM timestamp::timestamptz AT TIME ZONE 'UTC')::integer::text
      )
      GROUP BY "dayOfWeek"
      ORDER BY "dayOfWeek" ASC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as WeeklyPatternRow[];
}

export async function getPresetCostModelRows(
  database: PostgresDatabase,
  presetUnifiedSource: string,
  params: AnalyticsParams
): Promise<PresetCostModelRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        LOWER(model) as model,
        LOWER(provider) as provider,
        COALESCE(NULLIF(service_tier, ''), 'standard') as "serviceTier",
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(stored_cost), 0.0) as "storedCost",
        MAX(is_aggregated) as "isAggregated"
      FROM ${presetUnifiedSource} AS _pu
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), LOWER(model), LOWER(provider), "serviceTier"
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as PresetCostModelRow[];
}

export async function getEndpointUsageRows(
  database: PostgresDatabase,
  params: EndpointUsageParams = {}
): Promise<EndpointUsageRow[]> {
  const conditions: string[] = [];
  const bind: Record<string, unknown> = {};
  if (params.sinceIso) {
    conditions.push("timestamp >= @since");
    bind.since = params.sinceIso;
  }
  if (params.untilIso) {
    conditions.push("timestamp <= @until");
    bind.until = params.untilIso;
  }
  const whereSql = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        COALESCE(NULLIF(endpoint, ''), 'unknown') as endpoint,
        LOWER(COALESCE(provider, 'unknown')) as provider,
        LOWER(COALESCE(model, 'unknown')) as model,
        COUNT(*) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_cache_read), 0) as "cacheReadTokens",
        COALESCE(SUM(tokens_cache_creation), 0) as "cacheCreationTokens",
        COALESCE(SUM(tokens_reasoning), 0) as "reasoningTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens",
        COALESCE(AVG(latency_ms), 0) as "avgLatencyMs",
        COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0) as "successfulRequests",
        COALESCE(MAX(timestamp), '') as "lastUsed"
      FROM omniroute.usage_history
      ${whereSql}
      GROUP BY endpoint, LOWER(COALESCE(provider, 'unknown')), LOWER(COALESCE(model, 'unknown'))
      ORDER BY requests DESC
    `,
    bind,
    USAGE_NUMERIC_FIELDS
  )) as EndpointUsageRow[];
}

export async function getProviderDailyUsageRows(
  database: PostgresDatabase,
  unifiedSource: string,
  params: AnalyticsParams
): Promise<ProviderDailyUsageRow[]> {
  return (await queryNamedUsageRows(
    database,
    `
      SELECT
        to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD') as date,
        LOWER(provider) as provider,
        COUNT(*) as requests,
        COALESCE(SUM(tokens_input), 0) as "promptTokens",
        COALESCE(SUM(tokens_output), 0) as "completionTokens",
        COALESCE(SUM(tokens_input + tokens_output), 0) as "totalTokens"
      FROM ${unifiedSource} AS _u
      GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD'), LOWER(provider)
      ORDER BY date DESC, requests DESC
    `,
    params,
    USAGE_NUMERIC_FIELDS
  )) as ProviderDailyUsageRow[];
}

export async function getAllUsageHistory(
  database: PostgresDatabase
): Promise<Record<string, unknown>[]> {
  return (await queryUsageRows(
    database,
    "SELECT * FROM omniroute.usage_history",
    [],
    USAGE_NUMERIC_FIELDS
  )) as Record<string, unknown>[];
}

export async function getAllDomainCostHistory(
  database: PostgresDatabase
): Promise<Record<string, unknown>[]> {
  return (await queryUsageRows(
    database,
    "SELECT * FROM omniroute.domain_cost_history",
    [],
    USAGE_NUMERIC_FIELDS
  )) as Record<string, unknown>[];
}

export async function getAllDomainBudgets(
  database: PostgresDatabase
): Promise<Record<string, unknown>[]> {
  return (await queryUsageRows(
    database,
    "SELECT * FROM omniroute.domain_budgets",
    [],
    USAGE_NUMERIC_FIELDS
  )) as Record<string, unknown>[];
}
