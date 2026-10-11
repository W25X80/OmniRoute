/**
 * db/usageAnalytics/sources.ts — Pure SQL source-string builders for usage analytics.
 *
 * Builds the UNION subqueries that merge recent `usage_history` rows with older
 * `daily_usage_summary` aggregates. These functions are pure (no DB calls, no imports)
 * and are consumed by the query functions in the parent `usageAnalytics.ts` module.
 */

export type AnalyticsParams = Record<string, string>;

// ---------------------------------------------------------------------------
// Unified source CTE builder
// ---------------------------------------------------------------------------

export interface BuildUnifiedSourceOptions {
  /** ISO-8601 timestamp lower bound (e.g. "2024-01-01T00:00:00.000Z"). Null = all time. */
  sinceIso: string | null;
  /** ISO-8601 timestamp upper bound. Null = no upper bound. */
  untilIso: string | null;
  /** YYYY-MM-DD date string: rows older than this have been rolled up to daily_usage_summary. */
  rawCutoffDate: string;
  /**
   * SQL condition fragment for API-key filtering, e.g.
   * "(api_key_name IN (@apiKey0) OR api_key_id IN (@apiKey0))".
   * Empty string = no API-key filter.
   */
  apiKeyWhere: string;
  /** Named-param entries for the apiKey placeholders (apiKey0, apiKey1, …). */
  apiKeyParams: AnalyticsParams;
}

export interface UnifiedSourceResult {
  /** Pre-built subquery SQL string (parenthesised, suitable for `FROM <unifiedSource> AS _u`). */
  unifiedSource: string;
  /** Named params that must be passed to every query that uses `unifiedSource`. */
  unifiedParams: AnalyticsParams;
}

/**
 * Builds the UNION subquery that merges recent `usage_history` rows with
 * older `daily_usage_summary` aggregates, preventing double-counting and
 * preventing api_key leakage from summary rows.
 *
 * The returned `unifiedSource` is a parenthesised subquery suitable for use
 * as `FROM ${unifiedSource} AS _u`.  All WHERE filters are embedded inside
 * the subquery — no additional outer WHERE is needed.
 */
export function buildUnifiedSource(opts: BuildUnifiedSourceOptions): UnifiedSourceResult {
  const { sinceIso, untilIso, rawCutoffDate, apiKeyWhere, apiKeyParams } = opts;
  const sinceDate = sinceIso?.split("T")[0] ?? null;

  // Include summaries only when the window starts before rawCutoffDate and no api_key filter is active.
  const needsAggregated = (!sinceDate || sinceDate < rawCutoffDate) && !apiKeyWhere;

  const unifiedParams: AnalyticsParams = {};

  // Raw history remains authoritative while retention is incomplete or processing a batch.
  const rawConditions: string[] = [];
  if (sinceIso) {
    rawConditions.push("timestamp >= @since");
    unifiedParams.since = sinceIso;
  }
  if (untilIso) {
    rawConditions.push("timestamp <= @until");
    unifiedParams.until = untilIso;
  }
  if (apiKeyWhere) {
    rawConditions.push(apiKeyWhere);
    Object.assign(unifiedParams, apiKeyParams);
  }
  const rawWhere = rawConditions.length > 0 ? `WHERE ${rawConditions.join(" AND ")}` : "";

  // Legacy summaries cover old days only, and yield to surviving raw rows or native archives.
  const aggConditions: string[] = [];
  if (needsAggregated) {
    if (sinceIso) {
      aggConditions.push("date >= @sinceDate");
      unifiedParams.sinceDate = sinceDate!;
    }
    if (untilIso) {
      aggConditions.push("date <= @untilDate");
      unifiedParams.untilDate = untilIso.split("T")[0];
    }
    aggConditions.push("date < @rawCutoffDate");
    unifiedParams.rawCutoffDate = rawCutoffDate;
  }
  if (needsAggregated) {
    aggConditions.push(
      "NOT EXISTS (SELECT 1 FROM omniroute.native_usage_archive archived WHERE archived.provider=LOWER(daily_usage_summary.provider) AND archived.model=LOWER(daily_usage_summary.model) AND archived.date=daily_usage_summary.date)"
    );
    aggConditions.push(
      "NOT EXISTS (SELECT 1 FROM omniroute.usage_history raw WHERE LOWER(raw.provider)=LOWER(daily_usage_summary.provider) AND LOWER(raw.model)=LOWER(daily_usage_summary.model) AND to_char(raw.timestamp::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD')=daily_usage_summary.date)"
    );
  }
  const aggWhere = aggConditions.length > 0 ? `WHERE ${aggConditions.join(" AND ")}` : "";
  let unifiedSource = needsAggregated
    ? `(
        SELECT
          timestamp,
          provider,
          model,
          tokens_input,
          tokens_output,
          tokens_cache_read,
          tokens_cache_creation,
          tokens_reasoning,
          service_tier,
          success,
          latency_ms,
          connection_id,
          account_key,
          api_key_id,
          api_key_name,
          0.0 as stored_cost,
          0 as is_aggregated,
          1 as requests
        FROM omniroute.usage_history
        ${rawWhere}
        UNION ALL
        SELECT
          date || 'T12:00:00.000Z' as timestamp,
          provider,
          model,
          total_input_tokens as tokens_input,
          total_output_tokens as tokens_output,
          0 as tokens_cache_read,
          0 as tokens_cache_creation,
          0 as tokens_reasoning,
          'standard' as service_tier,
          1 as success,
          NULL as latency_ms,
          NULL as connection_id,
          NULL as account_key,
          NULL as api_key_id,
          NULL as api_key_name,
          COALESCE(total_cost, 0.0) as stored_cost,
          1 as is_aggregated,
          total_requests as requests
        FROM omniroute.daily_usage_summary
        ${aggWhere}
       )`
    : `(SELECT
          timestamp, provider, model,
          tokens_input, tokens_output,
          tokens_cache_read, tokens_cache_creation, tokens_reasoning,
          service_tier, success, latency_ms,
          connection_id, account_key, api_key_id, api_key_name,
          0.0 as stored_cost, 0 as is_aggregated,
          1 as requests
        FROM omniroute.usage_history
        ${rawWhere}
      )`;

  if (!apiKeyWhere) {
    const archiveConditions: string[] = [];
    if (sinceDate) {
      archiveConditions.push("date >= @archiveSince");
      unifiedParams.archiveSince = sinceDate;
    }
    if (untilIso) {
      archiveConditions.push("date <= @archiveUntil");
      unifiedParams.archiveUntil = untilIso.split("T")[0];
    }
    const archiveWhere = archiveConditions.length ? `WHERE ${archiveConditions.join(" AND ")}` : "";
    unifiedSource = `(SELECT * FROM ${unifiedSource} AS retained UNION ALL
      SELECT date || 'T12:00:00.000Z',provider,model,total_input_tokens,total_output_tokens,
        0,0,0,'standard',1,NULL,NULL,NULL,NULL,NULL,total_cost,1,total_requests
      FROM omniroute.native_usage_archive ${archiveWhere})`;
  }
  return { unifiedSource, unifiedParams };
}

/**
 * Builds the UNION subquery for preset cost calculations (narrower column set
 * than the main analytics query — no connection_id / api_key columns needed).
 */
export function buildPresetUnifiedSource(opts: BuildUnifiedSourceOptions): UnifiedSourceResult {
  const { sinceIso, untilIso, rawCutoffDate, apiKeyWhere, apiKeyParams } = opts;
  const sinceDate = sinceIso?.split("T")[0] ?? null;

  const needsAggregated = (!sinceDate || sinceDate < rawCutoffDate) && !apiKeyWhere;

  const presetParams: AnalyticsParams = {};

  const rawConditions: string[] = [];
  if (sinceIso) {
    rawConditions.push("timestamp >= @presetSince");
    presetParams.presetSince = sinceIso;
  }
  if (untilIso) {
    rawConditions.push("timestamp <= @presetUntil");
    presetParams.presetUntil = untilIso;
  }
  if (apiKeyWhere) {
    rawConditions.push(apiKeyWhere);
    Object.assign(presetParams, apiKeyParams);
  }
  const presetRawWhere = rawConditions.length > 0 ? `WHERE ${rawConditions.join(" AND ")}` : "";

  const aggConditions: string[] = [];
  if (needsAggregated) {
    if (sinceIso) {
      aggConditions.push("date >= @presetSinceDate");
      presetParams.presetSinceDate = sinceDate!;
    }
    if (untilIso) {
      aggConditions.push("date <= @presetUntilDate");
      presetParams.presetUntilDate = untilIso.split("T")[0];
    }
    aggConditions.push("date < @presetRawCutoffDate");
    presetParams.presetRawCutoffDate = rawCutoffDate;
  }
  if (needsAggregated) {
    aggConditions.push(
      "NOT EXISTS (SELECT 1 FROM omniroute.native_usage_archive archived WHERE archived.provider=LOWER(daily_usage_summary.provider) AND archived.model=LOWER(daily_usage_summary.model) AND archived.date=daily_usage_summary.date)"
    );
    aggConditions.push(
      "NOT EXISTS (SELECT 1 FROM omniroute.usage_history raw WHERE LOWER(raw.provider)=LOWER(daily_usage_summary.provider) AND LOWER(raw.model)=LOWER(daily_usage_summary.model) AND to_char(raw.timestamp::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD')=daily_usage_summary.date)"
    );
  }
  const presetAggWhere = aggConditions.length > 0 ? `WHERE ${aggConditions.join(" AND ")}` : "";

  let unifiedSource = needsAggregated
    ? `(
        SELECT timestamp, provider, model, service_tier,
          tokens_input, tokens_output,
          tokens_cache_read, tokens_cache_creation, tokens_reasoning,
          0.0 as stored_cost, 0 as is_aggregated
        FROM omniroute.usage_history
        ${presetRawWhere}
        UNION ALL
        SELECT
          date || 'T12:00:00.000Z' as timestamp,
          provider, model,
          'standard' as service_tier,
          total_input_tokens as tokens_input,
          total_output_tokens as tokens_output,
          0 as tokens_cache_read,
          0 as tokens_cache_creation,
          0 as tokens_reasoning,
          COALESCE(total_cost, 0.0) as stored_cost,
          1 as is_aggregated
        FROM omniroute.daily_usage_summary
        ${presetAggWhere}
      )`
    : `(SELECT timestamp, provider, model, service_tier,
          tokens_input, tokens_output,
          tokens_cache_read, tokens_cache_creation, tokens_reasoning,
          0.0 as stored_cost, 0 as is_aggregated
        FROM omniroute.usage_history
        ${presetRawWhere}
      )`;

  if (!apiKeyWhere) {
    const archiveConditions: string[] = [];
    if (sinceDate) {
      archiveConditions.push("date >= @presetArchiveSince");
      presetParams.presetArchiveSince = sinceDate;
    }
    if (untilIso) {
      archiveConditions.push("date <= @presetArchiveUntil");
      presetParams.presetArchiveUntil = untilIso.split("T")[0];
    }
    const archiveWhere = archiveConditions.length ? `WHERE ${archiveConditions.join(" AND ")}` : "";
    unifiedSource = `(SELECT * FROM ${unifiedSource} AS retained UNION ALL
      SELECT date || 'T12:00:00.000Z',provider,model,'standard',total_input_tokens,total_output_tokens,
       0,0,0,total_cost,1
      FROM omniroute.native_usage_archive ${archiveWhere})`;
  }
  return { unifiedSource, unifiedParams: presetParams };
}
