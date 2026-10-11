/**
 * db/settings/cacheMetrics.ts — Cache control metrics (computed from usage_history on-the-fly).
 */

import { getDbInstance } from "../core";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import {
  readPostgresCacheMetrics,
  readPostgresCacheTrend,
} from "../postgres/cacheMetricsRepository";
import { projectCacheMetrics } from "./cacheMetricsSemantics";

export async function getCacheMetrics() {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) return readPostgresCacheMetrics(postgres);
  const db = getDbInstance();

  try {
    // Aggregate totals from usage_history
    const totalsRow = db
      .prepare(
        `
      SELECT
        COUNT(*) as totalRequests,
        SUM(tokens_input) as totalInputTokens,
        SUM(tokens_cache_read) as totalCachedTokens,
        SUM(tokens_cache_creation) as totalCacheCreationTokens
      FROM usage_history
      WHERE tokens_cache_read > 0 OR tokens_cache_creation > 0
    `
      )
      .get() as
      | {
          totalRequests: number;
          totalInputTokens: number | null;
          totalCachedTokens: number | null;
          totalCacheCreationTokens: number | null;
        }
      | undefined;

    // Get all requests count (including those without cache activity)
    const allRequestsRow = db
      .prepare(
        `
      SELECT COUNT(*) as totalRequests
      FROM usage_history
    `
      )
      .get() as { totalRequests: number } | undefined;

    // Aggregate by provider
    const byProviderRows = db
      .prepare(
        `
      SELECT
        provider,
        COUNT(*) as totalRequests,
        SUM(CASE WHEN tokens_cache_read > 0 OR tokens_cache_creation > 0 THEN 1 ELSE 0 END) as cachedRequests,
        SUM(CASE WHEN tokens_cache_read > 0 OR tokens_cache_creation > 0 THEN tokens_input ELSE 0 END) as inputTokens,
        SUM(tokens_cache_read) as cachedTokens,
        SUM(tokens_cache_creation) as cacheCreationTokens
      FROM usage_history
      WHERE provider IS NOT NULL
      GROUP BY provider
      HAVING cachedRequests > 0
    `
      )
      .all() as Array<{
      provider: string;
      totalRequests: number;
      cachedRequests: number;
      inputTokens: number | null;
      cachedTokens: number | null;
      cacheCreationTokens: number | null;
    }>;

    // Aggregate by combo strategy (direct requests stored as 'direct')
    const byStrategyRows = db
      .prepare(
        `
      SELECT
        COALESCE(combo_strategy, 'direct') as strategy,
        COUNT(*) as requests,
        SUM(tokens_input) as inputTokens,
        SUM(tokens_cache_read) as cachedTokens,
        SUM(tokens_cache_creation) as cacheCreationTokens
      FROM usage_history
      WHERE (tokens_cache_read > 0 OR tokens_cache_creation > 0)
      GROUP BY combo_strategy
    `
      )
      .all() as Array<{
      strategy: string;
      requests: number;
      inputTokens: number | null;
      cachedTokens: number | null;
      cacheCreationTokens: number | null;
    }>;

    return projectCacheMetrics(totalsRow, allRequestsRow, byProviderRows, byStrategyRows);
  } catch (error) {
    console.error("Failed to fetch cache metrics from usage_history:", error);
    return {
      totalRequests: 0,
      requestsWithCacheControl: 0,
      totalInputTokens: 0,
      totalCachedTokens: 0,
      totalCacheCreationTokens: 0,
      tokensSaved: 0,
      estimatedCostSaved: 0,
      byProvider: {},
      byStrategy: {},
      lastUpdated: new Date().toISOString(),
    };
  }
}

export async function updateCacheMetrics(_metrics: Record<string, unknown>) {
  // No-op: metrics are now computed from usage_history on-the-fly
  // The usage_history table is the single source of truth
  return getCacheMetrics();
}

export interface CacheTrendPoint {
  timestamp: string | null;
  requests: number;
  cachedRequests: number;
  inputTokens: number;
  cachedTokens: number;
  cacheCreationTokens: number;
}

export async function getCacheTrend(hours = 24): Promise<CacheTrendPoint[]> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) return readPostgresCacheTrend(postgres, hours);
  const db = getDbInstance();

  try {
    const rows = db
      .prepare(
        `
        SELECT
          strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
          COUNT(*) as requests,
          SUM(CASE WHEN tokens_cache_read > 0 OR tokens_cache_creation > 0 THEN 1 ELSE 0 END) as cachedRequests,
          SUM(tokens_input) as inputTokens,
          SUM(tokens_cache_read) as cachedTokens,
          SUM(tokens_cache_creation) as cacheCreationTokens
        FROM usage_history
        WHERE timestamp >= datetime('now', ?)
        GROUP BY hour
        ORDER BY hour ASC
      `
      )
      .all(`-${hours} hours`) as Array<{
      hour: string;
      requests: number;
      cachedRequests: number;
      inputTokens: number | null;
      cachedTokens: number | null;
      cacheCreationTokens: number | null;
    }>;

    return rows.map((r) => ({
      timestamp: r.hour,
      requests: r.requests,
      cachedRequests: r.cachedRequests,
      inputTokens: r.inputTokens || 0,
      cachedTokens: r.cachedTokens || 0,
      cacheCreationTokens: r.cacheCreationTokens || 0,
    }));
  } catch (error) {
    console.error("Failed to fetch cache trend:", error);
    return [];
  }
}

export async function resetCacheMetrics() {
  // No-op: cache metrics are computed from usage_history.
  console.warn(
    "resetCacheMetrics is deprecated - cache metrics are now computed from usage_history"
  );
  return getCacheMetrics();
}
