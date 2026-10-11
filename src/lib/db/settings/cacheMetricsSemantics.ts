export interface CacheTotalsRow {
  totalRequests: number;
  totalInputTokens: number | null;
  totalCachedTokens: number | null;
  totalCacheCreationTokens: number | null;
}
export interface CacheProviderRow {
  provider: string;
  totalRequests: number;
  cachedRequests: number;
  inputTokens: number | null;
  cachedTokens: number | null;
  cacheCreationTokens: number | null;
}
export interface CacheStrategyRow {
  strategy: string;
  requests: number;
  inputTokens: number | null;
  cachedTokens: number | null;
  cacheCreationTokens: number | null;
}

/** Shared presentation policy for both database aggregate result sets. */
export function projectCacheMetrics(
  totalsRow: CacheTotalsRow | undefined,
  allRequestsRow: { totalRequests: number } | undefined,
  byProviderRows: readonly CacheProviderRow[],
  byStrategyRows: readonly CacheStrategyRow[]
) {
  const tokensSaved = totalsRow?.totalCachedTokens || 0;

  const AVG_INPUT_PRICE_PER_MILLION = 3;
  const CACHE_DISCOUNT = 0.9;
  const estimatedCostSaved =
    Math.round((tokensSaved / 1_000_000) * AVG_INPUT_PRICE_PER_MILLION * CACHE_DISCOUNT * 100) /
    100;

  // Build byProvider object
  const byProvider: Record<
    string,
    {
      requests: number;
      totalRequests: number;
      cachedRequests: number;
      inputTokens: number;
      cachedTokens: number;
      cacheCreationTokens: number;
    }
  > = {};
  for (const row of byProviderRows) {
    byProvider[row.provider] = {
      requests: row.cachedRequests,
      totalRequests: row.totalRequests,
      cachedRequests: row.cachedRequests,
      inputTokens: row.inputTokens || 0,
      cachedTokens: row.cachedTokens || 0,
      cacheCreationTokens: row.cacheCreationTokens || 0,
    };
  }

  // Build byStrategy object
  const byStrategy: Record<
    string,
    {
      requests: number;
      inputTokens: number;
      cachedTokens: number;
      cacheCreationTokens: number;
    }
  > = {};
  for (const row of byStrategyRows) {
    byStrategy[row.strategy] = {
      requests: row.requests,
      inputTokens: row.inputTokens || 0,
      cachedTokens: row.cachedTokens || 0,
      cacheCreationTokens: row.cacheCreationTokens || 0,
    };
  }

  return {
    totalRequests: allRequestsRow?.totalRequests || totalsRow?.totalRequests || 0,
    requestsWithCacheControl: totalsRow?.totalRequests || 0,
    totalInputTokens: totalsRow?.totalInputTokens || 0,
    totalCachedTokens: totalsRow?.totalCachedTokens || 0,
    totalCacheCreationTokens: totalsRow?.totalCacheCreationTokens || 0,
    tokensSaved,
    estimatedCostSaved,
    byProvider,
    byStrategy,
    lastUpdated: new Date().toISOString(),
  };
}
