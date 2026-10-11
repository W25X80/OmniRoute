import type { PostgresDatabase } from "@/lib/db/postgres/database";
import { PostgresQuotaRepository } from "@/lib/db/postgres/quotaRepository";
import { WINDOW_MS, dimensionKeyToString, type DimensionKey } from "./dimensions";
import type { PoolUsageSnapshot, QuotaStore } from "./types";
import { computeBurnRateFromWindow } from "./burnRate";
import { validateQuotaEventTimeMs } from "./eventTime";

function effective(curr: number, prev: number, nowMs: number, windowMs: number): number {
  return prev * (1 - (nowMs % windowMs) / windowMs) + curr;
}

/** PostgreSQL-native counters; a mutation and its readback share one acknowledged transaction. */
export class PostgresQuotaStore implements QuotaStore {
  private readonly repository: PostgresQuotaRepository;

  constructor(database: PostgresDatabase) {
    this.repository = new PostgresQuotaRepository(database);
  }

  async consume(
    apiKeyId: string,
    dim: DimensionKey,
    cost: number,
    eventTimeMs?: number
  ): Promise<number> {
    const nowMs = validateQuotaEventTimeMs(eventTimeMs === undefined ? Date.now() : eventTimeMs);
    const windowMs = WINDOW_MS[dim.window];
    const values = await this.repository.consume(
      apiKeyId,
      dimensionKeyToString(dim),
      Math.floor(nowMs / windowMs),
      cost,
      nowMs
    );
    return effective(values.curr, values.prev, nowMs, windowMs);
  }

  async peek(apiKeyId: string, dim: DimensionKey): Promise<number> {
    const nowMs = Date.now();
    const windowMs = WINDOW_MS[dim.window];
    const values = await this.repository.getPair(
      apiKeyId,
      dimensionKeyToString(dim),
      Math.floor(nowMs / windowMs)
    );
    return effective(values.curr, values.prev, nowMs, windowMs);
  }

  async poolConsumedTotal(_poolId: string, dim: DimensionKey): Promise<number> {
    const nowMs = Date.now();
    const windowMs = WINDOW_MS[dim.window];
    const values = await this.repository.sumPoolDimension(
      dimensionKeyToString(dim),
      Math.floor(nowMs / windowMs)
    );
    return effective(values.currTotal, values.prevTotal, nowMs, windowMs);
  }

  async poolUsage(poolId: string): Promise<PoolUsageSnapshot> {
    await this.repository.getPool(poolId);
    return { poolId, generatedAt: new Date().toISOString(), dimensions: [] };
  }

  async poolUsageWithDimensions(
    poolId: string,
    planDimensions: Array<{ unit: string; window: string; limit: number }>
  ): Promise<PoolUsageSnapshot> {
    const nowMs = Date.now();
    const requested = planDimensions.flatMap((dimension) => {
      const windowMs = WINDOW_MS[dimension.window as keyof typeof WINDOW_MS];
      if (!windowMs) return [];
      return [
        {
          dimension,
          key: dimensionKeyToString({
            poolId,
            unit: dimension.unit as DimensionKey["unit"],
            window: dimension.window as DimensionKey["window"],
          }),
          bucket: Math.floor(nowMs / windowMs),
          windowMs,
        },
      ];
    });
    const { pool, buckets } = await this.repository.usageRows(poolId, requested);
    if (!pool) return { poolId, generatedAt: new Date(nowMs).toISOString(), dimensions: [] };
    const totalWeight = pool.allocations.reduce((sum, allocation) => sum + allocation.weight, 0);
    const dimensions = requested.map(({ dimension, key, bucket, windowMs }) => {
      const perKey = pool.allocations.map((allocation) => {
        const curr =
          buckets.find(
            (row) =>
              row.api_key_id === allocation.apiKeyId &&
              row.dimension_key === key &&
              Number(row.bucket_index) === bucket
          )?.consumed ?? 0;
        const prev =
          buckets.find(
            (row) =>
              row.api_key_id === allocation.apiKeyId &&
              row.dimension_key === key &&
              Number(row.bucket_index) === bucket - 1
          )?.consumed ?? 0;
        const consumed = effective(curr, prev, nowMs, windowMs);
        const weight = totalWeight > 0 ? allocation.weight : 100 / pool.allocations.length;
        const fairShare = (weight / 100) * dimension.limit;
        return {
          apiKeyId: allocation.apiKeyId,
          consumed,
          fairShare,
          deficit: consumed - fairShare,
          borrowing: consumed > fairShare,
        };
      });
      return {
        unit: dimension.unit as PoolUsageSnapshot["dimensions"][number]["unit"],
        window: dimension.window as PoolUsageSnapshot["dimensions"][number]["window"],
        limit: dimension.limit,
        consumedTotal: perKey.reduce((sum, keyUsage) => sum + keyUsage.consumed, 0),
        perKey,
      };
    });
    const tokenDimension = dimensions.find((dimension) => dimension.unit === "tokens");
    let burnRate: PoolUsageSnapshot["burnRate"];
    if (tokenDimension && tokenDimension.consumedTotal > 0) {
      const rate = computeBurnRateFromWindow(
        tokenDimension.consumedTotal,
        WINDOW_MS[tokenDimension.window],
        tokenDimension.limit - tokenDimension.consumedTotal
      );
      burnRate = {
        tokensPerSecond: rate.tokensPerSecond,
        timeToExhaustionMs: rate.timeToExhaustionMs,
      };
    }
    return { poolId, generatedAt: new Date(nowMs).toISOString(), dimensions, burnRate };
  }

  async clear(apiKeyId: string, dim: DimensionKey): Promise<void> {
    await this.repository.clear(
      apiKeyId,
      dimensionKeyToString(dim),
      Math.floor(Date.now() / WINDOW_MS[dim.window])
    );
  }
}

const stores = new WeakMap<PostgresDatabase, PostgresQuotaStore>();
export function getPostgresQuotaStore(database: PostgresDatabase): PostgresQuotaStore {
  let store = stores.get(database);
  if (!store) {
    store = new PostgresQuotaStore(database);
    stores.set(database, store);
  }
  return store;
}
