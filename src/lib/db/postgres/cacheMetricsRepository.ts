import type { PostgresDatabase } from "./database";
import type { CacheTrendPoint } from "../settings/cacheMetrics";
import {
  projectCacheMetrics,
  type CacheTotalsRow,
  type CacheProviderRow,
  type CacheStrategyRow,
} from "../settings/cacheMetricsSemantics";

type NumericRow<Row> = {
  [Key in keyof Row]: Row[Key] extends number | null ? number | string | null : Row[Key];
};

function numbers<Row>(row: NumericRow<Row>, keys: readonly (keyof Row)[]): Row {
  const value = { ...row };
  for (const key of keys) {
    if (value[key] !== null && value[key] !== undefined)
      value[key] = Number(value[key]) as NumericRow<Row>[keyof Row];
  }
  return value as unknown as Row;
}

/** Native SQL aggregates; the presentation policy is shared with SQLite. */
export async function readPostgresCacheMetrics(database: PostgresDatabase) {
  try {
    return await database.transaction(async (transaction) => {
      const totals = (
        await transaction.query<NumericRow<CacheTotalsRow>>(`SELECT
        COUNT(*) AS "totalRequests", SUM(tokens_input)::bigint AS "totalInputTokens",
        SUM(tokens_cache_read)::bigint AS "totalCachedTokens", SUM(tokens_cache_creation)::bigint AS "totalCacheCreationTokens"
        FROM omniroute.usage_history WHERE tokens_cache_read > 0 OR tokens_cache_creation > 0`)
      ).rows[0];
      const all = (
        await transaction.query<{ totalRequests: string | number }>(
          `SELECT COUNT(*) AS "totalRequests" FROM omniroute.usage_history`
        )
      ).rows[0];
      const providers = (
        await transaction.query<NumericRow<CacheProviderRow>>(`SELECT provider,
        COUNT(*) AS "totalRequests",
        SUM(CASE WHEN tokens_cache_read > 0 OR tokens_cache_creation > 0 THEN 1 ELSE 0 END) AS "cachedRequests",
        SUM(CASE WHEN tokens_cache_read > 0 OR tokens_cache_creation > 0 THEN tokens_input ELSE 0 END)::bigint AS "inputTokens",
        SUM(tokens_cache_read)::bigint AS "cachedTokens", SUM(tokens_cache_creation)::bigint AS "cacheCreationTokens"
        FROM omniroute.usage_history WHERE provider IS NOT NULL GROUP BY provider
        HAVING SUM(CASE WHEN tokens_cache_read > 0 OR tokens_cache_creation > 0 THEN 1 ELSE 0 END) > 0
        ORDER BY provider COLLATE "C"`)
      ).rows;
      const strategies = (
        await transaction.query<NumericRow<CacheStrategyRow>>(`SELECT
        COALESCE(combo_strategy, 'direct') AS strategy, COUNT(*) AS requests,
        SUM(tokens_input)::bigint AS "inputTokens", SUM(tokens_cache_read)::bigint AS "cachedTokens",
        SUM(tokens_cache_creation)::bigint AS "cacheCreationTokens"
        FROM omniroute.usage_history WHERE tokens_cache_read > 0 OR tokens_cache_creation > 0
        GROUP BY combo_strategy ORDER BY combo_strategy COLLATE "C" ASC NULLS FIRST`)
      ).rows;
      return projectCacheMetrics(
        totals
          ? numbers<CacheTotalsRow>(totals, [
              "totalRequests",
              "totalInputTokens",
              "totalCachedTokens",
              "totalCacheCreationTokens",
            ])
          : undefined,
        all ? { totalRequests: Number(all.totalRequests) } : undefined,
        providers.map((row) =>
          numbers<CacheProviderRow>(row, [
            "totalRequests",
            "cachedRequests",
            "inputTokens",
            "cachedTokens",
            "cacheCreationTokens",
          ])
        ),
        strategies.map((row) =>
          numbers<CacheStrategyRow>(row, [
            "requests",
            "inputTokens",
            "cachedTokens",
            "cacheCreationTokens",
          ])
        )
      );
    });
  } catch (error) {
    console.error("Failed to fetch cache metrics from usage_history:", error);
    return projectCacheMetrics(undefined, undefined, [], []);
  }
}

interface TrendRow {
  timestamp: string;
  now_ms?: number | string;
  tokens_input: number | string | null;
  tokens_cache_read: number | string | null;
  tokens_cache_creation: number | string | null;
}

/** Domain hour bucketing for source-written ISO timestamps and accepted legacy date forms. */
export function cacheTrendHour(timestamp: string, now = Date.now()): string | null {
  const input = timestamp.trim();
  const format = (date: Date, hour = date.getUTCHours()): string | null => {
    if (
      !Number.isFinite(date.getTime()) ||
      date.getUTCFullYear() > 9999 ||
      date.getTime() < -210866760000000
    )
      return null;
    const year = date.getUTCFullYear();
    const formattedYear =
      year < 0 ? `-${String(-year).padStart(3, "0")}` : String(year).padStart(4, "0");
    return `${formattedYear}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}T${String(hour).padStart(2, "0")}:00:00Z`;
  };
  if (input.toLowerCase() === "now") return format(new Date(now));
  if (/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(input)) {
    const julian = Number(input);
    if (julian < 0 || julian >= 5373484.5) return null;
    return format(new Date(Math.round(julian * 86400000) - 210866760000000));
  }
  const dated = /^(-?\d{4})-(\d{2})-(\d{2})(?:[T\s]+(.*))?$/.exec(input);
  const time = /^(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?\s*(Z|[+-]\d{2}:\d{2})?$/i.exec(
    dated ? (dated[4] ?? "00:00") : input
  );
  if (!time) return null;
  const year = dated ? Number(dated[1]) : 2000;
  const month = dated ? Number(dated[2]) : 1;
  const day = dated ? Number(dated[3]) : 1;
  const hour = Number(time[1]),
    minute = Number(time[2]),
    second = Number(time[3] ?? 0);
  if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 24 || minute > 59 || second > 59)
    return null;
  const timezone = time[5] ?? "Z";
  const zoneHour = timezone.toUpperCase() === "Z" ? 0 : Number(timezone.slice(1, 3));
  const zoneMinute = timezone.toUpperCase() === "Z" ? 0 : Number(timezone.slice(4, 6));
  if (zoneHour > 14 || zoneMinute > 59) return null;
  const offset = (timezone.startsWith("-") ? -1 : 1) * (zoneHour * 60 + zoneMinute);
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(
    hour,
    minute - offset,
    second,
    Math.min(999, Math.round(Number(`0.${time[4] ?? 0}`) * 1000))
  );
  // SQLite keeps its original HMS when the timezone offset is zero. For days > 28
  // it normalizes YMD through the Julian day, which also accounts for hour 24.
  if (offset !== 0) return format(date);
  if (day > 28) return format(date, hour);
  const unchangedDay = new Date(0);
  unchangedDay.setUTCFullYear(year, month - 1, day);
  unchangedDay.setUTCHours(0, 0, 0, 0);
  return format(unchangedDay, hour);
}

/** Keep the source's lexical TEXT cutoff instead of changing it to instant comparison. */
export async function readPostgresCacheTrend(
  database: PostgresDatabase,
  hours: number
): Promise<CacheTrendPoint[]> {
  // SQLite's source modifier is '-${hours} hours'; a negative or nonfinite value is invalid.
  if (!Number.isFinite(hours) || hours < 0) return [];
  try {
    return await database.transaction(async (transaction) => {
      const { rows } = await transaction.query<TrendRow>(
        `SELECT timestamp, tokens_input,
        tokens_cache_read, tokens_cache_creation, EXTRACT(EPOCH FROM statement_timestamp()) * 1000 AS now_ms FROM omniroute.usage_history
        WHERE timestamp >= to_char((statement_timestamp() AT TIME ZONE 'UTC') -
          make_interval(secs => $1::double precision * 3600), 'YYYY-MM-DD HH24:MI:SS')
        ORDER BY timestamp COLLATE "C", id`,
        [hours]
      );
      const buckets = new Map<
        string | null,
        {
          timestamp: string | null;
          requests: number;
          cachedRequests: number;
          inputTokens: bigint;
          cachedTokens: bigint;
          cacheCreationTokens: bigint;
        }
      >();
      const integer = (value: string | number | null): bigint =>
        value === null ? 0n : BigInt(value);
      for (const row of rows) {
        const hour = cacheTrendHour(
          row.timestamp,
          row.now_ms === undefined ? Date.now() : Number(row.now_ms)
        );
        const point = buckets.get(hour) ?? {
          timestamp: hour,
          requests: 0,
          cachedRequests: 0,
          inputTokens: 0n,
          cachedTokens: 0n,
          cacheCreationTokens: 0n,
        };
        point.requests++;
        if (Number(row.tokens_cache_read) > 0 || Number(row.tokens_cache_creation) > 0)
          point.cachedRequests++;
        point.inputTokens += integer(row.tokens_input);
        point.cachedTokens += integer(row.tokens_cache_read);
        point.cacheCreationTokens += integer(row.tokens_cache_creation);
        for (const total of [point.inputTokens, point.cachedTokens, point.cacheCreationTokens]) {
          if (total < -9223372036854775808n || total > 9223372036854775807n)
            throw new Error("Cache trend token sum exceeds its source integer range");
        }
        buckets.set(hour, point);
      }
      return [...buckets.values()]
        .sort((left, right) =>
          left.timestamp === null
            ? right.timestamp === null
              ? 0
              : -1
            : right.timestamp === null
              ? 1
              : left.timestamp.localeCompare(right.timestamp)
        )
        .map((point) => ({
          ...point,
          inputTokens: Number(point.inputTokens),
          cachedTokens: Number(point.cachedTokens),
          cacheCreationTokens: Number(point.cacheCreationTokens),
        }));
    });
  } catch (error) {
    console.error("Failed to fetch cache trend:", error);
    return [];
  }
}
