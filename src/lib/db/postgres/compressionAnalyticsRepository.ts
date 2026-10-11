import type {
  CompressionAnalyticsRow,
  LatestCompressionAnalyticsRun,
} from "../compressionAnalytics";
import type { PostgresDatabase, PostgresTransaction } from "./database";
type EngineAggRow = { runs: number; original: number; compressed: number; saved: number };
const numeric = new Set([
  "total",
  "totalSaved",
  "avgPct",
  "avgDur",
  "cnt",
  "saved",
  "cacheRead",
  "cacheWrite",
  "usdSaved",
  "prompt",
  "completion",
  "original",
  "compressed",
  "runs",
  "duration_ms",
  "original_tokens",
  "compressed_tokens",
  "tokens_saved",
  "actual_prompt_tokens",
  "actual_completion_tokens",
  "actual_total_tokens",
  "actual_cache_read_tokens",
  "actual_cache_write_tokens",
  "estimated_usd_saved",
  "mcp_description_tokens_saved",
  "multimodal_skip_count",
  "validation_fallback",
  "rtk_raw_output_bytes",
  "rtk_raw_output_total_bytes",
  "id",
]);
function normalize(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).map(([k, v]) => [k, numeric.has(k) && v !== null ? Number(v) : v])
  );
}
async function queryRows(tx: PostgresTransaction, text: string, values: unknown[]) {
  return (await tx.query(text, values)).rows.map(normalize);
}
async function queryOne<Row = Record<string, unknown>>(
  tx: PostgresTransaction,
  text: string,
  values: unknown[]
): Promise<Row | undefined> {
  return (await queryRows(tx, text, values))[0] as unknown as Row | undefined;
}
async function queryWrite(tx: PostgresTransaction, text: string, values: unknown[]) {
  await tx.query(text, values);
}
function appendCondition(whereClause: string, condition: string) {
  return whereClause ? `${whereClause} AND ${condition}` : `WHERE ${condition}`;
}
function toFiniteInt(value: unknown): number | null {
  if ((typeof value === "number" || typeof value === "string") && String(value).trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return Math.max(0, Math.floor(parsed));
  }
  return null;
}

export async function insertCompressionAnalyticsRowPostgres(
  database: PostgresDatabase,
  row: CompressionAnalyticsRow
) {
  return database.transaction(async (tx) => {
    await queryWrite(
      tx,
      `
    INSERT INTO omniroute.compression_analytics (
      timestamp, combo_id, compression_combo_id, engine, provider, mode, original_tokens, compressed_tokens, tokens_saved,
      duration_ms, request_id, actual_prompt_tokens, actual_completion_tokens,
      actual_total_tokens, actual_cache_read_tokens, actual_cache_write_tokens,
      estimated_usd_saved, mcp_description_tokens_saved, multimodal_skip_count,
      receipt_source, validation_fallback, output_mode, rtk_raw_output_pointer, rtk_raw_output_bytes,
      rtk_raw_output_pointers, rtk_raw_output_total_bytes, skip_reason
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
  `,
      [
        row.timestamp,
        row.combo_id ?? null,
        row.compression_combo_id ?? null,
        row.engine ?? row.mode,
        row.provider ?? null,
        row.mode,
        row.original_tokens,
        row.compressed_tokens,
        row.tokens_saved,
        row.duration_ms ?? null,
        row.request_id ?? null,
        row.actual_prompt_tokens ?? null,
        row.actual_completion_tokens ?? null,
        row.actual_total_tokens ?? null,
        row.actual_cache_read_tokens ?? null,
        row.actual_cache_write_tokens ?? null,
        row.estimated_usd_saved ?? null,
        row.mcp_description_tokens_saved ?? 0,
        row.multimodal_skip_count ?? 0,
        row.receipt_source ?? null,
        row.validation_fallback ? 1 : 0,
        row.output_mode ?? null,
        row.rtk_raw_output_pointer ?? null,
        row.rtk_raw_output_bytes ?? null,
        row.rtk_raw_output_pointers ?? null,
        row.rtk_raw_output_total_bytes ?? null,
        row.skip_reason ?? null,
      ]
    );
  });
}

export async function attachCompressionUsageReceiptPostgres(
  database: PostgresDatabase,
  requestId: string | null | undefined,
  usage: Record<string, unknown> | null | undefined,
  source: "provider" | "estimated" | "stream" = "provider"
) {
  return database.transaction(async (tx) => {
    if (!requestId || !usage || typeof usage !== "object") return;
    const promptTokens = toFiniteInt(usage.prompt_tokens);
    const completionTokens = toFiniteInt(usage.completion_tokens);
    const totalTokens =
      toFiniteInt(usage.total_tokens) ?? (promptTokens ?? 0) + (completionTokens ?? 0);
    const promptDetails =
      usage.prompt_tokens_details && typeof usage.prompt_tokens_details === "object"
        ? (usage.prompt_tokens_details as Record<string, unknown>)
        : {};
    const cacheReadTokens = toFiniteInt(
      usage.cache_read_input_tokens ?? usage.cached_tokens ?? promptDetails.cached_tokens
    );
    const cacheWriteTokens = toFiniteInt(
      usage.cache_creation_input_tokens ?? promptDetails.cache_creation_tokens
    );
    if (promptTokens === null && completionTokens === null && totalTokens <= 0) return;

    await queryWrite(
      tx,
      `
    UPDATE omniroute.compression_analytics
    SET actual_prompt_tokens = $1,
        actual_completion_tokens = $2,
        actual_total_tokens = $3,
        actual_cache_read_tokens = $4,
        actual_cache_write_tokens = $5,
        receipt_source = $6
    WHERE request_id = $7
      AND id = (
        SELECT id FROM omniroute.compression_analytics
        WHERE request_id = $8
        ORDER BY id DESC
        LIMIT 1
      )
  `,
      [
        promptTokens,
        completionTokens,
        totalTokens,
        cacheReadTokens,
        cacheWriteTokens,
        source,
        requestId,
        requestId,
      ]
    );
  });
}

export async function getPerEngineAnalyticsPostgres(
  database: PostgresDatabase,
  engineId: string,
  days = 7
) {
  return database.transaction(async (tx) => {
    const since = new Date(Date.now() - days * 86400_000).toISOString();

    // (1) Per-engine contributions from stacked runs (one breakdown row per engine).
    const breakdown = (await queryOne(
      tx,
      `SELECT COUNT(*) AS "runs",
              COALESCE(SUM(original_tokens), 0) AS "original",
              COALESCE(SUM(compressed_tokens), 0) AS "compressed",
              COALESCE(SUM(tokens_saved), 0) AS "saved"
       FROM omniroute.compression_engine_breakdown
       WHERE engine = $1 AND timestamp >= $2`,
      [engineId, since]
    )) as EngineAggRow;

    // (2) Legacy single-engine rows from compression_analytics, EXCLUDING any request
    // that already has a per-engine breakdown — so a stacked run's aggregate row is not
    // double-counted on top of its breakdown rows.
    const legacy = (await queryOne(
      tx,
      `SELECT COUNT(*) AS "runs",
              COALESCE(SUM(original_tokens), 0) AS "original",
              COALESCE(SUM(compressed_tokens), 0) AS "compressed",
              COALESCE(SUM(tokens_saved), 0) AS "saved"
       FROM omniroute.compression_analytics
       WHERE COALESCE(engine, mode) = $1 AND timestamp >= $2
         AND (
           request_id IS NULL
           OR request_id NOT IN (
             SELECT request_id FROM omniroute.compression_engine_breakdown WHERE request_id IS NOT NULL
           )
         )`,
      [engineId, since]
    )) as EngineAggRow;

    const runs = breakdown.runs + legacy.runs;
    const original = breakdown.original + legacy.original;
    const compressed = breakdown.compressed + legacy.compressed;
    const tokensSaved = Math.max(0, breakdown.saved + legacy.saved);
    const avgSavingsPercent =
      original > 0 ? Math.round(((original - compressed) / original) * 1000) / 10 : 0;
    return { engineId, runs, tokensSaved, avgSavingsPercent, days };
  });
}

export async function getCompressionAnalyticsSummaryPostgres(
  database: PostgresDatabase,
  since?: string
) {
  return database.transaction(async (tx) => {
    let cutoff: string | null = null;
    if (since === "24h") {
      cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    } else if (since === "7d") {
      cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    } else if (since === "30d") {
      cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    }

    const whereClause = cutoff ? "WHERE timestamp >= $1" : "";
    const params = cutoff ? [cutoff] : [];
    // Saving aggregates count net-saving runs only: no-op/skip rows (skip_reason set)
    // are excluded so historical totals/avgs are unchanged, while skips are surfaced
    // separately below. (#4268)
    const successWhere = appendCondition(whereClause, "skip_reason IS NULL");

    type ScalarRow = { total: number; totalSaved: number; avgPct: number; avgDur: number };
    const scalar = (await queryOne(
      tx,
      `
    SELECT
      COUNT(*) AS "total",
      COALESCE(SUM(tokens_saved), 0) AS "totalSaved",
      COALESCE(AVG(CASE WHEN original_tokens > 0 THEN CAST(tokens_saved AS DOUBLE PRECISION) / original_tokens * 100 ELSE 0 END), 0) AS "avgPct",
      COALESCE(AVG(duration_ms), 0) AS "avgDur"
    FROM omniroute.compression_analytics ${successWhere}
  `,
      [...params]
    )) as ScalarRow | undefined;

    const modeRows = (await queryRows(
      tx,
      `
    SELECT mode, COUNT(*) AS "cnt", COALESCE(SUM(tokens_saved), 0) AS "saved",
      COALESCE(AVG(CASE WHEN original_tokens > 0 THEN CAST(tokens_saved AS DOUBLE PRECISION) / original_tokens * 100 ELSE 0 END), 0) AS "avgPct"
    FROM omniroute.compression_analytics ${successWhere}
    GROUP BY mode
  `,
      [...params]
    )) as Array<{ mode: string; cnt: number; saved: number; avgPct: number }>;

    // Attempted-but-no-op runs per mode (skip_reason set) — recorded since #4268 so
    // Stacked is visible even when it saves nothing.
    const skipModeRows = (await queryRows(
      tx,
      `
    SELECT mode, COUNT(*) AS "cnt"
    FROM omniroute.compression_analytics ${appendCondition(whereClause, "skip_reason IS NOT NULL")}
    GROUP BY mode
  `,
      [...params]
    )) as Array<{ mode: string; cnt: number }>;

    const byMode: Record<
      string,
      { count: number; tokensSaved: number; avgSavingsPct: number; skipped: number }
    > = {};
    for (const r of modeRows) {
      byMode[r.mode] = {
        count: r.cnt,
        tokensSaved: r.saved,
        avgSavingsPct: Math.round(r.avgPct),
        skipped: 0,
      };
    }
    for (const r of skipModeRows) {
      if (byMode[r.mode]) byMode[r.mode].skipped = r.cnt;
      else byMode[r.mode] = { count: 0, tokensSaved: 0, avgSavingsPct: 0, skipped: r.cnt };
    }

    const engineRows = (await queryRows(
      tx,
      `
    SELECT COALESCE(engine, mode) AS "engine", COUNT(*) AS "cnt", COALESCE(SUM(tokens_saved), 0) AS "saved",
      COALESCE(AVG(CASE WHEN original_tokens > 0 THEN CAST(tokens_saved AS DOUBLE PRECISION) / original_tokens * 100 ELSE 0 END), 0) AS "avgPct"
    FROM omniroute.compression_analytics ${successWhere}
    GROUP BY COALESCE(engine, mode)
  `,
      [...params]
    )) as Array<{ engine: string; cnt: number; saved: number; avgPct: number }>;

    const byEngine: Record<string, { count: number; tokensSaved: number; avgSavingsPct: number }> =
      {};
    for (const r of engineRows) {
      byEngine[r.engine] = {
        count: r.cnt,
        tokensSaved: r.saved,
        avgSavingsPct: Math.round(r.avgPct),
      };
    }

    const compressionComboRows = (await queryRows(
      tx,
      `
    SELECT compression_combo_id AS "compressionComboId", COUNT(*) AS "cnt",
      COALESCE(SUM(tokens_saved), 0) AS "saved"
    FROM omniroute.compression_analytics ${appendCondition(successWhere, "compression_combo_id IS NOT NULL")}
    GROUP BY compression_combo_id ORDER BY cnt DESC
  `,
      [...params]
    )) as Array<{ compressionComboId: string | null; cnt: number; saved: number }>;

    const byCompressionCombo: Record<string, { count: number; tokensSaved: number }> = {};
    for (const r of compressionComboRows) {
      const key = r.compressionComboId ?? "unknown";
      byCompressionCombo[key] = { count: r.cnt, tokensSaved: r.saved };
    }

    const provRows = (await queryRows(
      tx,
      `
    SELECT provider, COUNT(*) AS "cnt", COALESCE(SUM(tokens_saved), 0) AS "saved"
    FROM omniroute.compression_analytics ${successWhere}
    GROUP BY provider ORDER BY cnt DESC
  `,
      [...params]
    )) as Array<{ provider: string | null; cnt: number; saved: number }>;

    const byProvider: Record<string, { count: number; tokensSaved: number }> = {};
    for (const r of provRows) {
      const key = r.provider ?? "unknown";
      byProvider[key] = { count: r.cnt, tokensSaved: r.saved };
    }

    const last24hMap = new Map<string, { hour: string; count: number; tokensSaved: number }>();
    const now = new Date();
    for (let i = 23; i >= 0; i--) {
      const d = new Date(now.getTime() - i * 60 * 60 * 1000);
      const hourStr = d.toISOString().substring(0, 14) + "00:00Z";
      last24hMap.set(hourStr, { hour: hourStr, count: 0, tokensSaved: 0 });
    }

    const hourRows = (await queryRows(
      tx,
      `
    SELECT to_char(timestamp::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:00:00"Z"') AS "hour",
      COUNT(*) AS "cnt", COALESCE(SUM(tokens_saved), 0) AS "saved"
    FROM omniroute.compression_analytics
    WHERE timestamp >= $1 AND skip_reason IS NULL
    GROUP BY hour ORDER BY hour ASC
  `,
      [new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString()]
    )) as Array<{
      hour: string;
      cnt: number;
      saved: number;
    }>;

    for (const r of hourRows) {
      if (last24hMap.has(r.hour)) {
        last24hMap.set(r.hour, { hour: r.hour, count: r.cnt, tokensSaved: r.saved });
      }
    }

    const last24h = Array.from(last24hMap.values());

    const receiptRows = (await queryRows(
      tx,
      `
    SELECT receipt_source AS "source", COUNT(*) AS "cnt",
      COALESCE(SUM(actual_prompt_tokens), 0) AS "prompt",
      COALESCE(SUM(actual_completion_tokens), 0) AS "completion",
      COALESCE(SUM(actual_total_tokens), 0) AS "total",
      COALESCE(SUM(actual_cache_read_tokens), 0) AS "cacheRead",
      COALESCE(SUM(actual_cache_write_tokens), 0) AS "cacheWrite",
      COALESCE(SUM(estimated_usd_saved), 0) AS "usdSaved"
    FROM omniroute.compression_analytics ${appendCondition(successWhere, "receipt_source IS NOT NULL")}
    GROUP BY receipt_source
  `,
      [...params]
    )) as Array<{
      source: string | null;
      cnt: number;
      prompt: number;
      completion: number;
      total: number;
      cacheRead: number;
      cacheWrite: number;
      usdSaved: number;
    }>;

    const realUsage = {
      requestsWithReceipts: 0,
      promptTokens: 0,
      completionTokens: 0,
      totalTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
      estimatedUsdSaved: 0,
      bySource: {} as Record<string, number>,
    };
    for (const row of receiptRows) {
      const source = row.source ?? "unknown";
      realUsage.requestsWithReceipts += row.cnt;
      realUsage.promptTokens += row.prompt;
      realUsage.completionTokens += row.completion;
      realUsage.totalTokens += row.total;
      realUsage.cacheReadTokens += row.cacheRead;
      realUsage.cacheWriteTokens += row.cacheWrite;
      realUsage.estimatedUsdSaved += row.usdSaved;
      realUsage.bySource[source] = row.cnt;
    }

    const fallbackRow = (await queryOne(
      tx,
      `
    SELECT COUNT(*) AS "cnt"
    FROM omniroute.compression_analytics ${appendCondition(successWhere, "validation_fallback = 1")}
  `,
      [...params]
    )) as { cnt: number } | undefined;

    const mcpDescriptionRow = (await queryOne(
      tx,
      `
    SELECT COUNT(*) AS "cnt", COALESCE(SUM(mcp_description_tokens_saved), 0) AS "saved"
    FROM omniroute.compression_analytics ${appendCondition(successWhere, "mcp_description_tokens_saved > 0")}
  `,
      [...params]
    )) as { cnt: number; saved: number } | undefined;

    const skipReasonRows = (await queryRows(
      tx,
      `
    SELECT skip_reason AS "reason", COUNT(*) AS "cnt"
    FROM omniroute.compression_analytics ${appendCondition(whereClause, "skip_reason IS NOT NULL")}
    GROUP BY skip_reason
  `,
      [...params]
    )) as Array<{ reason: string | null; cnt: number }>;

    const bySkipReason: Record<string, number> = {};
    let totalSkipped = 0;
    for (const r of skipReasonRows) {
      const key = r.reason ?? "unknown";
      bySkipReason[key] = r.cnt;
      totalSkipped += r.cnt;
    }

    return {
      totalRequests: scalar?.total ?? 0,
      totalTokensSaved: scalar?.totalSaved ?? 0,
      avgSavingsPct: Math.round(scalar?.avgPct ?? 0),
      avgDurationMs: Math.round(scalar?.avgDur ?? 0),
      byMode,
      byEngine,
      byCompressionCombo,
      byProvider,
      last24h,
      totalSkipped,
      bySkipReason,
      validationFallbacks: fallbackRow?.cnt ?? 0,
      realUsage,
      mcpDescriptionCompression: {
        snapshots: mcpDescriptionRow?.cnt ?? 0,
        estimatedTokensSaved: mcpDescriptionRow?.saved ?? 0,
      },
    };
  });
}

export async function getLatestCompressionAnalyticsRunPostgres(database: PostgresDatabase) {
  return database.transaction(async (tx) => {
    return (await queryOne<LatestCompressionAnalyticsRun>(
      tx,
      `SELECT id, timestamp, combo_id, compression_combo_id, mode,
              original_tokens, compressed_tokens, tokens_saved, duration_ms,
              request_id, engine, validation_fallback
         FROM omniroute.compression_analytics
        ORDER BY timestamp DESC, id DESC
        LIMIT 1`,
      []
    )) as LatestCompressionAnalyticsRun | undefined;
  });
}
