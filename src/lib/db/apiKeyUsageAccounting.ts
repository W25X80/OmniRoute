import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";

export interface WeeklyQuotaObservation {
  remainingPercentage: number | null;
  nextResetAt: string | null;
  createdAt: string | null;
}
export interface PricedUsageRow {
  provider: string | null;
  model: string | null;
  serviceTier: string | null;
  promptTokens: number | null;
  completionTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  reasoningTokens: number | null;
}

export async function readWeeklyQuotaObservationsAsync(
  connectionId: string,
  nowIso: string
): Promise<WeeklyQuotaObservation[]> {
  const database = getSelectedPostgresDatabase();
  if (!database)
    return getDbInstance()
      .prepare(
        `SELECT remaining_percentage AS remainingPercentage,next_reset_at AS nextResetAt,created_at AS createdAt
    FROM quota_snapshots WHERE connection_id=? AND LOWER(window_key) LIKE '%weekly%' AND LOWER(window_key) NOT LIKE '%sonnet%'
    AND created_at <= ? ORDER BY created_at ASC,id ASC`
      )
      .all(connectionId, nowIso) as WeeklyQuotaObservation[];
  return database.transaction(
    async (tx) =>
      (
        await tx.query(
          `SELECT remaining_percentage AS "remainingPercentage",next_reset_at AS "nextResetAt",created_at AS "createdAt"
    FROM omniroute.quota_snapshots WHERE connection_id=$1 AND LOWER(window_key) LIKE '%weekly%' AND LOWER(window_key) NOT LIKE '%sonnet%'
    AND created_at <= $2 ORDER BY created_at ASC,id ASC`,
          [connectionId, nowIso]
        )
      ).rows as unknown as WeeklyQuotaObservation[]
  );
}

export async function readApiKeyPricedUsageAsync(
  id: string,
  sinceIso: string
): Promise<PricedUsageRow[]> {
  const database = getSelectedPostgresDatabase();
  if (!database)
    return getDbInstance()
      .prepare(
        `SELECT LOWER(provider) AS provider,LOWER(model) AS model,
    COALESCE(NULLIF(service_tier,''),'standard') AS serviceTier,COALESCE(SUM(tokens_input),0) AS promptTokens,
    COALESCE(SUM(tokens_output),0) AS completionTokens,COALESCE(SUM(tokens_cache_read),0) AS cacheReadTokens,
    COALESCE(SUM(tokens_cache_creation),0) AS cacheCreationTokens,COALESCE(SUM(tokens_reasoning),0) AS reasoningTokens
    FROM usage_history WHERE api_key_id=? AND timestamp >= ? AND success=1
    GROUP BY LOWER(provider),LOWER(model),serviceTier`
      )
      .all(id, sinceIso) as PricedUsageRow[];
  return database.transaction(async (tx) =>
    (
      await tx.query(
        `SELECT LOWER(provider) AS provider,LOWER(model) AS model,
    COALESCE(NULLIF(service_tier,''),'standard') AS "serviceTier",COALESCE(SUM(tokens_input),0) AS "promptTokens",
    COALESCE(SUM(tokens_output),0) AS "completionTokens",COALESCE(SUM(tokens_cache_read),0) AS "cacheReadTokens",
    COALESCE(SUM(tokens_cache_creation),0) AS "cacheCreationTokens",COALESCE(SUM(tokens_reasoning),0) AS "reasoningTokens"
    FROM omniroute.usage_history WHERE api_key_id=$1 AND timestamp >= $2 AND success=1
    GROUP BY LOWER(provider),LOWER(model),COALESCE(NULLIF(service_tier,''),'standard')`,
        [id, sinceIso]
      )
    ).rows.map((r) => ({
      provider: r.provider as string | null,
      model: r.model as string | null,
      serviceTier: r.serviceTier as string | null,
      promptTokens: Number(r.promptTokens),
      completionTokens: Number(r.completionTokens),
      cacheReadTokens: Number(r.cacheReadTokens),
      cacheCreationTokens: Number(r.cacheCreationTokens),
      reasoningTokens: Number(r.reasoningTokens),
    }))
  );
}
