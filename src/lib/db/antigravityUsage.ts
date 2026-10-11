import { getDbInstance } from "./core";
import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedUsageReadRepository } from "./postgres/usageReadRepository";

const ANTIGRAVITY_LOCAL_USAGE_WINDOW_MS = 5 * 60 * 60 * 1000;
const ANTIGRAVITY_LOCAL_USAGE_TOKENS_PER_UNIT = 1000;

export function getAntigravityLocalUsageUnits(
  provider: "antigravity" | "agy",
  connectionId: string | undefined,
  modelId: string,
  resetAt: string | null
): number {
  assertSqliteBackendAllowed();
  const window = localUsageWindow(connectionId, modelId, resetAt);
  if (!window) return 0;

  try {
    const db = getDbInstance() as unknown as {
      prepare: (sql: string) => { get: (...params: unknown[]) => unknown };
    };
    const row = db
      .prepare(
        `SELECT COALESCE(SUM(
           COALESCE(tokens_input, 0) + COALESCE(tokens_output, 0) + COALESCE(tokens_reasoning, 0)
         ), 0) AS tokens
         FROM usage_history
         WHERE provider = ?
           AND connection_id = ?
           AND model = ?
           AND success = 1
           AND timestamp >= ?
           AND timestamp < ?`
      )
      .get(provider, connectionId, modelId, window.start, window.end) as
      { tokens?: unknown } | undefined;

    return localUsageUnits(row?.tokens);
  } catch {
    return 0;
  }
}

function localUsageWindow(
  connectionId: string | undefined,
  modelId: string,
  resetAt: string | null
): { start: string; end: string; connectionId: string } | null {
  if (!connectionId || !modelId || !resetAt) return null;
  const resetMs = Date.parse(resetAt);
  if (!Number.isFinite(resetMs)) return null;
  return {
    connectionId,
    start: new Date(resetMs - ANTIGRAVITY_LOCAL_USAGE_WINDOW_MS).toISOString(),
    end: new Date(resetMs).toISOString(),
  };
}
function localUsageUnits(value: unknown): number {
  const tokens = Number(value || 0);
  if (!Number.isFinite(tokens) || tokens <= 0) return 0;
  return Math.max(1, Math.ceil(tokens / ANTIGRAVITY_LOCAL_USAGE_TOKENS_PER_UNIT));
}

/** Native failures never enter the legacy SQLite best-effort catch. */
export async function getAntigravityLocalUsageUnitsAsync(
  provider: "antigravity" | "agy",
  connectionId: string | undefined,
  modelId: string,
  resetAt: string | null
): Promise<number> {
  const postgres = getSelectedUsageReadRepository();
  if (!postgres) return getAntigravityLocalUsageUnits(provider, connectionId, modelId, resetAt);
  const window = localUsageWindow(connectionId, modelId, resetAt);
  if (!window) return 0;
  return localUsageUnits(
    await postgres.antigravityLocalTokenTotal(
      provider,
      window.connectionId,
      modelId,
      window.start,
      window.end
    )
  );
}
