import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogTableRows,
  compareCatalogText,
  requirePostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  writePostgresCatalog,
  afterPostgresCatalogCommit,
} from "./postgres/catalogSnapshot";
import { getDbInstance } from "./core";
import { invalidateDbCache } from "./readCache";

/**
 * Feature 5004 — self-correcting context-window overrides.
 *
 * A persisted override of a model's real context window that wins over the static
 * catalog / models.dev sync in `getModelContextLimit()`. Two sources:
 * - `manual`: operator-set; never overwritten by the auto reconciler.
 * - `auto:discovery`: written by the reconciler when a provider's own `/models`
 *   discovery declares a window that diverges from the catalog.
 *
 * Cacheless on purpose: the read path already touches the DB (synced capabilities),
 * and a single indexed PK lookup is negligible — this avoids any cache-staleness
 * hazard with `resetDbInstance()` in tests.
 */

export type ModelContextOverrideSource = "manual" | "auto:discovery";

export interface ModelContextOverride {
  provider: string;
  modelId: string;
  realContext: number;
  source: ModelContextOverrideSource;
  refreshedAt: string;
}

interface OverrideRow {
  provider: string;
  model_id: string;
  real_context: number;
  source: string;
  refreshed_at: string;
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

function normalizeKey(
  provider: unknown,
  modelId: unknown
): { provider: string; modelId: string } | null {
  const p = typeof provider === "string" ? provider.trim() : "";
  const m = typeof modelId === "string" ? modelId.trim() : "";
  if (!p || !m) return null;
  return { provider: p, modelId: m };
}

function toOverride(row: OverrideRow): ModelContextOverride {
  return {
    provider: row.provider,
    modelId: row.model_id,
    realContext: row.real_context,
    source: row.source === "auto:discovery" ? "auto:discovery" : "manual",
    refreshedAt: row.refreshed_at,
  };
}

/** Full override record for (provider, modelId), or null. Never throws. */
export function getModelContextOverrideRecord(
  provider: string | null | undefined,
  modelId: string | null | undefined
): ModelContextOverride | null {
  requirePostgresCatalogSnapshot();

  const key = normalizeKey(provider, modelId);
  if (!key) return null;
  try {
    const row = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("model_context_overrides").filter(
            (row) => row.provider === key.provider && row.model_id === key.modelId
          )[0]
        : getDbInstance()
            .prepare(
              "SELECT provider, model_id, real_context, source, refreshed_at " +
                "FROM model_context_overrides WHERE provider = ? AND model_id = ?"
            )
            .get(key.provider, key.modelId)
    ) as OverrideRow | undefined;
    return row ? toOverride(row) : null;
  } catch {
    // Table may not exist yet (pre-migration) — fall through to the catalog.
    return null;
  }
}

/** Nested provider → model → context map used by build-local snapshots. */
export type NestedContextOverrideMap = ReadonlyMap<string, ReadonlyMap<string, number>>;

/** The overridden context window (tokens) for (provider, modelId), or null. Never throws. */
export function getModelContextOverride(
  provider: string | null | undefined,
  modelId: string | null | undefined,
  bulkContextOverrides?: NestedContextOverrideMap | null
): number | null {
  if (bulkContextOverrides) {
    const key = normalizeKey(provider, modelId);
    if (!key) return null;
    return bulkContextOverrides.get(key.provider)?.get(key.modelId) ?? null;
  }
  const record = getModelContextOverrideRecord(provider, modelId);
  return record ? record.realContext : null;
}

/**
 * Upsert an override. `realContext` must be a positive integer (a token count);
 * anything else is rejected (no write). Returns true when a row was written.
 */
export async function setModelContextOverrideAsync(
  provider: string,
  modelId: string,
  realContext: number,
  source: ModelContextOverrideSource = "manual"
): Promise<boolean> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const key = normalizeKey(provider, modelId);
    if (!key || !isPositiveInteger(realContext)) return false;
    const normalizedSource: ModelContextOverrideSource =
      source === "auto:discovery" ? "auto:discovery" : "manual";
    getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "INSERT INTO omniroute.model_context_overrides (provider, model_id, real_context, source, refreshed_at) VALUES ($1, $2, $3, $4, to_char(timezone('UTC',clock_timestamp()),'YYYY-MM-DD HH24:MI:SS')) ON CONFLICT(provider,model_id) DO UPDATE SET real_context=EXCLUDED.real_context,source=EXCLUDED.source,refreshed_at=EXCLUDED.refreshed_at",
                [key.provider, key.modelId, realContext, normalizedSource]
              )
            ).rowCount ?? 0,
        }
      : getDbInstance()
          .prepare(
            "INSERT OR REPLACE INTO model_context_overrides " +
              "(provider, model_id, real_context, source, refreshed_at) " +
              "VALUES (?, ?, ?, ?, datetime('now'))"
          )
          .run(key.provider, key.modelId, realContext, normalizedSource);
    if (getSelectedPostgresDatabase())
      afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
    else invalidateDbCache("model-capabilities");
    return true;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/** Remove an override. Returns true when a row was deleted. */
export async function removeModelContextOverrideAsync(
  provider: string,
  modelId: string
): Promise<boolean> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const key = normalizeKey(provider, modelId);
    if (!key) return false;
    const info = getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "DELETE FROM omniroute.model_context_overrides WHERE provider = $1 AND model_id = $2",
                [key.provider, key.modelId]
              )
            ).rowCount ?? 0,
        }
      : getDbInstance()
          .prepare("DELETE FROM model_context_overrides WHERE provider = ? AND model_id = ?")
          .run(key.provider, key.modelId);
    if (info.changes > 0) {
      if (getSelectedPostgresDatabase())
        afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
      else invalidateDbCache("model-capabilities");
    }
    return info.changes > 0;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/** All overrides, newest refresh first. Never throws. */
export function listModelContextOverrides(): ModelContextOverride[] {
  requirePostgresCatalogSnapshot();

  try {
    const rows = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("model_context_overrides").sort(
            (a, b) =>
              compareCatalogText(b.refreshed_at, a.refreshed_at) ||
              compareCatalogText(a.provider, b.provider) ||
              compareCatalogText(a.model_id, b.model_id)
          )
        : getDbInstance()
            .prepare(
              "SELECT provider, model_id, real_context, source, refreshed_at " +
                "FROM model_context_overrides ORDER BY refreshed_at DESC"
            )
            .all()
    ) as OverrideRow[];
    return rows.map(toOverride);
  } catch {
    return [];
  }
}

export function setModelContextOverride(
  provider: string,
  modelId: string,
  realContext: number,
  source: ModelContextOverrideSource = "manual"
): boolean {
  assertSqliteBackendAllowed();
  const key = normalizeKey(provider, modelId);
  if (!key || !isPositiveInteger(realContext)) return false;
  const normalizedSource: ModelContextOverrideSource =
    source === "auto:discovery" ? "auto:discovery" : "manual";
  getDbInstance()
    .prepare(
      "INSERT OR REPLACE INTO model_context_overrides " +
        "(provider, model_id, real_context, source, refreshed_at) " +
        "VALUES (?, ?, ?, ?, datetime('now'))"
    )
    .run(key.provider, key.modelId, realContext, normalizedSource);
  invalidateDbCache("model-capabilities");
  return true;
}

export function removeModelContextOverride(provider: string, modelId: string): boolean {
  assertSqliteBackendAllowed();
  const key = normalizeKey(provider, modelId);
  if (!key) return false;
  const info = getDbInstance()
    .prepare("DELETE FROM model_context_overrides WHERE provider = ? AND model_id = ?")
    .run(key.provider, key.modelId);
  if (info.changes > 0) invalidateDbCache("model-capabilities");
  return info.changes > 0;
}
