import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogTableRows,
  compareCatalogText,
  hasPostgresCatalogSnapshot,
  withPostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  writePostgresCatalog,
} from "./postgres/catalogSnapshot";
/**
 * db/autoCandidateOverrides.ts — Per-API-key candidate exclusions for `auto/*`
 * channels (#7819, Level 2 of the "per-user candidate control" feature).
 *
 * OmniRoute is single-tenant (no `users` table, no `user_id`/`userId` column
 * anywhere under `src/lib/db/`) — `apiKeyId` is the closest real per-caller
 * identity this app has, so overrides are keyed by (apiKeyId, autoChannel,
 * connectionId) rather than "per user". See the Open Question in the #7819
 * plan for the follow-up decision on whether this should instead be global.
 *
 * Mirrors the relational style of `reasoningRoutingRules.ts` / `apiKeyGroups.ts`
 * rather than the key_value JSON-blob pattern — exclusions are a simple
 * row-per-candidate set, which is simpler to index and reason about than a
 * JSON array column.
 */
import { randomUUID } from "node:crypto";
import { getDbInstance } from "./core";

export interface AutoCandidateOverride {
  id: string;
  apiKeyId: string;
  autoChannel: string;
  connectionId: string;
  excluded: boolean;
  createdAt: string;
}

type OverrideRow = {
  id: string;
  api_key_id: string;
  auto_channel: string;
  connection_id: string;
  excluded: number;
  created_at: string;
  insertion_order?: string;
};

function rowToOverride(row: OverrideRow): AutoCandidateOverride {
  return {
    id: row.id,
    apiKeyId: row.api_key_id,
    autoChannel: row.auto_channel,
    connectionId: row.connection_id,
    excluded: row.excluded === 1,
    createdAt: row.created_at,
  };
}

/**
 * Returns the set of connection IDs excluded by this API key for this auto
 * channel. Empty set when no overrides exist (the default, unconfigured
 * path) — callers should treat an empty set as "no filtering needed" rather
 * than iterating a lookup for every candidate.
 */
export async function getExcludedConnectionIds(
  apiKeyId: string,
  autoChannel: string
): Promise<Set<string>> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    if (!apiKeyId || !autoChannel) return new Set<string>();
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const rows = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("auto_candidate_overrides").filter(
            (row) =>
              row.api_key_id === apiKeyId && row.auto_channel === autoChannel && row.excluded === 1
          )
        : db!
            .prepare(
              `SELECT connection_id FROM auto_candidate_overrides
       WHERE api_key_id = ? AND auto_channel = ? AND excluded = 1`
            )
            .all(apiKeyId, autoChannel)
    ) as Array<{ connection_id: string }>;
    return new Set(rows.map((row) => row.connection_id));
  };
  if (catalogDatabase && !hasPostgresCatalogSnapshot(catalogDatabase))
    return withPostgresCatalogSnapshot(catalogDatabase, execute);
  return execute();
}

/**
 * Sets (or clears) the excluded flag for one candidate connection, scoped to
 * one API key + auto channel. Idempotent — re-setting the same value is a
 * no-op write via UPSERT.
 */
export async function setExcluded(
  apiKeyId: string,
  autoChannel: string,
  connectionId: string,
  excluded: boolean
): Promise<AutoCandidateOverride> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const id = randomUUID();
    const createdAt = new Date().toISOString();
    getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "INSERT INTO omniroute.auto_candidate_overrides\n       (id, api_key_id, auto_channel, connection_id, excluded, created_at)\n     VALUES ($1, $2, $3, $4, $5, $6)\n     ON CONFLICT(api_key_id, auto_channel, connection_id)\n     DO UPDATE SET excluded = excluded.excluded",
                [id, apiKeyId, autoChannel, connectionId, excluded ? 1 : 0, createdAt]
              )
            ).rowCount ?? 0,
        }
      : db!
          .prepare(
            `INSERT INTO auto_candidate_overrides
       (id, api_key_id, auto_channel, connection_id, excluded, created_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(api_key_id, auto_channel, connection_id)
     DO UPDATE SET excluded = excluded.excluded`
          )
          .run(id, apiKeyId, autoChannel, connectionId, excluded ? 1 : 0, createdAt);

    const row = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("auto_candidate_overrides").filter(
            (row) =>
              row.api_key_id === apiKeyId &&
              row.auto_channel === autoChannel &&
              row.connection_id === connectionId
          )[0]
        : db!
            .prepare(
              `SELECT id, api_key_id, auto_channel, connection_id, excluded, created_at
       FROM auto_candidate_overrides
       WHERE api_key_id = ? AND auto_channel = ? AND connection_id = ?`
            )
            .get(apiKeyId, autoChannel, connectionId)
    ) as OverrideRow;
    return rowToOverride(row);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/** Lists all override rows for one API key + auto channel (excluded and not). */
export async function listOverrides(
  apiKeyId: string,
  autoChannel: string
): Promise<AutoCandidateOverride[]> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    if (!apiKeyId || !autoChannel) return [];
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const rows = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<OverrideRow>("auto_candidate_overrides")
            .filter((row) => row.api_key_id === apiKeyId && row.auto_channel === autoChannel)
            .sort(
              (a, b) =>
                compareCatalogText(a.created_at, b.created_at) ||
                Number(a.insertion_order) - Number(b.insertion_order)
            )
        : db!
            .prepare(
              `SELECT id, api_key_id, auto_channel, connection_id, excluded, created_at
       FROM auto_candidate_overrides
       WHERE api_key_id = ? AND auto_channel = ?
       ORDER BY created_at ASC`
            )
            .all(apiKeyId, autoChannel)
    ) as OverrideRow[];
    return rows.map(rowToOverride);
  };
  if (catalogDatabase && !hasPostgresCatalogSnapshot(catalogDatabase))
    return withPostgresCatalogSnapshot(catalogDatabase, execute);
  return execute();
}
