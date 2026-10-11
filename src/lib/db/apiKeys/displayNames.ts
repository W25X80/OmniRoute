import { getDbInstance } from "../core";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";

// Bind slots per IN(...) query — keeps the largest caller batch (a 200-row
// leaderboard page) in one statement while staying far below SQLite's default
// 999-variable limit for pathological lists.
const DISPLAY_NAME_LOOKUP_CHUNK = 200;

interface DisplayNameRow {
  id: string;
  name: string | null;
}

/**
 * Display names for a set of API key ids, keyed by id.
 *
 * Reads only `id` and `name` — never `key`, `key_hash`, `key_prefix` or any
 * policy column — so a caller can label a key (leaderboards, audit views)
 * without receiving a full key record that would need masking. Unknown ids and
 * blank names are simply absent from the result.
 */
export function getApiKeyDisplayNames(ids: readonly string[]): Map<string, string> {
  const names = new Map<string, string>();
  const unique = Array.from(
    new Set(ids.filter((id) => typeof id === "string" && id.trim() !== ""))
  );
  if (unique.length === 0) return names;

  const db = getDbInstance();
  for (let start = 0; start < unique.length; start += DISPLAY_NAME_LOOKUP_CHUNK) {
    const chunk = unique.slice(start, start + DISPLAY_NAME_LOOKUP_CHUNK);
    const placeholders = chunk.map(() => "?").join(", ");
    const rows = db
      .prepare(`SELECT id, name FROM api_keys WHERE id IN (${placeholders})`)
      .all(...chunk) as DisplayNameRow[];
    for (const row of rows) {
      if (typeof row.name === "string" && row.name.trim() !== "") {
        names.set(row.id, row.name);
      }
    }
  }
  return names;
}

/** Async selected-backend lookup with the same sparse, non-secret display contract. */
export async function getApiKeyDisplayNamesAsync(
  ids: readonly string[]
): Promise<Map<string, string>> {
  const postgres = getSelectedPostgresDatabase();
  if (!postgres) return getApiKeyDisplayNames(ids);
  const names = new Map<string, string>();
  const unique = Array.from(
    new Set(ids.filter((id) => typeof id === "string" && id.trim() !== ""))
  );
  if (unique.length === 0) return names;
  return postgres.transaction(async (transaction) => {
    for (let start = 0; start < unique.length; start += DISPLAY_NAME_LOOKUP_CHUNK) {
      const chunk = unique.slice(start, start + DISPLAY_NAME_LOOKUP_CHUNK);
      const { rows } = await transaction.query<DisplayNameRow>(
        "SELECT id, name FROM omniroute.api_keys WHERE id = ANY($1::text[])",
        [chunk]
      );
      for (const row of rows) {
        if (typeof row.name === "string" && row.name.trim() !== "") names.set(row.id, row.name);
      }
    }
    return names;
  });
}
