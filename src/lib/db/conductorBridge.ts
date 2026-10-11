import { getSelectedPostgresDatabase, assertSqliteBackendAllowed } from "./controlPlaneContext";
import { createPostgresKnowledgeRepository } from "./postgres/knowledgeRepository";
/**
 * Conductor bridge persistence — SSE cursor (`last_event_id`) for the hub mirror.
 *
 * Uses the existing `key_value` table under a dedicated namespace (no migration
 * needed — same pattern as settings.ts). The cursor lets the bridge resume the
 * hub SSE from where it stopped; the hub replay covers the gap.
 */

import { getDbInstance } from "./core";

const NAMESPACE = "conductor";
const CURSOR_KEY = "last_event_id";

export function getConductorCursor(): string | null {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const row = db
    .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
    .get(NAMESPACE, CURSOR_KEY) as { value: string } | undefined;
  if (!row) return null;
  try {
    const parsed = JSON.parse(row.value);
    return typeof parsed === "string" ? parsed : null;
  } catch {
    return null;
  }
}

export function setConductorCursor(value: string): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
    NAMESPACE,
    CURSOR_KEY,
    JSON.stringify(value)
  );
}

export async function getConductorCursorAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getConductorCursor();
  const repository = createPostgresKnowledgeRepository(database);
  const value = await repository.read("conductor", "last_event_id");
  return typeof value === "string" ? value : null;
}

export async function setConductorCursorAsync(value: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setConductorCursor(value);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.write("conductor", "last_event_id", value);
}
