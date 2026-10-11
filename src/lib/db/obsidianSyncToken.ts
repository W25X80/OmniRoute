import { getDbInstance } from "./core";
import { assertSqliteBackendAllowed, getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresKnowledgeRepository } from "./postgres/knowledgeRepository";

const SYNC_TOKEN_KEY = "omniroute_sync_token";

export function getSyncToken(): string | null {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get("sync", SYNC_TOKEN_KEY) as { value?: string } | undefined;
    return typeof row?.value === "string" ? JSON.parse(row.value) : null;
  } catch {
    return null;
  }
}

export function setSyncToken(token: string | null): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    if (token === null) {
      db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
        "sync",
        SYNC_TOKEN_KEY
      );
    } else {
      const existing = db
        .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
        .get("sync", SYNC_TOKEN_KEY);
      if (existing) {
        db.prepare("UPDATE key_value SET value = ? WHERE namespace = ? AND key = ?").run(
          JSON.stringify(token),
          "sync",
          SYNC_TOKEN_KEY
        );
      } else {
        db.prepare("INSERT INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
          "sync",
          SYNC_TOKEN_KEY,
          JSON.stringify(token)
        );
      }
    }
  } catch {
    /* ignore */
  }
}

export async function getSyncTokenAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  return database
    ? createPostgresKnowledgeRepository(database).readSecret("sync", SYNC_TOKEN_KEY)
    : getSyncToken();
}
export async function setSyncTokenAsync(token: string | null): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setSyncToken(token);
  const repository = createPostgresKnowledgeRepository(database);
  if (token === null) return repository.remove("sync", SYNC_TOKEN_KEY);
  return repository.writeSecret("sync", SYNC_TOKEN_KEY, token);
}
