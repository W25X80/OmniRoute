import { getSelectedPostgresDatabase, assertSqliteBackendAllowed } from "./controlPlaneContext";
import { createPostgresKnowledgeRepository } from "./postgres/knowledgeRepository";
import { getDbInstance } from "./core";

const NOTION_NAMESPACE = "notion";
const NOTION_TOKEN_KEY = "integration_token";

type KeyValueRow = {
  value?: string;
};

export function getNotionToken(): string | null {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(NOTION_NAMESPACE, NOTION_TOKEN_KEY) as KeyValueRow | undefined;
    return typeof row?.value === "string" ? JSON.parse(row.value) : null;
  } catch {
    return null;
  }
}

export function setNotionToken(token: string): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("INSERT OR IGNORE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      NOTION_NAMESPACE,
      NOTION_TOKEN_KEY,
      JSON.stringify(token)
    );
  } catch {
    // Non-fatal — token still works in-memory if persistence fails.
  }
}

export function clearNotionToken(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      NOTION_NAMESPACE,
      NOTION_TOKEN_KEY
    );
  } catch {
    // Non-fatal.
  }
}

export function getNotionConfig(): {
  token: string | null;
  connected: boolean;
} {
  assertSqliteBackendAllowed();
  const token = getNotionToken();
  return { token, connected: token !== null && token.length > 0 };
}

export async function getNotionTokenAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getNotionToken();
  const repository = createPostgresKnowledgeRepository(database);
  const value = await repository.read("notion", "integration_token");
  return value === "" ? "" : repository.decodeSecret(value);
}

export async function setNotionTokenAsync(token: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setNotionToken(token);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.writeSecret("notion", "integration_token", token, true);
}

export async function clearNotionTokenAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearNotionToken();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("notion", "integration_token");
}

export async function getNotionConfigAsync(): Promise<{
  token: string | null;
  connected: boolean;
}> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getNotionConfig();
  const token = await getNotionTokenAsync();
  return { token, connected: token !== null && token.length > 0 };
}
