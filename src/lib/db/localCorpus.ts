import { getSelectedPostgresDatabase, assertSqliteBackendAllowed } from "./controlPlaneContext";
import { createPostgresKnowledgeRepository } from "./postgres/knowledgeRepository";
import { getDbInstance } from "./core";

const LOCAL_CORPUS_NAMESPACE = "local_corpus";
const LOCAL_CORPUS_ROOT_KEY = "root_path";

type KeyValueRow = {
  value?: string;
};

export interface LocalCorpusConfig {
  rootPath: string | null;
  configured: boolean;
}

export function getLocalCorpusRoot(): string | null {
  assertSqliteBackendAllowed();
  try {
    const row = getDbInstance()
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(LOCAL_CORPUS_NAMESPACE, LOCAL_CORPUS_ROOT_KEY) as KeyValueRow | undefined;
    if (typeof row?.value !== "string") return null;
    const parsed = JSON.parse(row.value);
    return typeof parsed === "string" && parsed.trim() ? parsed : null;
  } catch {
    return null;
  }
}

export function setLocalCorpusRoot(rootPath: string): void {
  assertSqliteBackendAllowed();
  getDbInstance()
    .prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)")
    .run(LOCAL_CORPUS_NAMESPACE, LOCAL_CORPUS_ROOT_KEY, JSON.stringify(rootPath));
}

export function clearLocalCorpusRoot(): void {
  assertSqliteBackendAllowed();
  getDbInstance()
    .prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?")
    .run(LOCAL_CORPUS_NAMESPACE, LOCAL_CORPUS_ROOT_KEY);
}

export function getLocalCorpusConfig(): LocalCorpusConfig {
  assertSqliteBackendAllowed();
  const rootPath = getLocalCorpusRoot();
  return { rootPath, configured: rootPath !== null };
}

export async function getLocalCorpusRootAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getLocalCorpusRoot();
  const repository = createPostgresKnowledgeRepository(database);
  const value = await repository.read("local_corpus", "root_path");
  return typeof value === "string" && value.trim() ? value : null;
}

export async function setLocalCorpusRootAsync(rootPath: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setLocalCorpusRoot(rootPath);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.write("local_corpus", "root_path", rootPath);
}

export async function clearLocalCorpusRootAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearLocalCorpusRoot();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("local_corpus", "root_path");
}

export async function getLocalCorpusConfigAsync(): Promise<LocalCorpusConfig> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getLocalCorpusConfig();
  const rootPath = await getLocalCorpusRootAsync();
  return { rootPath, configured: rootPath !== null };
}
