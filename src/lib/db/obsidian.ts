import { getSelectedPostgresDatabase, assertSqliteBackendAllowed } from "./controlPlaneContext";
import { createPostgresKnowledgeRepository } from "./postgres/knowledgeRepository";
import { getApiKeyContextSourceAsync } from "./apiKeyContextSources";
import { getDbInstance } from "./core";
import { getApiKeyContextSource } from "./apiKeyContextSources";
import { encrypt, decrypt } from "./encryption";

const OBSIDIAN_NAMESPACE = "obsidian";
const OBSIDIAN_TOKEN_KEY = "api_key";

type KeyValueRow = {
  value?: string;
};

export function getObsidianToken(): string | null {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(OBSIDIAN_NAMESPACE, OBSIDIAN_TOKEN_KEY) as KeyValueRow | undefined;
    if (typeof row?.value !== "string") return null;
    const parsed = JSON.parse(row.value);
    if (typeof parsed !== "string" || parsed.length === 0) return null;
    // Graceful fallback: if decrypt fails (e.g. no key set) return as-is
    return decrypt(parsed) ?? parsed;
  } catch {
    return null;
  }
}

export function setObsidianToken(token: string): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const encrypted = encrypt(token) ?? token;
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      OBSIDIAN_NAMESPACE,
      OBSIDIAN_TOKEN_KEY,
      JSON.stringify(encrypted)
    );
  } catch {
    // Non-fatal — token still works in-memory if persistence fails.
  }
}

export function clearObsidianToken(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      OBSIDIAN_NAMESPACE,
      OBSIDIAN_TOKEN_KEY
    );
  } catch {
    // Non-fatal.
  }
}

export function getObsidianBaseUrl(): string {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(OBSIDIAN_NAMESPACE, "base_url") as KeyValueRow | undefined;
    if (typeof row?.value === "string") {
      const parsed = JSON.parse(row.value);
      return typeof parsed === "string" && parsed.length > 0 ? parsed : "http://127.0.0.1:27123";
    }
    return "http://127.0.0.1:27123";
  } catch {
    return "http://127.0.0.1:27123";
  }
}

export function setObsidianBaseUrl(url: string): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      OBSIDIAN_NAMESPACE,
      "base_url",
      JSON.stringify(url)
    );
  } catch {
    // Non-fatal.
  }
}

export function clearObsidianBaseUrl(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      OBSIDIAN_NAMESPACE,
      "base_url"
    );
  } catch {
    // Non-fatal.
  }
}

export function getObsidianVaultPath(): string | null {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(OBSIDIAN_NAMESPACE, "vault_path") as KeyValueRow | undefined;
    if (typeof row?.value === "string") {
      const parsed = JSON.parse(row.value);
      return typeof parsed === "string" && parsed.length > 0 ? parsed : null;
    }
    return null;
  } catch {
    return null;
  }
}

export function setObsidianVaultPath(vaultPath: string): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      OBSIDIAN_NAMESPACE,
      "vault_path",
      JSON.stringify(vaultPath)
    );
  } catch {
    // Non-fatal.
  }
}

export function clearObsidianVaultPath(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      OBSIDIAN_NAMESPACE,
      "vault_path"
    );
  } catch {
    // Non-fatal.
  }
}

export function getWebdavUsername(): string | null {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(OBSIDIAN_NAMESPACE, "webdav_username") as KeyValueRow | undefined;
    if (typeof row?.value === "string") {
      const parsed = JSON.parse(row.value);
      return typeof parsed === "string" && parsed.length > 0 ? parsed : null;
    }
    return null;
  } catch {
    return null;
  }
}

export function setWebdavUsername(username: string): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      OBSIDIAN_NAMESPACE,
      "webdav_username",
      JSON.stringify(username)
    );
  } catch {
    // Non-fatal.
  }
}

export function clearWebdavUsername(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      OBSIDIAN_NAMESPACE,
      "webdav_username"
    );
  } catch {
    // Non-fatal.
  }
}

export function getWebdavPassword(): string | null {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(OBSIDIAN_NAMESPACE, "webdav_password") as KeyValueRow | undefined;
    if (typeof row?.value === "string") {
      const parsed = JSON.parse(row.value);
      if (typeof parsed !== "string" || parsed.length === 0) return null;
      // Graceful fallback: if decrypt fails return as-is (plaintext backward compat)
      return decrypt(parsed) ?? parsed;
    }
    return null;
  } catch {
    return null;
  }
}

export function setWebdavPassword(password: string): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const encrypted = encrypt(password) ?? password;
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      OBSIDIAN_NAMESPACE,
      "webdav_password",
      JSON.stringify(encrypted)
    );
  } catch {
    // Non-fatal.
  }
}

export function clearWebdavPassword(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      OBSIDIAN_NAMESPACE,
      "webdav_password"
    );
  } catch {
    // Non-fatal.
  }
}

export function getWebdavEnabled(): boolean {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    const row = db
      .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
      .get(OBSIDIAN_NAMESPACE, "webdav_enabled") as KeyValueRow | undefined;
    if (typeof row?.value === "string") {
      return JSON.parse(row.value) === true;
    }
    return false;
  } catch {
    return false;
  }
}

export function setWebdavEnabled(enabled: boolean): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      OBSIDIAN_NAMESPACE,
      "webdav_enabled",
      JSON.stringify(enabled)
    );
  } catch {
    // Non-fatal.
  }
}

export function clearWebdavEnabled(): void {
  assertSqliteBackendAllowed();
  try {
    const db = getDbInstance();
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
      OBSIDIAN_NAMESPACE,
      "webdav_enabled"
    );
  } catch {
    // Non-fatal.
  }
}

export function getObsidianConfig(): {
  token: string | null;
  connected: boolean;
  baseUrl: string;
  vaultPath: string | null;
} {
  assertSqliteBackendAllowed();
  const token = getObsidianToken();
  const baseUrl = getObsidianBaseUrl();
  const vaultPath = getObsidianVaultPath();
  return {
    token,
    connected: token !== null && token.length > 0,
    baseUrl,
    vaultPath,
  };
}

export function getObsidianConfigForApiKey(apiKeyId: string | null | undefined): {
  token: string | null;
  baseUrl: string;
  vaultPath: string | null;
  source: "api_key" | "global";
} {
  assertSqliteBackendAllowed();
  if (apiKeyId) {
    try {
      const perKey = getApiKeyContextSource(apiKeyId, "obsidian");
      if (perKey && perKey.enabled && perKey.token) {
        return {
          token: perKey.token,
          baseUrl: perKey.baseUrl || getObsidianBaseUrl(),
          vaultPath: perKey.vaultPath || getObsidianVaultPath(),
          source: "api_key",
        };
      }
    } catch {
      // Per-key config not available — fall through to global
    }
  }
  return {
    token: getObsidianToken(),
    baseUrl: getObsidianBaseUrl(),
    vaultPath: getObsidianVaultPath(),
    source: "global",
  };
}

export async function getObsidianTokenAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getObsidianToken();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.readSecret("obsidian", "api_key");
}

export async function setObsidianTokenAsync(token: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setObsidianToken(token);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.writeSecret("obsidian", "api_key", token);
}

export async function clearObsidianTokenAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearObsidianToken();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("obsidian", "api_key");
}

export async function getObsidianBaseUrlAsync(): Promise<string> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getObsidianBaseUrl();
  const repository = createPostgresKnowledgeRepository(database);
  const value = await repository.read("obsidian", "base_url");
  return typeof value === "string" && value.length > 0 ? value : "http://127.0.0.1:27123";
}

export async function setObsidianBaseUrlAsync(url: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setObsidianBaseUrl(url);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.write("obsidian", "base_url", url);
}

export async function clearObsidianBaseUrlAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearObsidianBaseUrl();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("obsidian", "base_url");
}

export async function getObsidianVaultPathAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getObsidianVaultPath();
  const repository = createPostgresKnowledgeRepository(database);
  const value = await repository.read("obsidian", "vault_path");
  return typeof value === "string" && value.length > 0 ? value : null;
}

export async function setObsidianVaultPathAsync(vaultPath: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setObsidianVaultPath(vaultPath);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.write("obsidian", "vault_path", vaultPath);
}

export async function clearObsidianVaultPathAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearObsidianVaultPath();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("obsidian", "vault_path");
}

export async function getWebdavUsernameAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getWebdavUsername();
  const repository = createPostgresKnowledgeRepository(database);
  const value = await repository.read("obsidian", "webdav_username");
  return typeof value === "string" && value.length > 0 ? value : null;
}

export async function setWebdavUsernameAsync(username: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setWebdavUsername(username);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.write("obsidian", "webdav_username", username);
}

export async function clearWebdavUsernameAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearWebdavUsername();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("obsidian", "webdav_username");
}

export async function getWebdavPasswordAsync(): Promise<string | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getWebdavPassword();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.readSecret("obsidian", "webdav_password");
}

export async function setWebdavPasswordAsync(password: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setWebdavPassword(password);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.writeSecret("obsidian", "webdav_password", password);
}

export async function clearWebdavPasswordAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearWebdavPassword();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("obsidian", "webdav_password");
}

export async function getWebdavEnabledAsync(): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getWebdavEnabled();
  const repository = createPostgresKnowledgeRepository(database);
  return (await repository.read("obsidian", "webdav_enabled")) === true;
}

export async function setWebdavEnabledAsync(enabled: boolean): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return setWebdavEnabled(enabled);
  const repository = createPostgresKnowledgeRepository(database);
  return repository.write("obsidian", "webdav_enabled", enabled);
}

export async function clearWebdavEnabledAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearWebdavEnabled();
  const repository = createPostgresKnowledgeRepository(database);
  return repository.remove("obsidian", "webdav_enabled");
}

export async function getObsidianConfigAsync(): Promise<{
  token: string | null;
  connected: boolean;
  baseUrl: string;
  vaultPath: string | null;
}> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getObsidianConfig();
  const repository = createPostgresKnowledgeRepository(database);
  const values = await repository.readMany("obsidian", ["api_key", "base_url", "vault_path"]);
  const token = repository.decodeSecret(values.api_key);
  const baseUrl =
    typeof values.base_url === "string" && values.base_url.length
      ? values.base_url
      : "http://127.0.0.1:27123";
  const vaultPath =
    typeof values.vault_path === "string" && values.vault_path.length ? values.vault_path : null;
  return {
    token,
    connected: token !== null && token.length > 0,
    baseUrl,
    vaultPath,
  };
}

export async function getObsidianConfigForApiKeyAsync(
  apiKeyId: string | null | undefined
): Promise<{
  token: string | null;
  baseUrl: string;
  vaultPath: string | null;
  source: "api_key" | "global";
}> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getObsidianConfigForApiKey(apiKeyId);
  if (apiKeyId) {
    const perKey = await getApiKeyContextSourceAsync(apiKeyId, "obsidian");
    if (perKey && perKey.enabled && perKey.token) {
      const global = !perKey.baseUrl || !perKey.vaultPath ? await getObsidianConfigAsync() : null;
      return {
        token: perKey.token,
        baseUrl: perKey.baseUrl || global!.baseUrl,
        vaultPath: perKey.vaultPath || global!.vaultPath,
        source: "api_key",
      };
    }
  }
  const config = await getObsidianConfigAsync();
  return {
    token: config.token,
    baseUrl: config.baseUrl,
    vaultPath: config.vaultPath,
    source: "global",
  };
}

/** One metadata snapshot for an injected host WebDAV reader; credentials remain at rest encrypted. */
export async function getWebdavConfigurationAsync(): Promise<{
  enabled: boolean;
  username: string | null;
  password: string | null;
  vaultPath: string | null;
}> {
  const database = getSelectedPostgresDatabase();
  if (!database)
    return {
      enabled: getWebdavEnabled(),
      username: getWebdavUsername(),
      password: getWebdavPassword(),
      vaultPath: getObsidianVaultPath(),
    };
  const repository = createPostgresKnowledgeRepository(database);
  const values = await repository.readMany("obsidian", [
    "webdav_enabled",
    "webdav_username",
    "webdav_password",
    "vault_path",
  ]);
  return {
    enabled: values.webdav_enabled === true,
    username:
      typeof values.webdav_username === "string" && values.webdav_username.length
        ? values.webdav_username
        : null,
    password: repository.decodeSecret(values.webdav_password),
    vaultPath:
      typeof values.vault_path === "string" && values.vault_path.length ? values.vault_path : null,
  };
}
export async function saveObsidianSyncConfigurationAsync(
  vaultPath: string,
  username: string,
  password: string
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    setObsidianVaultPath(vaultPath);
    setWebdavUsername(username);
    setWebdavPassword(password);
    setWebdavEnabled(true);
    return;
  }
  await createPostgresKnowledgeRepository(database).writeMany("obsidian", [
    { key: "vault_path", value: vaultPath },
    { key: "webdav_username", value: username },
    { key: "webdav_password", value: password, secret: true },
    { key: "webdav_enabled", value: true },
  ]);
}
export async function clearObsidianSyncConfigurationAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    clearObsidianVaultPath();
    clearWebdavUsername();
    clearWebdavPassword();
    clearWebdavEnabled();
    return;
  }
  await createPostgresKnowledgeRepository(database).removeMany("obsidian", [
    "vault_path",
    "webdav_username",
    "webdav_password",
    "webdav_enabled",
  ]);
}

export async function saveObsidianConnectionAsync(token: string, baseUrl?: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    setObsidianToken(token);
    if (baseUrl) setObsidianBaseUrl(baseUrl);
    return;
  }
  const entries: Array<{ key: string; value: unknown; secret?: boolean }> = [
    { key: "api_key", value: token, secret: true },
  ];
  if (baseUrl) entries.push({ key: "base_url", value: baseUrl });
  await createPostgresKnowledgeRepository(database).writeMany("obsidian", entries);
}
