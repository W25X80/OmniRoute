import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresCloudAgentRepository } from "./postgres/cloudAgentRepository";
import { requireExplicitStorageEncryptionKey } from "./encryption";
import { getDbInstance } from "@/lib/db/core";
import { encrypt, decrypt } from "@/lib/db/encryption";
import type { AgentCredentials } from "@/lib/cloudAgent/baseAgent";

// The `cloud_agent_credentials` table is provisioned by migration
// `061_cloud_agent_credentials.sql` at database initialization (see
// src/lib/db/migrations/). Do not create it inline here — the project
// migration policy requires versioned, transaction-wrapped DDL.

/** Mask API key for display — show last 4 chars only */
export function maskApiKey(key: string): string {
  if (!key || key.length <= 4) return "****";
  return "****" + key.slice(-4);
}

/** Get decrypted credentials for a provider */
export function getCloudAgentCredentialFromDb(providerId: string): AgentCredentials | null {
  const db = getDbInstance();
  const row = db
    .prepare(
      "SELECT api_key_encrypted, base_url FROM cloud_agent_credentials WHERE provider_id = ?"
    )
    .get(providerId) as { api_key_encrypted: string; base_url: string | null } | undefined;

  if (!row) return null;

  const decryptedKey = decrypt(row.api_key_encrypted);
  if (!decryptedKey) return null;

  const creds: AgentCredentials = { apiKey: decryptedKey };
  if (row.base_url) creds.baseUrl = row.base_url;
  return creds;
}

/** List all credentials with masked keys */
export function listCloudAgentCredentials(): Array<{
  providerId: string;
  apiKey: string;
  baseUrl: string | null;
  updatedAt: string;
}> {
  const db = getDbInstance();
  const rows = db
    .prepare(
      "SELECT provider_id, api_key_encrypted, base_url, updated_at FROM cloud_agent_credentials"
    )
    .all() as {
    provider_id: string;
    api_key_encrypted: string;
    base_url: string | null;
    updated_at: string;
  }[];

  return rows.map((row) => {
    const decrypted = decrypt(row.api_key_encrypted) ?? "";
    return {
      providerId: row.provider_id,
      apiKey: maskApiKey(decrypted),
      baseUrl: row.base_url,
      updatedAt: row.updated_at,
    };
  });
}

/** Save or update credentials (encrypts API key at rest) */
export function saveCloudAgentCredential(
  providerId: string,
  apiKey: string,
  baseUrl?: string
): void {
  const encrypted = encrypt(apiKey);
  if (!encrypted) throw new Error("Failed to encrypt API key");

  const db = getDbInstance();
  db.prepare(
    `INSERT INTO cloud_agent_credentials (provider_id, api_key_encrypted, base_url, updated_at)
     VALUES (@providerId, @apiKey, @baseUrl, datetime('now'))
     ON CONFLICT(provider_id) DO UPDATE SET
       api_key_encrypted = excluded.api_key_encrypted,
       base_url = excluded.base_url,
       updated_at = excluded.updated_at`
  ).run({ providerId, apiKey: encrypted, baseUrl: baseUrl ?? null });
}

/** Delete credentials for a provider */
export function deleteCloudAgentCredential(providerId: string): void {
  const db = getDbInstance();
  db.prepare("DELETE FROM cloud_agent_credentials WHERE provider_id = ?").run(providerId);
}

export async function getCloudAgentCredentialFromDbAsync(
  providerId: string
): Promise<AgentCredentials | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getCloudAgentCredentialFromDb(providerId);
  requireExplicitStorageEncryptionKey();
  const row =
    await createPostgresCloudAgentRepository(database).readCloudAgentCredential(providerId);
  if (!row) return null;
  const apiKey = decrypt(row.api_key_encrypted);
  return apiKey ? { apiKey, ...(row.base_url ? { baseUrl: row.base_url } : {}) } : null;
}
export async function listCloudAgentCredentialsAsync(): Promise<
  ReturnType<typeof listCloudAgentCredentials>
> {
  const database = getSelectedPostgresDatabase();
  if (!database) return listCloudAgentCredentials();
  requireExplicitStorageEncryptionKey();
  const rows = await createPostgresCloudAgentRepository(database).listCloudAgentCredentialRows();
  return rows.map((row) => ({
    providerId: row.provider_id,
    apiKey: maskApiKey(decrypt(row.api_key_encrypted) ?? ""),
    baseUrl: row.base_url,
    updatedAt: row.updated_at,
  }));
}
export async function saveCloudAgentCredentialAsync(
  providerId: string,
  apiKey: string,
  baseUrl?: string
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    saveCloudAgentCredential(providerId, apiKey, baseUrl);
    return;
  }
  requireExplicitStorageEncryptionKey();
  const encrypted = encrypt(apiKey);
  if (!encrypted) throw new Error("Failed to encrypt API key");
  await createPostgresCloudAgentRepository(database).saveCloudAgentCredential(
    providerId,
    encrypted,
    baseUrl
  );
}
export async function deleteCloudAgentCredentialAsync(providerId: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    deleteCloudAgentCredential(providerId);
    return;
  }
  await createPostgresCloudAgentRepository(database).deleteCloudAgentCredential(providerId);
}
