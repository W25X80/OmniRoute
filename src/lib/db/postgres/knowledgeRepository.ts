import {
  decrypt,
  encrypt,
  looksEncrypted,
  requireExplicitStorageEncryptionKey,
} from "../encryption";
import type { PostgresDatabase } from "./database";
import { acquirePostgresLock } from "./locks";

export type KnowledgeNamespace = "notion" | "obsidian" | "local_corpus" | "conductor" | "sync";

/** Internal namespaced metadata owner; host content and external services stay outside SQL. */
export function createPostgresKnowledgeRepository(database: PostgresDatabase) {
  async function read(namespace: KnowledgeNamespace, key: string): Promise<unknown> {
    const { rows } = await database.transaction((transaction) =>
      transaction.query<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace=$1 AND key=$2",
        [namespace, key]
      )
    );
    if (!rows[0]) return null;
    try {
      return JSON.parse(rows[0].value);
    } catch {
      return null;
    }
  }
  function decodeSecret(value: unknown): string | null {
    if (typeof value !== "string" || !value) return null;
    requireExplicitStorageEncryptionKey();
    if (!looksEncrypted(value)) return value;
    const plaintext = decrypt(value);
    if (plaintext == null) throw new Error("Knowledge credential could not be decrypted");
    return plaintext;
  }
  async function readMany(
    namespace: KnowledgeNamespace,
    keys: readonly string[]
  ): Promise<Record<string, unknown>> {
    const { rows } = await database.transaction((tx) =>
      tx.query<{ key: string; value: string }>(
        "SELECT key,value FROM omniroute.key_value WHERE namespace=$1 AND key=ANY($2::text[])",
        [namespace, keys]
      )
    );
    const result: Record<string, unknown> = {};
    for (const row of rows) {
      try {
        result[row.key] = JSON.parse(row.value);
      } catch {
        result[row.key] = null;
      }
    }
    return result;
  }
  async function readSecret(namespace: KnowledgeNamespace, key: string): Promise<string | null> {
    const value = await read(namespace, key);
    return decodeSecret(value);
  }

  async function write(
    namespace: KnowledgeNamespace,
    key: string,
    value: unknown,
    firstOnly = false
  ): Promise<void> {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) throw new Error("Knowledge metadata value is not serializable");
    await database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "knowledge");
      await transaction.query(
        `INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3)
         ON CONFLICT(namespace,key) ${firstOnly ? "DO NOTHING" : "DO UPDATE SET value=EXCLUDED.value"}`,
        [namespace, key, encoded]
      );
    });
  }
  async function writeSecret(
    namespace: KnowledgeNamespace,
    key: string,
    value: string,
    firstOnly = false
  ): Promise<void> {
    requireExplicitStorageEncryptionKey();
    if (typeof value !== "string") throw new Error("Knowledge credential must be a string");
    const encoded = encrypt(value);
    if (value.length && !looksEncrypted(encoded))
      throw new Error("Knowledge credential encryption failed");
    await write(namespace, key, encoded, firstOnly);
  }
  async function remove(namespace: KnowledgeNamespace, key: string): Promise<void> {
    await database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "knowledge");
      await transaction.query("DELETE FROM omniroute.key_value WHERE namespace=$1 AND key=$2", [
        namespace,
        key,
      ]);
    });
  }
  async function writeMany(
    namespace: KnowledgeNamespace,
    entries: readonly { key: string; value: unknown; secret?: boolean }[]
  ): Promise<void> {
    const prepared = entries.map((entry) => {
      let value = entry.value;
      if (entry.secret) {
        requireExplicitStorageEncryptionKey();
        if (typeof value !== "string") throw new Error("Knowledge credential must be a string");
        const cipher = encrypt(value);
        if (value.length && !looksEncrypted(cipher))
          throw new Error("Knowledge credential encryption failed");
        value = cipher;
      }
      const encoded = JSON.stringify(value);
      if (encoded === undefined) throw new Error("Knowledge metadata value is not serializable");
      return { key: entry.key, encoded };
    });
    await database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "knowledge");
      for (const entry of prepared)
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3) ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value",
          [namespace, entry.key, entry.encoded]
        );
    });
  }
  async function removeMany(namespace: KnowledgeNamespace, keys: readonly string[]): Promise<void> {
    await database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "knowledge");
      await tx.query("DELETE FROM omniroute.key_value WHERE namespace=$1 AND key=ANY($2::text[])", [
        namespace,
        keys,
      ]);
    });
  }
  return {
    read,
    readMany,
    decodeSecret,
    readSecret,
    write,
    writeSecret,
    remove,
    writeMany,
    removeMany,
  };
}
