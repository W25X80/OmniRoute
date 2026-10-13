import {
  decrypt,
  encrypt,
  looksEncrypted,
  requireExplicitStorageEncryptionKey,
} from "../encryption";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

export interface SecretRepository {
  get(key: string): Promise<string | null>;
  getOrCreate(key: string, generate: () => string): Promise<string>;
}

export class PostgresSecretError extends Error {
  constructor() {
    super("Persistent secret is invalid or cannot be decrypted");
    this.name = "PostgresSecretError";
  }
}

function decode(value: string): string {
  let stored: unknown;
  try {
    stored = JSON.parse(value);
  } catch {
    throw new PostgresSecretError();
  }
  if (typeof stored !== "string" || stored.trim() === "") throw new PostgresSecretError();
  const plaintext = decrypt(stored, { quiet: true });
  if (typeof plaintext !== "string" || plaintext.trim() === "") throw new PostgresSecretError();
  return plaintext;
}

async function read(tx: PostgresTransaction, key: string): Promise<string | null> {
  const { rows } = await tx.query<{ value: string }>(
    "SELECT value FROM omniroute.key_value WHERE namespace = 'secrets' AND key = $1",
    [key]
  );
  return rows.length ? decode(rows[0].value) : null;
}

/** First writer wins, and every caller returns the durable winner after COMMIT acknowledgement. */
export function createPostgresSecretRepository(database: PostgresDatabase): SecretRepository {
  return {
    get: (key) => database.transaction((tx) => read(tx, key)),
    getOrCreate: (key, generate) =>
      database.transaction(async (tx) => {
        requireExplicitStorageEncryptionKey();
        // Serialize even the absent-row case before generating candidate secrets.
        await acquirePostgresLock(tx, "secrets");
        const existing = await read(tx, key);
        if (existing !== null) return existing;
        const candidate = generate();
        if (typeof candidate !== "string" || candidate.trim() === "")
          throw new PostgresSecretError();
        const ciphertext = encrypt(candidate);
        if (!looksEncrypted(ciphertext)) throw new PostgresSecretError();
        await tx.query(
          `INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('secrets', $1, $2)
           ON CONFLICT(namespace, key) DO NOTHING`,
          [key, JSON.stringify(ciphertext)]
        );
        const persisted = await read(tx, key);
        if (persisted === null) throw new PostgresSecretError();
        return persisted;
      }),
  };
}
