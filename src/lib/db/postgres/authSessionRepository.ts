import { randomUUID } from "node:crypto";
import {
  encrypt,
  decrypt,
  looksEncrypted,
  requireExplicitStorageEncryptionKey,
} from "../encryption";
import type {
  CommandCodeAuthMetadata,
  CommandCodeAuthSafeStatus,
  CommandCodeAuthStatus,
  ConsumedCommandCodeAuthSecret,
} from "../commandCodeAuth";
import type { SyncTokenRecord } from "../syncTokens";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { asciiFoldSql } from "./routingSemantics";

type AuthSessionRow = {
  id: string;
  state_hash: string;
  status: CommandCodeAuthStatus;
  encrypted_api_key: string | null;
  metadata_json: string | null;
  created_at: string;
  expires_at: string;
  received_at: string | null;
  applied_at: string | null;
  updated_at: string;
};

function safeStatus(row: AuthSessionRow): CommandCodeAuthSafeStatus {
  let metadata: CommandCodeAuthMetadata | null = null;
  try {
    const parsed: unknown = row.metadata_json ? JSON.parse(row.metadata_json) : null;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      metadata = parsed as CommandCodeAuthMetadata;
    }
  } catch {
    /* malformed metadata does not expose the encrypted secret */
  }
  return {
    id: row.id,
    stateHash: row.state_hash,
    status: row.status,
    metadata,
    createdAt: row.created_at,
    expiresAt: row.expires_at,
    receivedAt: row.received_at,
    appliedAt: row.applied_at,
    updatedAt: row.updated_at,
  };
}

async function expire(transaction: PostgresTransaction, stateHash: string, now: string) {
  await transaction.query(
    `UPDATE omniroute.command_code_auth_sessions SET status='expired', updated_at=$1
     WHERE state_hash=$2 AND status IN ('pending', 'received') AND expires_at <= $1`,
    [now, stateHash]
  );
}

export async function createPostgresCommandAuth(
  database: PostgresDatabase,
  input: {
    stateHash: string;
    expiresAt: string;
  }
): Promise<CommandCodeAuthSafeStatus> {
  const now = new Date().toISOString();
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<AuthSessionRow>(
      `INSERT INTO omniroute.command_code_auth_sessions
       (id, state_hash, status, created_at, expires_at, updated_at)
       VALUES ($1, $2, 'pending', $3, $4, $3) RETURNING *`,
      [randomUUID(), input.stateHash, now, input.expiresAt]
    );
    return safeStatus(rows[0]);
  });
}

export async function receivePostgresCommandAuth(
  database: PostgresDatabase,
  input: {
    stateHash: string;
    apiKey: string;
    metadata?: CommandCodeAuthMetadata;
  }
): Promise<CommandCodeAuthSafeStatus | null> {
  requireExplicitStorageEncryptionKey();
  const encrypted = encrypt(input.apiKey);
  if (!looksEncrypted(encrypted))
    throw new Error("Command authentication secret must be encrypted");
  const now = new Date().toISOString();
  return database.transaction(async (transaction) => {
    await expire(transaction, input.stateHash, now);
    await transaction.query(
      `UPDATE omniroute.command_code_auth_sessions
       SET status='received', encrypted_api_key=$1, metadata_json=$2, received_at=$3, updated_at=$3
       WHERE state_hash=$4 AND status IN ('pending', 'received') AND expires_at > $3`,
      [
        encrypted,
        JSON.stringify({ ...(input.metadata || {}), receivedAt: now }),
        now,
        input.stateHash,
      ]
    );
    const { rows } = await transaction.query<AuthSessionRow>(
      "SELECT * FROM omniroute.command_code_auth_sessions WHERE state_hash=$1",
      [input.stateHash]
    );
    return rows[0] ? safeStatus(rows[0]) : null;
  });
}

export async function getPostgresCommandAuth(database: PostgresDatabase, stateHash: string) {
  return database.transaction(async (transaction) => {
    await expire(transaction, stateHash, new Date().toISOString());
    const { rows } = await transaction.query<AuthSessionRow>(
      "SELECT * FROM omniroute.command_code_auth_sessions WHERE state_hash=$1",
      [stateHash]
    );
    return rows[0] ? safeStatus(rows[0]) : null;
  });
}

export async function consumePostgresCommandAuth(
  database: PostgresDatabase,
  stateHash: string
): Promise<ConsumedCommandCodeAuthSecret | null> {
  requireExplicitStorageEncryptionKey();
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<AuthSessionRow>(
      "SELECT * FROM omniroute.command_code_auth_sessions WHERE state_hash=$1 FOR UPDATE",
      [stateHash]
    );
    const row = rows[0];
    const now = new Date().toISOString();
    if (!row || row.status !== "received" || !row.encrypted_api_key) return null;
    if (row.expires_at <= now) {
      await expire(transaction, stateHash, now);
      return null;
    }
    const apiKey = decrypt(row.encrypted_api_key);
    if (!apiKey) return null;
    const applied = await transaction.query<AuthSessionRow>(
      `UPDATE omniroute.command_code_auth_sessions SET status='applied', encrypted_api_key=NULL,
       applied_at=$1, updated_at=$1 WHERE id=$2 AND status='received' RETURNING *`,
      [now, row.id]
    );
    return applied.rows[0] ? { ...safeStatus(applied.rows[0]), apiKey } : null;
  });
}

type SyncRow = {
  id: string;
  name: string;
  token_hash: string;
  sync_api_key_id: string | null;
  revoked_at: string | null;
  last_used_at: string | null;
  created_at: string;
  updated_at: string;
};

function syncRecord(row: SyncRow | undefined): SyncTokenRecord | null {
  if (!row) return null;
  return {
    id: row.id,
    name: row.name,
    tokenHash: row.token_hash,
    syncApiKeyId: row.sync_api_key_id,
    revokedAt: row.revoked_at,
    lastUsedAt: row.last_used_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export async function listPostgresSyncTokens(
  database: PostgresDatabase
): Promise<SyncTokenRecord[]> {
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<SyncRow>(
      `SELECT * FROM omniroute.sync_tokens ORDER BY created_at::timestamptz DESC,
       ${asciiFoldSql("name")} COLLATE "C" ASC, insertion_order ASC`
    );
    return rows.map((row) => syncRecord(row)!);
  });
}

export async function getPostgresSyncToken(
  database: PostgresDatabase,
  field: "id" | "token_hash",
  value: string
) {
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<SyncRow>(
      `SELECT * FROM omniroute.sync_tokens WHERE ${field}=$1`,
      [value]
    );
    return syncRecord(rows[0]);
  });
}

export async function createPostgresSyncToken(
  database: PostgresDatabase,
  input: {
    name: string;
    tokenHash: string;
    syncApiKeyId?: string | null;
  }
): Promise<SyncTokenRecord> {
  const now = new Date().toISOString();
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<SyncRow>(
      `INSERT INTO omniroute.sync_tokens
       (id, name, token_hash, sync_api_key_id, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $5) RETURNING *`,
      [randomUUID(), input.name, input.tokenHash, input.syncApiKeyId || null, now]
    );
    return syncRecord(rows[0])!;
  });
}

export async function revokePostgresSyncToken(database: PostgresDatabase, id: string) {
  const now = new Date().toISOString();
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<SyncRow>(
      `UPDATE omniroute.sync_tokens SET
       revoked_at=COALESCE(revoked_at, $1),
       updated_at=CASE WHEN revoked_at IS NULL THEN $1 ELSE updated_at END
       WHERE id=$2 RETURNING *`,
      [now, id]
    );
    return syncRecord(rows[0]);
  });
}

export async function touchPostgresSyncToken(
  database: PostgresDatabase,
  id: string,
  usedAt: string
) {
  return database.transaction(async (transaction) => {
    const result = await transaction.query(
      "UPDATE omniroute.sync_tokens SET last_used_at=$1, updated_at=$1 WHERE id=$2",
      [usedAt, id]
    );
    return (result.rowCount || 0) > 0;
  });
}
