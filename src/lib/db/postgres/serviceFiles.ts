import { v4 as uuidv4 } from "uuid";
import { DEFAULT_BATCH_EXPIRATION_SECONDS } from "@/shared/constants/batch";
import type { FileRecord } from "../files";
import { rowToCamel } from "../core";
import type { PostgresTransaction } from "./database";
import { NATIVE_BLOB_LIMITS } from "./nativeBlobCarrier";
import { affected, first, rows, safeInteger, lockServices } from "./serviceQuery";

const METADATA =
  "id, bytes, created_at, filename, purpose, mime_type, api_key_id, expires_at, deleted_at";
function mapFile(row: Record<string, unknown>): FileRecord {
  const file = rowToCamel(row) as unknown as FileRecord;
  file.bytes = safeInteger(file.bytes);
  file.createdAt = safeInteger(file.createdAt);
  if (file.expiresAt != null) file.expiresAt = safeInteger(file.expiresAt);
  if (file.deletedAt != null) file.deletedAt = safeInteger(file.deletedAt);
  return file;
}

export async function createFile(
  transaction: PostgresTransaction,
  file: Omit<FileRecord, "id" | "createdAt">
): Promise<FileRecord> {
  await lockServices(transaction);
  const createdAt = Math.floor(Date.now() / 1000);
  const record: FileRecord = {
    bytes: file.bytes,
    filename: file.filename,
    purpose: file.purpose,
    id: "file-" + uuidv4().replaceAll("-", "").substring(0, 24),
    createdAt,
    content: file.content ?? null,
    mimeType: file.mimeType ?? null,
    apiKeyId: file.apiKeyId ?? null,
    expiresAt:
      file.expiresAt === undefined && file.purpose === "batch"
        ? createdAt + DEFAULT_BATCH_EXPIRATION_SECONDS
        : (file.expiresAt ?? null),
    deletedAt: null,
  };
  await transaction.query(
    `INSERT INTO omniroute.files
    (id, bytes, created_at, filename, purpose, content, mime_type, api_key_id, expires_at, deleted_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
    [
      record.id,
      record.bytes,
      record.createdAt,
      record.filename,
      record.purpose,
      record.content,
      record.mimeType,
      record.apiKeyId,
      record.expiresAt,
      record.deletedAt,
    ]
  );
  return record;
}
export async function getFile(
  transaction: PostgresTransaction,
  id: string
): Promise<FileRecord | null> {
  const row = await first(
    transaction,
    `SELECT ${METADATA} FROM omniroute.files WHERE id = $1 AND deleted_at IS NULL`,
    [id]
  );
  return row ? mapFile(row) : null;
}
export async function getFileContent(
  transaction: PostgresTransaction,
  id: string
): Promise<Buffer | null> {
  // Default bytea text decoding first constructs a hex string. Read bounded
  // fields while retaining one row version across the complete transaction.
  const row = await first<{ content_length: number | null }>(
    transaction,
    "SELECT octet_length(content) AS content_length FROM omniroute.files WHERE id = $1 AND deleted_at IS NULL FOR SHARE",
    [id]
  );
  if (!row || row.content_length === null) return null;
  const size = safeInteger(row.content_length);
  if (size < 0) throw new Error("Stored file content is incomplete or invalid");
  const content = Buffer.allocUnsafe(size);
  for (let offset = 0; offset < size; offset += NATIVE_BLOB_LIMITS.chunkBytes) {
    const length = Math.min(NATIVE_BLOB_LIMITS.chunkBytes, size - offset);
    const chunk = await first<{ content: Buffer | null }>(
      transaction,
      "SELECT substr(content, $2, $3) AS content FROM omniroute.files WHERE id = $1 AND deleted_at IS NULL",
      [id, offset + 1, length]
    );
    if (!Buffer.isBuffer(chunk?.content) || chunk.content.byteLength !== length) {
      throw new Error("Stored file content is incomplete or invalid");
    }
    chunk.content.copy(content, offset);
  }
  return content;
}
export async function listFiles(
  transaction: PostgresTransaction,
  options: Parameters<typeof import("../files").listFiles>[0] = {}
): Promise<FileRecord[]> {
  const { apiKeyId, purpose, limit = 20, after, order = "desc" } = options;
  const conditions = ["deleted_at IS NULL"];
  const values: unknown[] = [];
  const bind = (value: unknown) => {
    values.push(value);
    return `$${values.length}`;
  };
  if (apiKeyId) conditions.push(`api_key_id = ${bind(apiKeyId)}`);
  if (purpose) conditions.push(`purpose = ${bind(purpose)}`);
  if (after) {
    const cursor = await getFile(transaction, after);
    if (cursor && (!apiKeyId || cursor.apiKeyId === apiKeyId)) {
      const comparison = order === "asc" ? ">" : "<";
      const created = bind(cursor.createdAt);
      conditions.push(
        `(created_at ${comparison} ${created} OR (created_at = ${created} AND id COLLATE "C" ${comparison} ${bind(after)}))`
      );
    }
  }
  const direction = order === "asc" ? "ASC" : "DESC";
  const limitParameter = bind(limit);
  return (
    await rows(
      transaction,
      `SELECT ${METADATA} FROM omniroute.files WHERE ${conditions.join(" AND ")}
    ORDER BY created_at ${direction}, id COLLATE "C" ${direction}
    LIMIT CASE WHEN ${limitParameter}::bigint < 0 THEN NULL ELSE ${limitParameter}::bigint END`,
      values
    )
  ).map(mapFile);
}
export async function countFiles(
  transaction: PostgresTransaction,
  options: { apiKeyId?: string; purpose?: string } = {}
): Promise<number> {
  const conditions = ["deleted_at IS NULL"];
  const values: unknown[] = [];
  if (options.apiKeyId) {
    values.push(options.apiKeyId);
    conditions.push(`api_key_id = $${values.length}`);
  }
  if (options.purpose) {
    values.push(options.purpose);
    conditions.push(`purpose = $${values.length}`);
  }
  const row = await first<{ count: string }>(
    transaction,
    `SELECT count(*) AS count FROM omniroute.files WHERE ${conditions.join(" AND ")}`,
    values
  );
  return safeInteger(row?.count ?? 0);
}
export async function deleteFile(transaction: PostgresTransaction, id: string): Promise<boolean> {
  await lockServices(transaction);
  return (
    (await affected(
      transaction,
      "UPDATE omniroute.files SET deleted_at = $1, content = NULL WHERE id = $2",
      [Math.floor(Date.now() / 1000), id]
    )) > 0
  );
}
export async function deleteFileOwnedBy(
  transaction: PostgresTransaction,
  id: string,
  apiKeyId: string
): Promise<boolean> {
  if (typeof apiKeyId !== "string" || apiKeyId.trim() === "")
    throw new Error("deleteFileOwnedBy: apiKeyId is required");
  await lockServices(transaction);
  return (
    (await affected(
      transaction,
      "UPDATE omniroute.files SET deleted_at = $1, content = NULL WHERE id = $2 AND api_key_id = $3",
      [Math.floor(Date.now() / 1000), id, apiKeyId]
    )) > 0
  );
}
export async function pruneExpiredFiles(
  transaction: PostgresTransaction,
  now: number
): Promise<number> {
  await lockServices(transaction);
  return affected(
    transaction,
    "UPDATE omniroute.files SET deleted_at = $1, content = NULL WHERE expires_at IS NOT NULL AND expires_at < $1 AND deleted_at IS NULL",
    [now]
  );
}
