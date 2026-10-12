import { v4 as uuidv4 } from "uuid";
import { DEFAULT_BATCH_EXPIRATION_SECONDS } from "@/shared/constants/batch";
import type { FileRecord } from "../files";
import { rowToCamel } from "../core";
import type { PostgresTransaction } from "./database";
import { NATIVE_BLOB_LIMITS } from "./nativeBlobCarrier";
import { first, rows, safeInteger, lockServices } from "./serviceQuery";

import { openStoredFileContent, replaceStoredFileContent } from "./serviceFileContent";

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
  if (file.content && file.content.byteLength > NATIVE_BLOB_LIMITS.fileBytes) {
    throw new Error("File content exceeds the supported size limit");
  }
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
      null,
      record.mimeType,
      record.apiKeyId,
      record.expiresAt,
      record.deletedAt,
    ]
  );
  await replaceStoredFileContent(transaction, record.id, record.content ?? null);
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
  id: string,
  reserve?: (sizeBytes: number) => void
): Promise<Buffer | null> {
  const stored = await openStoredFileContent(transaction, id, { includeDeleted: false });
  if (!stored || stored.sizeBytes === null) return null;
  // Private app-consumer admission is synchronous/fail-fast while the SQL lease is held.
  // Its caller retains ownership through SQL settlement and response/body consumption.
  reserve?.(stored.sizeBytes);
  const content = Buffer.allocUnsafe(stored.sizeBytes);
  const count = Math.ceil(stored.sizeBytes / NATIVE_BLOB_LIMITS.chunkBytes);
  for (let index = 0; index < count; index++) {
    const chunk = await stored.readChunk(index);
    const offset = index * NATIVE_BLOB_LIMITS.chunkBytes;
    if (
      !Buffer.isBuffer(chunk) ||
      chunk.byteLength !== Math.min(NATIVE_BLOB_LIMITS.chunkBytes, stored.sizeBytes - offset)
    ) {
      throw new Error("Stored file content is incomplete or invalid");
    }
    chunk.copy(content, offset);
  }
  if ((await stored.readChunk(count)) !== null) {
    throw new Error("Stored file content is incomplete or invalid");
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
  const result = await transaction.query(
    "UPDATE omniroute.files SET deleted_at = $1, content = NULL WHERE id = $2",
    [Math.floor(Date.now() / 1000), id]
  );
  if ((result.rowCount ?? 0) === 0) return false;
  await transaction.query("DELETE FROM omniroute.files_content_state WHERE file_id = $1", [id]);
  return true;
}
export async function deleteFileOwnedBy(
  transaction: PostgresTransaction,
  id: string,
  apiKeyId: string
): Promise<boolean> {
  if (typeof apiKeyId !== "string" || apiKeyId.trim() === "")
    throw new Error("deleteFileOwnedBy: apiKeyId is required");
  await lockServices(transaction);
  const result = await transaction.query(
    "UPDATE omniroute.files SET deleted_at = $1, content = NULL WHERE id = $2 AND api_key_id = $3",
    [Math.floor(Date.now() / 1000), id, apiKeyId]
  );
  if ((result.rowCount ?? 0) === 0) return false;
  await transaction.query("DELETE FROM omniroute.files_content_state WHERE file_id = $1", [id]);
  return true;
}
export async function pruneExpiredFiles(
  transaction: PostgresTransaction,
  now: number
): Promise<number> {
  await lockServices(transaction);
  const result = await first<{ count: string }>(
    transaction,
    `WITH expired AS (
      UPDATE omniroute.files SET deleted_at = $1, content = NULL
      WHERE expires_at IS NOT NULL AND expires_at < $1 AND deleted_at IS NULL RETURNING id
    ), cleared AS (
      DELETE FROM omniroute.files_content_state WHERE file_id IN (SELECT id FROM expired)
    ) SELECT COUNT(*)::text AS count FROM expired`,
    [now]
  );
  return safeInteger(result?.count);
}
