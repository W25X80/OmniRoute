import { v4 as uuidv4 } from "uuid";
import type {
  BatchRecord,
  BatchItemCheckpoint,
  DeleteCompletedBatchesScope,
  BatchTransitionResult,
  BatchCompletion,
  BatchOutputFile,
} from "../batches";
import {
  parseBatchRow,
  parseBatchItemCheckpoint,
  INSTANCE_SWEEP_CHUNK,
  MAX_CHUNKS_PER_REQUEST,
  getExpiredBatchFileIds,
} from "../batches/persistenceSemantics";
import { objToSnake } from "../caseMapping";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { affected, first, rows, safeInteger, lockServices, placeholders } from "./serviceQuery";
import { createFile, getFile, deleteFile, deleteFileOwnedBy } from "./serviceFiles";

const BATCH_COLUMNS = new Set([
  "id",
  "endpoint",
  "completion_window",
  "status",
  "input_file_id",
  "output_file_id",
  "error_file_id",
  "created_at",
  "in_progress_at",
  "expires_at",
  "finalizing_at",
  "completed_at",
  "failed_at",
  "expired_at",
  "cancelling_at",
  "cancelled_at",
  "request_counts_total",
  "request_counts_completed",
  "request_counts_failed",
  "metadata",
  "api_key_id",
  "errors",
  "model",
  "usage",
  "output_expires_after_seconds",
  "output_expires_after_anchor",
]);
function storedBatch(value: Record<string, unknown>): Record<string, unknown> {
  const snake = objToSnake(value) as Record<string, unknown>;
  for (const key of Object.keys(snake)) {
    if (!BATCH_COLUMNS.has(key)) throw new Error("Unsupported batch field");
    if (
      ["metadata", "errors", "usage"].includes(key) &&
      snake[key] &&
      typeof snake[key] !== "string"
    )
      snake[key] = JSON.stringify(snake[key]);
  }
  return snake;
}
function mapBatch(row: Record<string, unknown>): BatchRecord {
  const { insertion_order: _sequence, ...value } = row;
  return parseBatchRow(value);
}
export async function createBatch(
  transaction: PostgresTransaction,
  batch: Parameters<typeof import("../batches").createBatch>[0]
): Promise<BatchRecord> {
  await lockServices(transaction);
  // Recheck after acquiring the same authority used by expiry and explicit deletion.
  if (!(await getFile(transaction, batch.inputFileId))) throw new Error("Input file not found");
  const record: BatchRecord = {
    ...batch,
    id: "batch_" + uuidv4().replaceAll("-", "").substring(0, 24),
    createdAt: Math.floor(Date.now() / 1000),
    status: batch.status || "validating",
    requestCountsTotal: 0,
    requestCountsCompleted: 0,
    requestCountsFailed: 0,
    errors: batch.errors || null,
    model: batch.model || null,
    usage: batch.usage || null,
    outputExpiresAfterSeconds: batch.outputExpiresAfterSeconds || null,
    outputExpiresAfterAnchor: batch.outputExpiresAfterAnchor || null,
  };
  const data = storedBatch(record as unknown as Record<string, unknown>);
  const columns = Object.keys(data);
  await transaction.query(
    `INSERT INTO omniroute.batches (${columns.join(", ")}) VALUES (${placeholders(columns.length)})`,
    Object.values(data)
  );
  return record;
}
export async function getBatch(
  transaction: PostgresTransaction,
  id: string
): Promise<BatchRecord | null> {
  const row = await first(transaction, "SELECT * FROM omniroute.batches WHERE id = $1", [id]);
  return row ? mapBatch(row) : null;
}
export async function updateBatch(
  transaction: PostgresTransaction,
  id: string,
  updates: Partial<BatchRecord>
): Promise<boolean> {
  await lockServices(transaction);
  const data = storedBatch(updates as Record<string, unknown>);
  const columns = Object.keys(data);
  if (!columns.length) return false;
  return (
    (await affected(
      transaction,
      `UPDATE omniroute.batches SET ${columns.map((column, index) => `${column} = $${index + 1}`).join(", ")} WHERE id = $${columns.length + 1}`,
      [...Object.values(data), id]
    )) > 0
  );
}
/** Lock order is the existing files-batches advisory lock, then file content row locks. */
export async function transitionBatch(
  transaction: PostgresTransaction,
  id: string,
  expectedStatuses: readonly BatchRecord["status"][],
  updates: Partial<BatchRecord> & { status: BatchRecord["status"] },
  discardOutputs = false
): Promise<BatchTransitionResult> {
  await lockServices(transaction);
  const current = await getBatch(transaction, id);
  if (!current || !expectedStatuses.includes(current.status))
    return { changed: false, batch: current };
  await updateBatch(transaction, id, updates);
  if (discardOutputs) {
    for (const fileId of new Set([current.outputFileId, current.errorFileId])) {
      if (fileId && !(await isFileReferencedByOtherBatch(transaction, fileId, [])))
        await deleteFile(transaction, fileId);
    }
  }
  return { changed: true, batch: await getBatch(transaction, id) };
}

/** No provider I/O: both result files and their links commit with the terminal state. */
export async function completeBatchWithFiles(
  transaction: PostgresTransaction,
  id: string,
  completion: BatchCompletion,
  output: BatchOutputFile | null,
  error: BatchOutputFile | null
): Promise<BatchTransitionResult> {
  await lockServices(transaction);
  const current = await getBatch(transaction, id);
  if (current?.status === "cancelling") {
    return transitionBatch(transaction, id, ["cancelling"], {
      status: "cancelled",
      cancelledAt: Math.floor(Date.now() / 1000),
    });
  }
  if (!current || current.status !== "finalizing") return { changed: false, batch: current };
  const save = async (file: BatchOutputFile | null) =>
    file
      ? (
          await createFile(transaction, {
            ...file,
            bytes: file.content.byteLength,
            purpose: "batch_output",
            apiKeyId: current.apiKeyId,
          })
        ).id
      : null;
  const outputFileId = await save(output);
  const errorFileId = await save(error);
  await updateBatch(transaction, id, {
    ...completion,
    status: "completed",
    outputFileId,
    errorFileId,
  });
  return { changed: true, batch: await getBatch(transaction, id) };
}

export async function ensureBatchItemCheckpoints(
  transaction: PostgresTransaction,
  batchId: string,
  items: Array<{ lineNumber: number; customId: string | null }>
): Promise<void> {
  if (!items.length) return;
  await lockServices(transaction);
  const now = Math.floor(Date.now() / 1000);
  for (let index = 0; index < items.length; index += 100) {
    const chunk = items.slice(index, index + 100);
    const values: unknown[] = [];
    const tuples = chunk.map((item) => {
      const offset = values.length;
      values.push(batchId, item.lineNumber, item.customId, now);
      return `($${offset + 1}, $${offset + 2}, $${offset + 3}, 'pending', NULL, NULL, $${offset + 4}, $${offset + 4})`;
    });
    await transaction.query(
      `INSERT INTO omniroute.batch_item_checkpoints
      (batch_id, line_number, custom_id, status, result_json, error_json, created_at, updated_at)
      VALUES ${tuples.join(", ")} ON CONFLICT (batch_id, line_number) DO NOTHING`,
      values
    );
  }
}
export async function countBatchItemCheckpoints(
  transaction: PostgresTransaction,
  batchId: string
): Promise<number> {
  return safeInteger(
    (
      await first<{ count: string }>(
        transaction,
        "SELECT count(*) AS count FROM omniroute.batch_item_checkpoints WHERE batch_id = $1",
        [batchId]
      )
    )?.count ?? 0
  );
}
export async function listBatchItemCheckpoints(
  transaction: PostgresTransaction,
  batchId: string
): Promise<BatchItemCheckpoint[]> {
  return (
    await rows(
      transaction,
      "SELECT * FROM omniroute.batch_item_checkpoints WHERE batch_id = $1 ORDER BY line_number ASC",
      [batchId]
    )
  ).map(parseBatchItemCheckpoint);
}
export async function markBatchItemProcessing(
  transaction: PostgresTransaction,
  batchId: string,
  item: { lineNumber: number; customId: string | null }
): Promise<void> {
  await lockServices(transaction);
  const now = Math.floor(Date.now() / 1000);
  await transaction.query(
    `INSERT INTO omniroute.batch_item_checkpoints
    (batch_id, line_number, custom_id, status, result_json, error_json, created_at, updated_at)
    VALUES ($1, $2, $3, 'processing', NULL, NULL, $4, $4)
    ON CONFLICT (batch_id, line_number) DO UPDATE SET custom_id = excluded.custom_id,
    status = 'processing', result_json = NULL, error_json = NULL, updated_at = excluded.updated_at`,
    [batchId, item.lineNumber, item.customId, now]
  );
}
export async function markBatchItemResult(
  transaction: PostgresTransaction,
  batchId: string,
  item: { lineNumber: number; customId: string | null },
  result: unknown
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.batch_item_checkpoints SET custom_id = $1, status = 'completed', result_json = $2, error_json = NULL, updated_at = $3 WHERE batch_id = $4 AND line_number = $5",
    [item.customId, JSON.stringify(result), Math.floor(Date.now() / 1000), batchId, item.lineNumber]
  );
}
export async function markBatchItemError(
  transaction: PostgresTransaction,
  batchId: string,
  item: { lineNumber: number; customId: string | null },
  error: unknown
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.batch_item_checkpoints SET custom_id = $1, status = 'errored', result_json = NULL, error_json = $2, updated_at = $3 WHERE batch_id = $4 AND line_number = $5",
    [item.customId, JSON.stringify(error), Math.floor(Date.now() / 1000), batchId, item.lineNumber]
  );
}
export async function listBatches(
  transaction: PostgresTransaction,
  apiKeyId?: string,
  limit = 20,
  after?: string
): Promise<BatchRecord[]> {
  const cursor = after ? await getBatch(transaction, after) : null;
  const conditions: string[] = [];
  const values: unknown[] = [];
  if (apiKeyId) {
    values.push(apiKeyId);
    conditions.push("api_key_id = $1");
  }
  if (cursor && (!apiKeyId || cursor.apiKeyId === apiKeyId)) {
    values.push(cursor.createdAt, after);
    conditions.push(
      `(created_at < $${values.length - 1} OR (created_at = $${values.length - 1} AND id COLLATE "C" < $${values.length}))`
    );
  }
  values.push(limit);
  return (
    await rows(
      transaction,
      `SELECT * FROM omniroute.batches ${conditions.length ? `WHERE ${conditions.join(" AND ")}` : ""}
    ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT CASE WHEN $${values.length}::bigint < 0 THEN NULL ELSE $${values.length}::bigint END`,
      values
    )
  ).map(mapBatch);
}
export async function countBatches(
  transaction: PostgresTransaction,
  apiKeyId?: string
): Promise<number> {
  return safeInteger(
    (
      await first<{ count: string }>(
        transaction,
        `SELECT count(*) AS count FROM omniroute.batches ${apiKeyId ? "WHERE api_key_id = $1" : ""}`,
        apiKeyId ? [apiKeyId] : []
      )
    )?.count ?? 0
  );
}
export async function getPendingBatches(transaction: PostgresTransaction): Promise<BatchRecord[]> {
  return (
    await rows(
      transaction,
      "SELECT * FROM omniroute.batches WHERE status IN ('validating', 'in_progress', 'finalizing', 'cancelling') ORDER BY insertion_order"
    )
  ).map(mapBatch);
}
export async function getTerminalBatches(transaction: PostgresTransaction): Promise<BatchRecord[]> {
  return (
    await rows(
      transaction,
      "SELECT * FROM omniroute.batches WHERE status IN ('completed', 'failed', 'cancelled', 'expired') ORDER BY created_at ASC, insertion_order ASC"
    )
  ).map(mapBatch);
}
export async function deleteExpiredBatchFiles(
  transaction: PostgresTransaction,
  id: string,
  now = Math.floor(Date.now() / 1000)
): Promise<void> {
  await lockServices(transaction);
  const batch = await getBatch(transaction, id);
  if (!batch) return;
  for (const fileId of getExpiredBatchFileIds(batch, now)) {
    if (!(await isFileReferencedByOtherBatch(transaction, fileId, [id])))
      await deleteFile(transaction, fileId);
  }
}
export async function isFileReferencedByOtherBatch(
  transaction: PostgresTransaction,
  fileId: string,
  excludeBatchIds: string[]
): Promise<boolean> {
  return !!(await first(
    transaction,
    `SELECT 1 FROM omniroute.batches WHERE (input_file_id = $1 OR output_file_id = $1 OR error_file_id = $1) ${excludeBatchIds.length ? "AND NOT (id = ANY($2::text[]))" : ""} LIMIT 1`,
    excludeBatchIds.length ? [fileId, excludeBatchIds] : [fileId]
  ));
}
export async function deleteBatch(transaction: PostgresTransaction, id: string): Promise<boolean> {
  await lockServices(transaction);
  const batch = await getBatch(transaction, id);
  if (!batch) return false;
  for (const fileId of new Set(
    [batch.inputFileId, batch.outputFileId, batch.errorFileId].filter(
      (value): value is string => typeof value === "string" && value !== ""
    )
  )) {
    if (!(await isFileReferencedByOtherBatch(transaction, fileId, [id])))
      await deleteFile(transaction, fileId);
  }
  return (await affected(transaction, "DELETE FROM omniroute.batches WHERE id = $1", [id])) > 0;
}
function validateScope(scope: DeleteCompletedBatchesScope): {
  allTenants: boolean;
  apiKeyId?: string;
} {
  const object = scope && typeof scope === "object" ? scope : {};
  const allTenants = "allTenants" in object && object.allTenants === true;
  const apiKeyId = "apiKeyId" in object ? object.apiKeyId : undefined;
  if (!allTenants && (typeof apiKeyId !== "string" || apiKeyId.trim() === ""))
    throw new Error("deleteBatchesMatching: apiKeyId required unless allTenants");
  if (allTenants && "apiKeyId" in object)
    throw new Error("deleteBatchesMatching: apiKeyId and allTenants are mutually exclusive");
  return { allTenants, apiKeyId: typeof apiKeyId === "string" ? apiKeyId : undefined };
}
async function sweep(
  database: PostgresDatabase,
  scope: DeleteCompletedBatchesScope,
  cutoff?: number
): Promise<{ deletedBatches: number; deletedFiles: number; hasMore: boolean }> {
  const { allTenants, apiKeyId } = validateScope(scope);
  const totals = { deletedBatches: 0, deletedFiles: 0, hasMore: false };
  const where =
    cutoff === undefined
      ? "status = 'completed'"
      : "status IN ('completed', 'failed', 'cancelled', 'expired') AND COALESCE(completed_at, failed_at, cancelled_at, expired_at, created_at) < $1";
  const values: unknown[] = cutoff === undefined ? [] : [cutoff];
  const owned = allTenants ? "" : ` AND api_key_id = $${values.length + 1}`;
  if (!allTenants) values.push(apiKeyId);
  for (let chunk = 0; chunk <= MAX_CHUNKS_PER_REQUEST; chunk++) {
    const part = await database.transaction(async (transaction) => {
      await lockServices(transaction);
      const batchRows = await rows<{
        id: string;
        input_file_id: string;
        output_file_id: string | null;
        error_file_id: string | null;
      }>(
        transaction,
        `SELECT id, input_file_id, output_file_id, error_file_id FROM omniroute.batches WHERE ${where}${owned} ORDER BY insertion_order LIMIT ${INSTANCE_SWEEP_CHUNK} FOR UPDATE`,
        values
      );
      if (!batchRows.length) return null;
      if (chunk === MAX_CHUNKS_PER_REQUEST)
        return { deletedBatches: 0, deletedFiles: 0, hasMore: true };
      const ids = batchRows.map((row) => row.id);
      let deletedFiles = 0;
      const fileIds = new Set(
        batchRows
          .flatMap((row) => [row.input_file_id, row.output_file_id, row.error_file_id])
          .filter((id): id is string => !!id)
      );
      for (const fileId of fileIds) {
        if (await isFileReferencedByOtherBatch(transaction, fileId, ids)) continue;
        if (
          allTenants
            ? await deleteFile(transaction, fileId)
            : await deleteFileOwnedBy(transaction, fileId, apiKeyId!)
        )
          deletedFiles++;
      }
      const deletedBatches = await affected(
        transaction,
        "DELETE FROM omniroute.batches WHERE id = ANY($1::text[])",
        [ids]
      );
      if (!deletedBatches) throw new Error("Batch cleanup made no progress");
      return { deletedBatches, deletedFiles, hasMore: false };
    });
    if (!part) break;
    totals.deletedBatches += part.deletedBatches;
    totals.deletedFiles += part.deletedFiles;
    if (part.hasMore) {
      totals.hasMore = true;
      break;
    }
  }
  return totals;
}
export function deleteCompletedBatches(
  database: PostgresDatabase,
  scope: DeleteCompletedBatchesScope
) {
  return sweep(database, scope);
}
export function deleteTerminalBatchesOlderThan(database: PostgresDatabase, days: number) {
  return sweep(database, { allTenants: true }, Math.floor(Date.now() / 1000) - days * 86400);
}
