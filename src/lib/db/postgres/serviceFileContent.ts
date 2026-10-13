import { createHash } from "node:crypto";
import { setImmediate as yieldChunk } from "node:timers/promises";
import type { PostgresTransaction } from "./database";
import {
  checkedNativeBlobReference,
  NATIVE_BLOB_LIMITS,
  NativeBlobCarrierError,
  type NativeBlobReference,
  type NativeBlobSource,
} from "./nativeBlobCarrier";
import { safeInteger } from "./serviceQuery";

const CHECKSUM = /^[0-9a-f]{64}$/;
const EMPTY_CHECKSUM = createHash("sha256").digest("hex");
const hash = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const invalid = () => new Error("Stored file content is incomplete or invalid");

export interface StoredFileContent {
  /** NULL is distinct from an empty Buffer. Metadata files.bytes is independent. */
  sizeBytes: number | null;
  /** Legacy inline rows have no stored whole checksum. */
  checksum: string | null;
  /** Sequential indexes only; final mapped chunk verifies whole SHA before ACK. */
  readChunk(index: number): Promise<Buffer | null>;
}

interface Header {
  inline_length: number | null;
  stored_file_id: string | null;
  size_bytes: string | null;
  chunk_count: number | null;
  checksum: string | null;
}
interface ChunkStats {
  count: string;
  total: string;
  first: number | null;
  last: number | null;
  complete: boolean | null;
}
function validStats(stats: ChunkStats | undefined, size: number, count: number): boolean {
  return (
    !!stats &&
    safeInteger(stats.count) === count &&
    safeInteger(stats.total) === size &&
    (count === 0
      ? stats.first === null && stats.last === null && stats.complete === null
      : stats.first === 0 && stats.last === count - 1 && stats.complete === true)
  );
}

function decodeChunk(value: unknown, length: number): Buffer {
  const encodedLength = Math.ceil(length / 3) * 4;
  if (typeof value !== "string" || value.length !== encodedLength) throw invalid();
  const bytes = Buffer.from(value, "base64");
  if (bytes.byteLength !== length || bytes.toString("base64") !== value) throw invalid();
  return bytes;
}

/** Bound to the supplied client/transaction. Maintenance includes deleted rows. */
async function openStoredFileContentReader(
  transaction: PostgresTransaction,
  id: string,
  options: { includeDeleted?: boolean } = {}
): Promise<EncodedFileReader | null> {
  const result = await transaction.query<Header>(
    `SELECT octet_length(f.content) AS inline_length, s.file_id AS stored_file_id,
      s.size_bytes, s.chunk_count, s.checksum
    FROM omniroute.files f LEFT JOIN omniroute.files_content_state s ON s.file_id = f.id
    WHERE f.id = $1 ${options.includeDeleted === false ? "AND f.deleted_at IS NULL" : ""}
    FOR SHARE OF f`,
    [id]
  );
  if (result.rows.length === 0) return null;
  if (result.rows.length !== 1) throw invalid();
  const row = result.rows[0];
  const mapped = row.stored_file_id !== null;
  if (mapped && (row.stored_file_id !== id || row.inline_length !== null)) throw invalid();
  const rawSize = mapped ? row.size_bytes : row.inline_length;
  const sizeBytes = rawSize === null ? null : safeInteger(rawSize);
  if (sizeBytes !== null && (sizeBytes < 0 || sizeBytes > NATIVE_BLOB_LIMITS.fileBytes))
    throw invalid();
  const count = sizeBytes === null ? 0 : Math.ceil(sizeBytes / NATIVE_BLOB_LIMITS.chunkBytes);
  const checksum = mapped ? row.checksum : null;
  if (mapped) {
    if (
      sizeBytes === null ||
      row.chunk_count !== count ||
      typeof checksum !== "string" ||
      !CHECKSUM.test(checksum)
    )
      throw invalid();
    const stats = (
      await transaction.query<ChunkStats>(
        `SELECT COUNT(*)::text AS count, COALESCE(SUM(octet_length(content)),0)::text AS total,
        MIN(chunk_index) AS first, MAX(chunk_index) AS last,
        BOOL_AND(octet_length(content) = LEAST($2::bigint, $3::bigint - chunk_index * $2::bigint)) AS complete
      FROM omniroute.files_content_chunks WHERE file_id = $1`,
        [id, NATIVE_BLOB_LIMITS.chunkBytes, sizeBytes]
      )
    ).rows;
    if (stats.length !== 1 || !validStats(stats[0], sizeBytes, count)) throw invalid();
    if (sizeBytes === 0 && checksum !== EMPTY_CHECKSUM) throw invalid();
  } else {
    const orphans = await transaction.query<{ present: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM omniroute.files_content_chunks WHERE file_id = $1) AS present",
      [id]
    );
    if (orphans.rows.length !== 1 || orphans.rows[0].present !== false) throw invalid();
  }
  let next = 0;
  return {
    sizeBytes,
    checksum,
    async readEncodedChunk(index) {
      if (!Number.isSafeInteger(index) || index < 0 || index !== next || index > count)
        throw invalid();
      if (index === count) return null;
      const length = Math.min(
        NATIVE_BLOB_LIMITS.chunkBytes,
        sizeBytes! - index * NATIVE_BLOB_LIMITS.chunkBytes
      );
      const chunks = mapped
        ? await transaction.query<{ chunk_index: number; content: string; checksum: string }>(
            `SELECT chunk_index, replace(encode(content, 'base64'), E'\\n', '') AS content, checksum
            FROM omniroute.files_content_chunks WHERE file_id = $1 AND chunk_index = $2`,
            [id, index]
          )
        : await transaction.query<{ chunk_index: number; content: string; checksum: null }>(
            `SELECT $4::integer AS chunk_index, replace(encode(substr(content, $2, $3), 'base64'), E'\\n', '') AS content, NULL AS checksum
            FROM omniroute.files WHERE id = $1`,
            [id, index * NATIVE_BLOB_LIMITS.chunkBytes + 1, length, index]
          );
      const chunk = chunks.rows[0];
      if (
        chunks.rows.length !== 1 ||
        typeof chunk?.chunk_index !== "number" ||
        !Number.isSafeInteger(chunk.chunk_index) ||
        chunk.chunk_index !== index
      )
        throw invalid();
      next++;
      return { content: chunk.content, checksum: chunk.checksum, length };
    },
  };
}

interface EncodedChunk {
  content: unknown;
  checksum: unknown;
  length: number;
}
interface EncodedFileReader {
  sizeBytes: number | null;
  checksum: string | null;
  readEncodedChunk(index: number): Promise<EncodedChunk | null>;
}
export interface StoredFileSnapshot {
  readonly sizeBytes: number;
  readonly checksum: string | null;
  readonly chunks: readonly EncodedChunk[];
}

function checkedChunk(chunk: EncodedChunk, mapped: boolean): Buffer {
  const bytes = decodeChunk(chunk.content, chunk.length);
  if (
    mapped &&
    (typeof chunk.checksum !== "string" ||
      !CHECKSUM.test(chunk.checksum) ||
      hash(bytes) !== chunk.checksum)
  )
    throw invalid();
  return bytes;
}

/** Explicit offline streaming contract: the supplied transaction remains owned by the caller. */
export async function openStoredFileContent(
  transaction: PostgresTransaction,
  id: string,
  options: { includeDeleted?: boolean } = {}
): Promise<StoredFileContent | null> {
  const reader = await openStoredFileContentReader(transaction, id, options);
  if (!reader) return null;
  const whole = reader.checksum !== null ? createHash("sha256") : null;
  let count = 0;
  return {
    sizeBytes: reader.sizeBytes,
    checksum: reader.checksum,
    async readChunk(index) {
      const chunk = await reader.readEncodedChunk(index);
      if (!chunk) return null;
      const bytes = checkedChunk(chunk, whole !== null);
      whole?.update(bytes);
      count += bytes.length;
      if (whole && count === reader.sizeBytes && whole.digest("hex") !== reader.checksum)
        throw invalid();
      return bytes;
    },
  };
}

/** Capture one complete version under the parent lock; no payload decode/hash/copy here. */
export async function captureStoredFileContent(
  transaction: PostgresTransaction,
  id: string,
  reserve?: (sizeBytes: number) => void
): Promise<StoredFileSnapshot | null> {
  const reader = await openStoredFileContentReader(transaction, id, { includeDeleted: false });
  if (!reader || reader.sizeBytes === null) return null;
  reserve?.(reader.sizeBytes);
  const chunks: EncodedChunk[] = [];
  const count = Math.ceil(reader.sizeBytes / NATIVE_BLOB_LIMITS.chunkBytes);
  for (let index = 0; index < count; index++) {
    const chunk = await reader.readEncodedChunk(index);
    if (!chunk) throw invalid();
    chunks.push(chunk);
  }
  return { sizeBytes: reader.sizeBytes, checksum: reader.checksum, chunks };
}

/** Runs after SQL settlement. Never expose a partially verified result. */
export async function materializeStoredFileContent(
  snapshot: StoredFileSnapshot | null
): Promise<Buffer | null> {
  if (!snapshot) return null;
  const output = Buffer.allocUnsafe(snapshot.sizeBytes);
  const whole = snapshot.checksum !== null ? createHash("sha256") : null;
  let offset = 0;
  for (const chunk of snapshot.chunks) {
    const bytes = checkedChunk(chunk, whole !== null);
    if (offset + bytes.length > output.length) throw invalid();
    whole?.update(bytes);
    bytes.copy(output, offset);
    offset += bytes.length;
    await yieldChunk();
  }
  if (offset !== output.length || (whole && whole.digest("hex") !== snapshot.checksum))
    throw invalid();
  return output;
}

interface PreparedChunk {
  readonly bytes: Buffer;
  readonly checksum: string;
}
export interface PreparedFileContent {
  readonly sizeBytes: number | null;
  readonly checksum: string | null;
}
const preparedChunks = new WeakMap<PreparedFileContent, readonly PreparedChunk[]>();

/** Capture caller bytes before SQL admission; bounded macrotasks let other SQL owners progress. */
export async function prepareFileContent(content: Buffer | null): Promise<PreparedFileContent> {
  const sizeBytes = content === null ? null : content.byteLength;
  if (sizeBytes !== null && sizeBytes > NATIVE_BLOB_LIMITS.fileBytes)
    throw new Error("File content exceeds the supported size limit");
  const chunks: PreparedChunk[] = [],
    whole = createHash("sha256");
  for (
    let offset = 0;
    content && offset < content.length;
    offset += NATIVE_BLOB_LIMITS.chunkBytes
  ) {
    const bytes = Buffer.from(content.subarray(offset, offset + NATIVE_BLOB_LIMITS.chunkBytes));
    chunks.push({ bytes, checksum: hash(bytes) });
    whole.update(bytes);
    await yieldChunk();
  }
  const prepared = Object.freeze({
    sizeBytes,
    checksum: sizeBytes === null ? null : whole.digest("hex"),
  });
  preparedChunks.set(prepared, chunks);
  return prepared;
}

/** Only pre-captured content is accepted; this serving primitive performs database work only. */
export async function replacePreparedFileContent(
  transaction: PostgresTransaction,
  id: string,
  prepared: PreparedFileContent
): Promise<void> {
  const chunks = preparedChunks.get(prepared);
  if (!chunks) throw new Error("File content was not prepared before SQL admission");
  const parent = await transaction.query("SELECT id FROM omniroute.files WHERE id=$1 FOR UPDATE", [
    id,
  ]);
  if (parent.rows.length !== 1) throw invalid();
  await transaction.query("DELETE FROM omniroute.files_content_state WHERE file_id=$1", [id]);
  if (prepared.sizeBytes !== null) {
    for (let index = 0; index < chunks.length; index++) {
      const chunk = chunks[index];
      const inserted = await transaction.query(
        "INSERT INTO omniroute.files_content_chunks(file_id,chunk_index,content,checksum) VALUES($1,$2,$3,$4)",
        [id, index, chunk.bytes, chunk.checksum]
      );
      if (inserted.rowCount !== 1) throw invalid();
    }
    const marker = await transaction.query(
      "INSERT INTO omniroute.files_content_state(file_id,size_bytes,chunk_count,checksum) VALUES($1,$2,$3,$4)",
      [id, prepared.sizeBytes, chunks.length, prepared.checksum]
    );
    if (marker.rowCount !== 1) throw invalid();
  }
  const updated = await transaction.query("UPDATE omniroute.files SET content=NULL WHERE id=$1", [
    id,
  ]);
  if (updated.rowCount !== 1) throw invalid();
}

/** Offline import/restore streaming; serving callers must use prepared content instead. */
export async function replaceStoredFileContent(
  transaction: PostgresTransaction,
  id: string,
  content: Buffer | null | NativeBlobReference,
  source?: NativeBlobSource
): Promise<void> {
  const reference =
    content !== null && !Buffer.isBuffer(content) ? checkedNativeBlobReference(content) : null;
  const size = reference
    ? reference.sizeBytes
    : content === null
      ? null
      : (content as Buffer).byteLength;
  if (
    size !== null &&
    (!Number.isSafeInteger(size) || size < 0 || size > NATIVE_BLOB_LIMITS.fileBytes)
  )
    throw new Error("File content exceeds the supported size limit");
  if (reference && !source) throw invalid();
  const parent = await transaction.query(
    "SELECT id FROM omniroute.files WHERE id = $1 FOR UPDATE",
    [id]
  );
  if (parent.rows.length !== 1) throw invalid();
  await transaction.query("DELETE FROM omniroute.files_content_state WHERE file_id = $1", [id]);
  if (size !== null) {
    const whole = createHash("sha256");
    const count = Math.ceil(size / NATIVE_BLOB_LIMITS.chunkBytes);
    for (let index = 0; index < count; index++) {
      const length = Math.min(
        NATIVE_BLOB_LIMITS.chunkBytes,
        size - index * NATIVE_BLOB_LIMITS.chunkBytes
      );
      const incoming = reference
        ? await source!.readChunk(reference.id, index)
        : (content as Buffer).subarray(
            index * NATIVE_BLOB_LIMITS.chunkBytes,
            index * NATIVE_BLOB_LIMITS.chunkBytes + length
          );
      if (!(incoming instanceof Uint8Array) || incoming.byteLength !== length) {
        throw reference ? new NativeBlobCarrierError("verification") : invalid();
      }
      // One bounded private copy binds the hash and binary SQL parameter to the same bytes.
      const bytes = Buffer.from(incoming);
      const checksum = hash(bytes);
      if (reference && checksum !== reference.chunks[index].checksum) {
        throw new NativeBlobCarrierError("verification");
      }
      whole.update(bytes);
      const inserted = await transaction.query(
        `INSERT INTO omniroute.files_content_chunks(file_id, chunk_index, content, checksum)
        VALUES ($1, $2, $3, $4)`,
        [id, index, bytes, checksum]
      );
      if (inserted.rowCount !== 1) throw invalid();
    }
    const checksum = whole.digest("hex");
    if (reference && checksum !== reference.checksum) {
      throw new NativeBlobCarrierError("verification");
    }
    const marker = await transaction.query(
      `INSERT INTO omniroute.files_content_state(file_id, size_bytes, chunk_count, checksum)
      VALUES ($1, $2, $3, $4)`,
      [id, size, count, checksum]
    );
    if (marker.rowCount !== 1) throw invalid();
  }
  const updated = await transaction.query(
    "UPDATE omniroute.files SET content = NULL WHERE id = $1",
    [id]
  );
  if (updated.rowCount !== 1) throw invalid();
}

/** Metadata-only preflight. Physical rows are not silently omitted from portable backups. */
export async function getStoredFileContentBounds(
  transaction: PostgresTransaction
): Promise<{ maximum: number; total: string }> {
  const result = await transaction.query<{
    maximum: number;
    total: string;
    invalid: boolean;
  }>(`WITH headers AS (
    SELECT f.id, f.content IS NOT NULL AS inline_present, s.file_id, s.size_bytes, s.chunk_count, s.checksum,
      CASE WHEN s.file_id IS NULL THEN octet_length(f.content) ELSE s.size_bytes END AS size
    FROM omniroute.files f LEFT JOIN omniroute.files_content_state s ON s.file_id = f.id
  ) SELECT COALESCE(MAX(size),0)::integer AS maximum, COALESCE(SUM(size),0)::text AS total,
    EXISTS (SELECT 1 FROM headers WHERE size < 0 OR size > 536870912 OR
      (file_id IS NOT NULL AND (inline_present OR size_bytes IS NULL OR chunk_count IS NULL OR
        chunk_count <> (size_bytes + 8388607) / 8388608 OR checksum IS NULL OR checksum !~ '^[0-9a-f]{64}$' OR
        (size_bytes = 0 AND checksum <> '${EMPTY_CHECKSUM}')))) OR
    EXISTS (SELECT 1 FROM omniroute.files_content_state s LEFT JOIN omniroute.files f ON f.id = s.file_id WHERE f.id IS NULL) OR
    EXISTS (SELECT 1 FROM omniroute.files_content_chunks c LEFT JOIN omniroute.files_content_state s ON s.file_id = c.file_id WHERE s.file_id IS NULL) OR
    EXISTS (SELECT 1 FROM omniroute.files_content_state s CROSS JOIN LATERAL (
      SELECT COUNT(*) AS count, COALESCE(SUM(octet_length(content)),0) AS total,
        MIN(chunk_index) AS first, MAX(chunk_index) AS last,
        BOOL_AND(octet_length(content) = LEAST(8388608, s.size_bytes - chunk_index * 8388608)) AS complete
      FROM omniroute.files_content_chunks WHERE file_id = s.file_id
    ) c WHERE c.count <> s.chunk_count OR c.total <> s.size_bytes OR
      (s.chunk_count = 0 AND (c.first IS NOT NULL OR c.last IS NOT NULL OR c.complete IS NOT NULL)) OR
      (s.chunk_count > 0 AND (c.first <> 0 OR c.last <> s.chunk_count - 1 OR c.complete IS NOT TRUE))) AS invalid
  FROM headers`);
  const row = result.rows[0];
  if (result.rows.length !== 1 || row.invalid !== false) throw invalid();
  const maximum = safeInteger(row.maximum);
  if (
    maximum < 0 ||
    maximum > NATIVE_BLOB_LIMITS.fileBytes ||
    typeof row.total !== "string" ||
    !/^\d+$/.test(row.total)
  )
    throw invalid();
  return { maximum, total: row.total };
}
