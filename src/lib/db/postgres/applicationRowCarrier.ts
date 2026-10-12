import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import type { SqliteAdapter } from "../adapters/types";

/** Offline conversation history only. The inline snapshot budget remains unchanged. */
export const APPLICATION_ROW_LIMITS = {
  rows: 10_000_000,
  chunkRows: 5_000,
  chunkBytes: 8 * 1024 * 1024,
  carrierBytes: 10 * 1024 ** 3,
  chunks: 10_000,
} as const;
export const TURN_NODE_COLUMNS = [
  "id",
  "conversation_id",
  "parent_id",
  "role",
  "content_hash",
  "last_correlation_id",
  "first_seen_at",
  "last_seen_at",
] as const;
export const applicationRowCarrierSchema = z
  .object({
    format: z.literal("omniroute-turn-history-carrier-v1"),
    id: z.string().uuid(),
    rowCount: z.number().int().min(0).max(APPLICATION_ROW_LIMITS.rows),
    chunks: z
      .array(
        z
          .object({
            rows: z.number().int().min(1).max(APPLICATION_ROW_LIMITS.chunkRows),
            bytes: z.number().int().min(1).max(APPLICATION_ROW_LIMITS.chunkBytes),
            sha256: z.string().regex(/^[0-9a-f]{64}$/),
          })
          .strict()
      )
      .max(APPLICATION_ROW_LIMITS.chunks),
  })
  .strict();
export type ApplicationRowCarrier = z.infer<typeof applicationRowCarrierSchema>;
export interface ApplicationRowSink {
  writeChunk(id: string, index: number, bytes: Uint8Array): void;
}
export interface ApplicationRowSource {
  readChunk(id: string, index: number): Promise<Uint8Array | null>;
}
const signedInteger = z
  .string()
  .regex(/^-?(?:0|[1-9][0-9]*)$/)
  .refine(
    (value) => BigInt(value) >= -9223372036854775808n && BigInt(value) <= 9223372036854775807n
  );
const rowSchema = z
  .object({
    rowid: signedInteger,
    values: z.tuple([
      z.string(),
      z.string(),
      z.string().nullable(),
      z.string(),
      z.string(),
      z.string().nullable(),
      z.string(),
      z.string(),
    ]),
  })
  .strict();
export type ApplicationHistoryRow = z.infer<typeof rowSchema>;
export class ApplicationRowCarrierError extends Error {
  constructor(readonly kind: "limits" | "verification" | "schema") {
    super(`Offline turn history qualification failed (${kind})`);
    this.name = "ApplicationRowCarrierError";
  }
}
const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
export function checkedApplicationRowCarrier(input: unknown): ApplicationRowCarrier {
  const parsed = applicationRowCarrierSchema.safeParse(input);
  if (!parsed.success) throw new ApplicationRowCarrierError("schema");
  const value = parsed.data;
  if (
    value.chunks.reduce((sum, chunk) => sum + chunk.rows, 0) !== value.rowCount ||
    value.chunks.reduce((sum, chunk) => sum + chunk.bytes, 0) > APPLICATION_ROW_LIMITS.carrierBytes
  )
    throw new ApplicationRowCarrierError("limits");
  return value;
}
function checkedRows(input: unknown): ApplicationHistoryRow[] {
  const parsed = z.array(rowSchema).max(APPLICATION_ROW_LIMITS.chunkRows).safeParse(input);
  if (!parsed.success) throw new ApplicationRowCarrierError("schema");
  return parsed.data;
}
/** Keyset capture runs within the caller's consistent read-only SQLite transaction. */
export function captureApplicationHistory(
  source: SqliteAdapter,
  sink: ApplicationRowSink
): ApplicationRowCarrier {
  if (!source.inTransaction || source.pragma("query_only", { simple: true }) !== 1)
    throw new ApplicationRowCarrierError("verification");
  const id = randomUUID(),
    chunks: ApplicationRowCarrier["chunks"] = [];
  let last: string | undefined,
    rowCount = 0,
    totalBytes = 0;
  const columns = TURN_NODE_COLUMNS.map((name) => `"${name}"`).join(",");
  const lengths = TURN_NODE_COLUMNS.map(
    (name) => `COALESCE(length(CAST("${name}" AS BLOB)),0)`
  ).join("+");
  const bound = source
    .prepare(
      `SELECT CAST(count(*) AS TEXT) AS count,
    max(${lengths}) AS maximum FROM conversation_turn_nodes`
    )
    .get() as { count: string; maximum: number | null };
  const rowBound = 6 * (bound.maximum ?? 0) + 512;
  if (
    BigInt(bound.count) > BigInt(APPLICATION_ROW_LIMITS.rows) ||
    rowBound > APPLICATION_ROW_LIMITS.chunkBytes
  )
    throw new ApplicationRowCarrierError("limits");
  const batchRows = Math.min(
    APPLICATION_ROW_LIMITS.chunkRows,
    Math.floor(APPLICATION_ROW_LIMITS.chunkBytes / rowBound)
  );
  for (;;) {
    const batch = source
      .prepare(
        `SELECT CAST(rowid AS TEXT) AS rowid,${columns}
      FROM conversation_turn_nodes ${last === undefined ? "" : "WHERE rowid > ?"}
      ORDER BY conversation_turn_nodes.rowid LIMIT ?`
      )
      .all(...(last === undefined ? [] : [last]), batchRows) as Record<string, unknown>[];
    if (!batch.length) break;
    const rows = checkedRows(
      batch.map((row) => ({
        rowid: row.rowid,
        values: TURN_NODE_COLUMNS.map((name) => row[name]),
      }))
    );
    for (const row of rows) {
      if (last !== undefined && BigInt(row.rowid) <= BigInt(last))
        throw new ApplicationRowCarrierError("verification");
      last = row.rowid;
    }
    const bytes = Buffer.from(JSON.stringify(rows));
    rowCount += rows.length;
    totalBytes += bytes.byteLength;
    if (
      rowCount > APPLICATION_ROW_LIMITS.rows ||
      bytes.byteLength > APPLICATION_ROW_LIMITS.chunkBytes ||
      totalBytes > APPLICATION_ROW_LIMITS.carrierBytes ||
      chunks.length >= APPLICATION_ROW_LIMITS.chunks
    )
      throw new ApplicationRowCarrierError("limits");
    if (sink.writeChunk(id, chunks.length, bytes) !== undefined)
      throw new ApplicationRowCarrierError("verification");
    chunks.push({ rows: rows.length, bytes: bytes.byteLength, sha256: digest(bytes) });
  }
  if (BigInt(rowCount) !== BigInt(bound.count))
    throw new ApplicationRowCarrierError("verification");
  return checkedApplicationRowCarrier({
    format: "omniroute-turn-history-carrier-v1",
    id,
    rowCount,
    chunks,
  });
}
/** Bytes are revalidated on every read; a callback cannot attest database contents. */
export async function* readApplicationHistory(
  carrier: ApplicationRowCarrier,
  source: ApplicationRowSource
): AsyncGenerator<ApplicationHistoryRow[]> {
  const checked = checkedApplicationRowCarrier(carrier);
  let last: string | undefined;
  for (let index = 0; index < checked.chunks.length; index++) {
    const expected = checked.chunks[index],
      bytes = await source.readChunk(checked.id, index);
    if (
      !(bytes instanceof Uint8Array) ||
      bytes.byteLength !== expected.bytes ||
      digest(bytes) !== expected.sha256
    )
      throw new ApplicationRowCarrierError("verification");
    let value: unknown;
    try {
      value = JSON.parse(Buffer.from(bytes).toString("utf8"));
    } catch {
      throw new ApplicationRowCarrierError("verification");
    }
    const rows = checkedRows(value);
    if (rows.length !== expected.rows) throw new ApplicationRowCarrierError("verification");
    for (const row of rows) {
      if (last !== undefined && BigInt(row.rowid) <= BigInt(last))
        throw new ApplicationRowCarrierError("verification");
      last = row.rowid;
    }
    yield rows;
  }
}
