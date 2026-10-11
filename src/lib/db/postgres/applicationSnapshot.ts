import { createHash } from "node:crypto";
import { realpathSync, statSync } from "node:fs";
import { z } from "zod";
import type { SqliteAdapter } from "../adapters/types";
import { runtimeRequire } from "../adapters/runtimeRequire";
import { tryOpenSync } from "../adapters/driverFactory";
import { createNodeSqliteAdapterFromDatabase } from "../adapters/nodeSqliteShared";
import type { NodeSqliteDatabaseLike } from "../adapters/nodeSqliteShared";
import {
  nativeBlobReferenceSchema,
  checkedNativeBlobReference,
  captureNativeBlob,
  NATIVE_BLOB_LIMITS,
  type NativeBlobSink,
} from "./nativeBlobCarrier";
import {
  APPLICATION_IMPORT_TABLES,
  APPLICATION_IMPORT_SOURCE_METADATA,
} from "./applicationImportManifest";
import {
  qualifySqliteVecDerivedStorage,
  qualifiedSqliteVecMetadata,
  SqliteVecQualificationError,
  SQLITE_VEC_CAPTURE_DECODER,
  SQLITE_VEC_INFO_TABLE,
} from "./sqliteVecDerivedTables";

export const APPLICATION_SNAPSHOT_LIMITS = {
  rows: 250_000,
  bytes: 512 * 1024 * 1024,
  blobBytes: 8 * 1024 * 1024,
  chunkRows: 100,
} as const;
const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;
export function importIdentifier(value: string): string {
  if (!identifier.test(value)) throw new ApplicationImportError("schema");
  return `"${value}"`;
}
export type ApplicationImportFailure =
  | "schema"
  | "limits"
  | "value"
  | "target_not_empty"
  | "different_import"
  | "verification"
  | "artifact"
  | "source_not_readonly";
export class ApplicationImportError extends Error {
  constructor(
    readonly kind: ApplicationImportFailure,
    readonly table?: string,
    readonly column?: string
  ) {
    super(`Offline application storage qualification failed (${kind})`);
    this.name = "ApplicationImportError";
  }
}
const cellSchema = z.union([
  nativeBlobReferenceSchema,
  z.null(),
  z.string(),
  z.number().finite(),
  z.boolean(),
  z
    .object({ kind: z.literal("integer"), value: z.string().regex(/^-?(?:0|[1-9][0-9]*)$/) })
    .strict(),
  z
    .object({
      kind: z.literal("bytes"),
      value: z.string().regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/),
    })
    .strict(),
  z
    .object({ kind: z.literal("vector"), value: z.array(z.number().finite()).min(1).max(65_536) })
    .strict(),
]);
export type ApplicationImportCell = z.infer<typeof cellSchema>;
const tableSchema = z
  .object({
    name: z.string().regex(identifier),
    columns: z.array(z.string().regex(identifier)).max(512),
    rowOrder: z.enum(["rowid", "primary-key", "native"]),
    rows: z.array(
      z
        .object({
          rowid: z
            .string()
            .regex(/^-?(?:0|[1-9][0-9]*)$/)
            .nullable(),
          values: z.array(cellSchema),
        })
        .strict()
    ),
  })
  .strict();
const schema = z
  .object({
    format: z.literal("omniroute-application-storage-v1"),
    source: z.enum(["sqlite", "postgres"]),
    tables: z.array(tableSchema),
    metadataTables: z.array(tableSchema),
    definitions: z.array(
      z.object({ name: z.string(), type: z.string(), sql: z.string().nullable() }).strict()
    ),
    excluded: z.array(z.object({ name: z.string(), reason: z.string() }).strict()),
  })
  .strict();
export type ApplicationStorageSnapshot = z.infer<typeof schema>;
export type ApplicationSnapshotTable = z.infer<typeof tableSchema>;

export function checkedApplicationSnapshot(input: unknown): ApplicationStorageSnapshot {
  const parsed = schema.safeParse(input);
  if (!parsed.success) throw new ApplicationImportError("schema");
  let count = 0,
    carrierBytes = 0;
  const carrierIds = new Map<string, string>();
  const allowedMetadata = new Set<string>([
    ...APPLICATION_IMPORT_SOURCE_METADATA,
    "vec_memories",
    ...(parsed.data.source === "postgres"
      ? [
          "schema_migrations",
          "routing_imports",
          "application_import_receipts",
          "postgres_sequence_state",
        ]
      : []),
  ]);
  for (const table of parsed.data.metadataTables) {
    if (table.name === SQLITE_VEC_INFO_TABLE) {
      if (!qualifiedSqliteVecMetadata(parsed.data.definitions, table))
        throw new ApplicationImportError("schema", table.name);
    } else if (!allowedMetadata.has(table.name))
      throw new ApplicationImportError("schema", table.name);
  }
  const names = new Set<string>();
  for (const table of [...parsed.data.tables, ...parsed.data.metadataTables]) {
    if (names.has(table.name) || new Set(table.columns).size !== table.columns.length)
      throw new ApplicationImportError("schema", table.name);
    names.add(table.name);
    for (const row of table.rows) {
      if (row.values.length !== table.columns.length)
        throw new ApplicationImportError("schema", table.name);
      for (let at = 0; at < row.values.length; at++) {
        const cell = row.values[at];
        if (cell && typeof cell === "object" && cell.kind === "blob-reference") {
          if (table.name !== "files" || table.columns[at] !== "content")
            throw new ApplicationImportError("schema", table.name, table.columns[at]);
          const reference = checkedNativeBlobReference(cell),
            serialized = JSON.stringify(reference);
          if (carrierIds.has(reference.id) && carrierIds.get(reference.id) !== serialized)
            throw new ApplicationImportError("verification", table.name);
          if (!carrierIds.has(reference.id)) carrierBytes += reference.sizeBytes;
          carrierIds.set(reference.id, serialized);
          if (carrierBytes > NATIVE_BLOB_LIMITS.carrierBytes)
            throw new ApplicationImportError("limits", table.name);
        }
        if (cell && typeof cell === "object" && cell.kind === "bytes") {
          if (Buffer.from(cell.value, "base64").byteLength > APPLICATION_SNAPSHOT_LIMITS.blobBytes)
            throw new ApplicationImportError("limits", table.name);
        }
      }
    }
    count += table.rows.length;
  }
  if (
    count > APPLICATION_SNAPSHOT_LIMITS.rows ||
    Buffer.byteLength(JSON.stringify(parsed.data)) > APPLICATION_SNAPSHOT_LIMITS.bytes
  )
    throw new ApplicationImportError("limits");
  return parsed.data;
}
export function applicationSnapshotDigest(input: unknown): string {
  const snapshot = checkedApplicationSnapshot(input);
  return createHash("sha256").update(JSON.stringify(snapshot)).digest("hex");
}

interface SourceDefinition {
  name: string;
  type: string;
  sql: string | null;
}
interface SourceTable {
  name: string;
  type: string;
  wr: number;
  schema: string;
}
interface SourceColumn {
  name: string;
  type: string;
  pk: number;
  hidden: number;
}

function readTable(
  source: SqliteAdapter,
  table: SourceTable,
  budget: { rows: number; bytes: number; carrierBytes: number },
  sink?: NativeBlobSink
): ApplicationSnapshotTable {
  const name = importIdentifier(table.name);
  const metadata = source.pragma(`table_xinfo(${name})`) as SourceColumn[];
  if (!metadata.length || metadata.some((column) => column.hidden !== 0))
    throw new ApplicationImportError("schema", table.name);
  const columns = metadata.map((column) => column.name);
  for (const column of columns) importIdentifier(column);
  const pseudo = ["rowid", "_rowid_", "oid"].find((column) => !columns.includes(column));
  const primary = metadata
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
  const rowid = table.wr === 0 && pseudo ? pseudo : null;
  const order = rowid ? importIdentifier(rowid) : primary.map(importIdentifier).join(",");
  if (!order) throw new ApplicationImportError("schema", table.name);
  const carried = (column: string) => !!sink && table.name === "files" && column === "content";
  if (sink && table.name === "files" && !rowid) throw new ApplicationImportError("schema", "files");
  const projection = columns.flatMap((column, index) => {
    const quoted = importIdentifier(column);
    return [
      carried(column)
        ? `CASE WHEN typeof(${quoted})='blob' THEN length(${quoted}) ELSE ${quoted} END AS ${quoted}`
        : `CASE WHEN typeof(${quoted}) = 'integer' THEN CAST(${quoted} AS TEXT) ELSE ${quoted} END AS ${quoted}`,
      `typeof(${quoted}) AS "_source_type_${index}"`,
    ];
  });
  const lengths = columns.map((column) => {
    const name = importIdentifier(column);
    return carried(column)
      ? `CASE WHEN typeof(${name})='blob' THEN 16384 ELSE 6*COALESCE(length(CAST(${name} AS BLOB)),0) END`
      : `CASE WHEN typeof(${name})='blob' THEN 4*((length(${name})+2)/3) ELSE 6*COALESCE(length(CAST(${name} AS BLOB)),0) END`;
  });
  const blobs = columns.map((column) =>
    carried(column)
      ? `0`
      : `CASE WHEN typeof(${importIdentifier(column)})='blob' THEN length(${importIdentifier(column)}) ELSE 0 END`
  );
  const maximum = blobs.length > 1 ? `max(${blobs.join(",")})` : blobs[0];
  const estimate = source
    .prepare(
      `SELECT CAST(COUNT(*) AS TEXT) AS count,CAST(COALESCE(SUM(${lengths.join("+")}),0) AS TEXT) AS bytes,MAX(${maximum}) AS blob FROM ${name}`
    )
    .get() as { count: string; bytes: string; blob: number | null };
  if (
    BigInt(estimate.count) > BigInt(APPLICATION_SNAPSHOT_LIMITS.rows - budget.rows) ||
    BigInt(estimate.bytes) > BigInt(APPLICATION_SNAPSHOT_LIMITS.bytes - budget.bytes) ||
    (estimate.blob ?? 0) > APPLICATION_SNAPSHOT_LIMITS.blobBytes
  )
    throw new ApplicationImportError("limits", table.name);
  if (sink && table.name === "files" && columns.includes("content")) {
    const bound = source
      .prepare(
        `SELECT MAX(length(content)) AS maximum,COALESCE(SUM(length(content)),0) AS total FROM ${name} WHERE typeof(content)='blob'`
      )
      .get() as { maximum: number | null; total: number };
    if (
      (bound.maximum ?? 0) > NATIVE_BLOB_LIMITS.fileBytes ||
      !Number.isSafeInteger(bound.total) ||
      bound.total > NATIVE_BLOB_LIMITS.carrierBytes - budget.carrierBytes
    )
      throw new ApplicationImportError("limits", "files", "content");
    budget.carrierBytes += bound.total;
  }
  const raw: Record<string, unknown>[] = [];
  for (let start = 0; ; start += APPLICATION_SNAPSHOT_LIMITS.chunkRows) {
    const batch = source
      .prepare(
        `SELECT ${rowid ? `CAST(${importIdentifier(rowid)} AS TEXT)` : "NULL"} AS "_source_rowid",${projection.join(",")} FROM ${name} ORDER BY ${order} LIMIT ? OFFSET ?`
      )
      .all(APPLICATION_SNAPSHOT_LIMITS.chunkRows, start) as Record<string, unknown>[];
    for (const row of batch) {
      budget.bytes +=
        64 +
        columns.reduce((size, column) => {
          const value = row[column];
          return (
            size +
            (value instanceof Uint8Array
              ? 4 * Math.ceil(value.byteLength / 3) + 40
              : typeof value === "string"
                ? Buffer.byteLength(JSON.stringify(value)) + 40
                : 64)
          );
        }, 0);
      if (budget.bytes > APPLICATION_SNAPSHOT_LIMITS.bytes)
        throw new ApplicationImportError("limits", table.name);
      raw.push(row);
    }
    if (batch.length < APPLICATION_SNAPSHOT_LIMITS.chunkRows) break;
  }
  budget.rows += raw.length;
  if (budget.rows > APPLICATION_SNAPSHOT_LIMITS.rows) throw new ApplicationImportError("limits");
  return {
    name: table.name,
    columns,
    rowOrder: rowid ? "rowid" : "primary-key",
    rows: raw.map((row) => ({
      rowid: row._source_rowid as string | null,
      values: columns.map((column, index) => {
        const value = row[column],
          type = row[`_source_type_${index}`];
        if (type === "null") return null;
        if (type === "integer" && typeof value === "string")
          return { kind: "integer" as const, value };
        if (type === "real" && typeof value === "number" && Number.isFinite(value)) return value;
        if (type === "text" && typeof value === "string") return value;
        if (type === "blob" && carried(column) && typeof value === "number" && sink && rowid) {
          return captureNativeBlob(
            value,
            (offset, length) => {
              const chunk = source
                .prepare(
                  `SELECT substr(${importIdentifier(column)},?,?) AS chunk FROM ${name} WHERE ${importIdentifier(rowid)}=?`
                )
                .get(offset + 1, length, row._source_rowid) as { chunk: Uint8Array } | undefined;
              return chunk?.chunk ?? null;
            },
            sink
          );
        }
        if (type === "blob" && value instanceof Uint8Array)
          return { kind: "bytes" as const, value: Buffer.from(value).toString("base64") };
        throw new ApplicationImportError("value", table.name, column);
      }),
    })),
  };
}

/** Read-only, synchronous and consistent. Never await or run migrations/repair in this source scope. */
export function captureSqliteApplicationSnapshot(
  source: SqliteAdapter,
  options: { blobSink?: NativeBlobSink } = {}
): ApplicationStorageSnapshot {
  if (source.inTransaction || source.pragma("query_only", { simple: true }) !== 1)
    throw new ApplicationImportError("source_not_readonly");
  return source.transaction(() => {
    const definitions = source
      .prepare("SELECT name,type,sql FROM sqlite_schema ORDER BY type,name")
      .all() as SourceDefinition[];
    const tables = (source.pragma("table_list") as SourceTable[])
      .filter((table) => table.schema === "main")
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    const derived = new Set<string>();
    const excluded: ApplicationStorageSnapshot["excluded"] = [];
    for (const table of tables)
      if (table.type === "virtual") {
        const declaration =
          definitions.find((definition) => definition.name === table.name)?.sql ?? "";
        const content = /\bcontent\s*=\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]/i.exec(declaration)?.[1];
        if (
          /\bUSING\s+fts[345]\s*\(/i.test(declaration) &&
          content &&
          (APPLICATION_IMPORT_TABLES as readonly string[]).includes(content)
        ) {
          derived.add(table.name);
          excluded.push({
            name: table.name,
            reason: `derived FTS index of ${content}; sqlite_schema declaration binds its source`,
          });
        } else if (table.name === "vec_memories" && /\bUSING\s+vec0\s*\(/i.test(declaration)) {
          derived.add(table.name);
        } else throw new ApplicationImportError("schema", table.name);
      }
    let vectorStorage: ReturnType<typeof qualifySqliteVecDerivedStorage> | undefined;
    if (derived.has("vec_memories")) {
      try {
        vectorStorage = qualifySqliteVecDerivedStorage(source, definitions, tables);
      } catch (error) {
        if (error instanceof SqliteVecQualificationError)
          throw new ApplicationImportError("schema", error.table);
        throw error;
      }
    }
    const snapshot: ApplicationStorageSnapshot = {
      format: "omniroute-application-storage-v1",
      source: "sqlite",
      tables: [],
      metadataTables: [],
      definitions,
      excluded,
    };
    const budget = {
      rows: 0,
      bytes: Buffer.byteLength(JSON.stringify(definitions)),
      carrierBytes: 0,
    };
    for (const table of tables) {
      if (table.name === "sqlite_schema") continue;
      if (derived.has(table.name)) continue;
      if (vectorStorage?.names.has(table.name)) {
        if (table.name === SQLITE_VEC_INFO_TABLE)
          snapshot.metadataTables.push(readTable(source, table, budget));
        else
          excluded.push({
            name: table.name,
            reason: `derived vec_memories storage qualified by sqlite-vec ${SQLITE_VEC_CAPTURE_DECODER}; source producer ${vectorStorage.producerVersion}; logical embeddings captured`,
          });
        continue;
      }
      if (table.type === "view") {
        excluded.push({
          name: table.name,
          reason: "derived view definition preserved in snapshot metadata",
        });
        continue;
      }
      if (
        table.type === "shadow" &&
        [...derived].some(
          (parent) => parent !== "vec_memories" && table.name.startsWith(`${parent}_`)
        )
      ) {
        excluded.push({
          name: table.name,
          reason: "shadow storage of an evidenced FTS parent",
        });
        continue;
      }
      if (table.type !== "table") throw new ApplicationImportError("schema", table.name);
      if ((APPLICATION_IMPORT_SOURCE_METADATA as readonly string[]).includes(table.name))
        snapshot.metadataTables.push(readTable(source, table, budget));
      else if ((APPLICATION_IMPORT_TABLES as readonly string[]).includes(table.name))
        snapshot.tables.push(readTable(source, table, budget, options.blobSink));
      else throw new ApplicationImportError("schema", table.name);
    }
    if (!snapshot.tables.some((table) => table.name === "key_value"))
      throw new ApplicationImportError("schema", "key_value");
    if (derived.has("vec_memories")) {
      const definition = definitions.find((row) => row.name === "vec_memories")?.sql ?? "";
      if (/int8\s*\[/i.test(definition))
        throw new ApplicationImportError("value", "vec_memories", "embedding");
      const meta = snapshot.tables.find((table) => table.name === "memory_vec_meta");
      const signatureIndex = meta?.columns.indexOf("embedding_signature") ?? -1;
      const dimensionIndex = meta?.columns.indexOf("active_dim") ?? -1;
      const signature = meta?.rows[0]?.values[signatureIndex];
      const dimension = meta?.rows[0]?.values[dimensionIndex];
      if (
        typeof signature !== "string" ||
        !dimension ||
        typeof dimension !== "object" ||
        dimension.kind !== "integer"
      )
        throw new ApplicationImportError("schema", "memory_vec_meta");
      const size = Number(dimension.value);
      if (!Number.isInteger(size) || size < 1 || size > 65536)
        throw new ApplicationImportError("value", "vec_memories");
      const vectors = source
        .prepare(
          "SELECT CAST(v.rowid AS TEXT) AS source_rowid,m.id AS memory_id,v.embedding FROM vec_memories v JOIN memories m ON m.rowid=v.rowid ORDER BY v.rowid LIMIT ?"
        )
        .all(APPLICATION_SNAPSHOT_LIMITS.rows - budget.rows + 1) as {
        source_rowid: string;
        memory_id: string;
        embedding: Uint8Array;
      }[];
      budget.rows += vectors.length;
      const sourceCount = source
        .prepare("SELECT CAST(COUNT(*) AS TEXT) AS count FROM vec_memories")
        .get() as { count: string };
      if (BigInt(sourceCount.count) !== BigInt(vectors.length))
        throw new ApplicationImportError("verification", "vec_memories");
      snapshot.metadataTables.push({
        name: "vec_memories",
        columns: ["embedding"],
        rowOrder: "rowid",
        rows: vectors.map((row) => ({
          rowid: row.source_rowid,
          values: [{ kind: "bytes", value: Buffer.from(row.embedding).toString("base64") }],
        })),
      });
      snapshot.tables.push({
        name: "memory_vectors",
        columns: ["memory_id", "signature", "embedding"],
        rowOrder: "native",
        rows: vectors.map((row) => {
          if (!(row.embedding instanceof Uint8Array) || row.embedding.byteLength !== size * 4)
            throw new ApplicationImportError("value", "vec_memories", "embedding");
          const bytes = Buffer.from(row.embedding);
          const values = Array.from({ length: size }, (_, index) => bytes.readFloatLE(index * 4));
          if (values.some((value) => !Number.isFinite(value)))
            throw new ApplicationImportError("value", "vec_memories", "embedding");
          return {
            rowid: null,
            values: [row.memory_id, signature, { kind: "vector", value: values }],
          };
        }),
      });
    }
    snapshot.tables.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    return checkedApplicationSnapshot(snapshot);
  })();
}

interface OfflineNodeDatabase extends NodeSqliteDatabaseLike {
  readonly isTransaction: boolean;
  enableLoadExtension?(enabled: boolean): void;
}

function openLargeFileSqliteSource(filename: string): SqliteAdapter {
  let database: OfflineNodeDatabase | undefined;
  try {
    // Resolve an existing regular file so URI/memory names cannot create another database.
    const resolved = realpathSync(filename);
    if (!statSync(resolved).isFile()) throw new ApplicationImportError("schema");
    const { DatabaseSync } = runtimeRequire("node:sqlite") as {
      DatabaseSync: new (
        filename: string,
        options: {
          readOnly: boolean;
          allowExtension: boolean;
          timeout: number;
        }
      ) => OfflineNodeDatabase;
    };
    database = new DatabaseSync(resolved, {
      readOnly: true,
      allowExtension: true,
      timeout: 3000,
    });
    if (typeof database.isTransaction !== "boolean") throw new ApplicationImportError("schema");
    const raw = database;
    const source = createNodeSqliteAdapterFromDatabase(raw, resolved);
    // Offline capture rejects an already active transaction on every selected driver.
    Object.defineProperty(source, "inTransaction", { get: () => source.open && raw.isTransaction });
    return source;
  } catch {
    try {
      database?.close();
    } catch {
      /* Preserve the fixed source-opening error. */
    }
    throw new ApplicationImportError("schema");
  }
}

/** Existing file only; the returned adapter is never installed as the application singleton. */
export function openOfflineSqliteSource(
  filename: string,
  options: { largeFiles?: boolean } = {}
): SqliteAdapter {
  const source =
    options.largeFiles === true
      ? openLargeFileSqliteSource(filename)
      : tryOpenSync(filename, { readonly: true, fileMustExist: true, timeout: 3000 });
  if (!source) throw new ApplicationImportError("schema");
  try {
    source.pragma("query_only = ON");
    const vector = source
      .prepare("SELECT sql FROM sqlite_schema WHERE name='vec_memories'")
      .get() as { sql?: string } | undefined;
    if (vector?.sql && /\bUSING\s+vec0\s*\(/i.test(vector.sql)) {
      const extension = runtimeRequire("sqlite-vec") as { load(database: unknown): void };
      extension.load(source.raw);
    }
    if (options.largeFiles === true) {
      const raw = source.raw as OfflineNodeDatabase;
      if (typeof raw.enableLoadExtension === "function") raw.enableLoadExtension(false);
    }
    return source;
  } catch {
    source.close();
    throw new ApplicationImportError("schema", "vec_memories");
  }
}
