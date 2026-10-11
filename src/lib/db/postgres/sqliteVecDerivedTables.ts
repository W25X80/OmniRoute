import type { SqliteAdapter } from "../adapters/types";

export const SQLITE_VEC_CAPTURE_DECODER = "v0.1.9";
export const SQLITE_VEC_INFO_TABLE = "vec_memories_info";
const PHYSICAL_SQL = {
  vec_memories_chunks:
    'CREATE TABLE "vec_memories_chunks"(chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,size INTEGER NOT NULL,validity BLOB NOT NULL,rowids BLOB NOT NULL)',
  vec_memories_info: 'CREATE TABLE "vec_memories_info" (key text primary key, value any)',
  vec_memories_rowids:
    'CREATE TABLE "vec_memories_rowids"(rowid INTEGER PRIMARY KEY AUTOINCREMENT,id,chunk_id INTEGER,chunk_offset INTEGER)',
  vec_memories_vector_chunks00:
    'CREATE TABLE "vec_memories_vector_chunks00"(rowid PRIMARY KEY,vectors BLOB NOT NULL)',
} as const;
export interface SqliteVecDefinition {
  name: string;
  type: string;
  sql: string | null;
}
interface VersionRow {
  key: unknown;
  value: unknown;
  valueType: unknown;
}
export interface SqliteVecMetadataTable {
  columns: readonly string[];
  rows: readonly { values: readonly unknown[] }[];
}

export class SqliteVecQualificationError extends Error {
  constructor(readonly table = "vec_memories") {
    super("SQLite vector storage layout was not qualified");
    this.name = "SqliteVecQualificationError";
  }
}

function supportedParent(sql: string | null): boolean {
  const match =
    /^CREATE\s+VIRTUAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:vec_memories|"vec_memories"|`vec_memories`|\[vec_memories\])\s+USING\s+vec0\s*\(\s*embedding\s+(?:float|int8)\s*\[\s*([0-9]+)\s*\]\s*\)\s*;?\s*$/i.exec(
      sql ?? ""
    );
  const dimension = match ? Number(match[1]) : NaN;
  return Number.isInteger(dimension) && dimension >= 1 && dimension <= 65_536;
}

/** A prefix never proves ownership; these definitions are actual sqlite-vec 0.1.9 output. */
function physicalLayout(definitions: readonly SqliteVecDefinition[]): boolean {
  const parent = definitions.filter((definition) => definition.name === "vec_memories");
  if (parent.length !== 1 || parent[0].type !== "table" || !supportedParent(parent[0].sql))
    return false;
  return Object.entries(PHYSICAL_SQL).every(([name, sql]) => {
    const matches = definitions.filter((definition) => definition.name === name);
    return matches.length === 1 && matches[0].type === "table" && matches[0].sql === sql;
  });
}

function producerVersion(rows: readonly VersionRow[]): string | null {
  if (rows.length !== 4) return null;
  const names = [
    "CREATE_VERSION",
    "CREATE_VERSION_MAJOR",
    "CREATE_VERSION_MINOR",
    "CREATE_VERSION_PATCH",
  ];
  if (
    new Set(rows.map((row) => row.key)).size !== 4 ||
    rows.some((row) => !names.includes(String(row.key)))
  )
    return null;
  const version = rows.find((row) => row.key === "CREATE_VERSION");
  if (
    version?.valueType !== "text" ||
    typeof version.value !== "string" ||
    Buffer.byteLength(version.value) > 128
  )
    return null;
  // SemVer core fields are the module's explicit metadata. Suffixes remain opaque lineage.
  const match =
    /^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/.exec(
      version.value
    );
  if (
    !match ||
    match[4]?.split(".").some((part) => /^[0-9]+$/.test(part) && part.length > 1 && part[0] === "0")
  )
    return null;
  for (let index = 1; index <= 3; index++) {
    const row = rows.find((item) => item.key === names[index]);
    if (row?.valueType !== "integer" || typeof row.value !== "string" || row.value !== match[index])
      return null;
  }
  return version.value;
}

/** Guarded metadata admission also applies to imported JSON snapshots, without database access. */
export function qualifiedSqliteVecMetadata(
  definitions: readonly SqliteVecDefinition[],
  table: SqliteVecMetadataTable
): boolean {
  if (
    !physicalLayout(definitions) ||
    table.columns.length !== 2 ||
    table.columns[0] !== "key" ||
    table.columns[1] !== "value" ||
    table.rows.some((row) => row.values.length !== 2)
  )
    return false;
  const rows: VersionRow[] = table.rows.map(({ values }) => {
    const value = values[1];
    const integer =
      value &&
      typeof value === "object" &&
      "kind" in value &&
      value.kind === "integer" &&
      "value" in value;
    return {
      key: values[0],
      value: integer ? value.value : value,
      valueType: integer ? "integer" : typeof value === "string" ? "text" : typeof value,
    };
  });
  return producerVersion(rows) !== null;
}

export function qualifySqliteVecDerivedStorage(
  source: SqliteAdapter,
  definitions: readonly SqliteVecDefinition[],
  tables: readonly { name: string; type: string; wr: number }[]
): { names: Set<string>; producerVersion: string } {
  if (!physicalLayout(definitions)) throw new SqliteVecQualificationError();
  for (const name of Object.keys(PHYSICAL_SQL)) {
    const table = tables.find((table) => table.name === name);
    if (!table || !["table", "shadow"].includes(table.type) || table.wr !== 0)
      throw new SqliteVecQualificationError(name);
  }
  try {
    const decoder = source.prepare("SELECT vec_version() AS version").get() as {
      version?: unknown;
    };
    if (decoder?.version !== SQLITE_VEC_CAPTURE_DECODER) throw new SqliteVecQualificationError();
    // No full BLOB/value read is needed to qualify four small module-owned version fields.
    const rows = source
      .prepare(
        `SELECT
      CASE WHEN typeof(key)='text' AND length(CAST(key AS BLOB))<=128 THEN key END AS key,
      typeof(value) AS valueType,
      CASE WHEN typeof(value)='integer' THEN CAST(value AS TEXT)
        WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=128 THEN value END AS value
      FROM vec_memories_info LIMIT 5`
      )
      .all() as VersionRow[];
    const version = producerVersion(rows);
    if (!version) throw new SqliteVecQualificationError(SQLITE_VEC_INFO_TABLE);
    return {
      names: new Set(Object.keys(PHYSICAL_SQL)),
      producerVersion: version,
    };
  } catch (error) {
    if (error instanceof SqliteVecQualificationError) throw error;
    throw new SqliteVecQualificationError();
  }
}
