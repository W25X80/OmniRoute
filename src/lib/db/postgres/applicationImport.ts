import { randomUUID } from "node:crypto";
import {
  applicationImportDigest as canonical,
  APPLICATION_IMPORT_DIGEST_ALGORITHM,
} from "./applicationImportDigest";
import { requireExplicitStorageEncryptionKey, decrypt } from "../encryption";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import {
  APPLICATION_IMPORT_TABLES,
  APPLICATION_IMPORT_NATIVE_ONLY,
  APPLICATION_IMPORT_NATIVE_FILE_TABLES,
} from "./applicationImportManifest";
import {
  ApplicationImportError,
  APPLICATION_SNAPSHOT_LIMITS,
  applicationSnapshotDigest,
  checkedApplicationSnapshot,
  importIdentifier,
  type ApplicationImportCell,
  type ApplicationStorageSnapshot,
  type ApplicationSnapshotTable,
} from "./applicationSnapshot";
import {
  verifyApplicationArtifacts,
  assertApplicationArtifactPublicationScope,
  type OfflineArtifactMapping,
} from "./applicationImportArtifacts";
import type { NativeArtifactStore } from "@/domain/persistence/artifactStore";

import {
  captureNativeBlobAsync,
  verifyNativeBlob,
  NATIVE_BLOB_LIMITS,
  type NativeBlobReference,
  type NativeBlobSource,
  type NativeBlobSink,
} from "./nativeBlobCarrier";

import {
  openStoredFileContent,
  replaceStoredFileContent,
  getStoredFileContentBounds,
  type StoredFileContent,
} from "./serviceFileContent";

export interface ApplicationBlobImportOptions {
  source: NativeBlobSource;
  memoryBudgetBytes: number;
}
interface RestoreAuthority {
  sourceAuthority: string;
  sourceGeneration: string;
  targetAuthority: string;
  targetGeneration: "0";
}
interface TargetColumn {
  name: string;
  type: string;
  nullable: boolean;
  defaultExpression: string | null;
  identity: string;
  generated: string;
}
interface TargetTable {
  name: string;
  columns: TargetColumn[];
  primary: string[];
  parents: string[];
}
interface TargetSchema {
  tables: TargetTable[];
  digest: string;
}
export interface ApplicationImportReceipt {
  format: "omniroute-application-import-receipt-v1";
  digestAlgorithm: typeof APPLICATION_IMPORT_DIGEST_ALGORITHM;
  sourceDigest: string;
  planDigest: string;
  schemaDigest: string;
  rowCount: number;
  tableCounts: Record<string, number>;
  artifacts: OfflineArtifactMapping[];
  transformations: {
    table: string;
    row: number;
    column: string;
    source: ApplicationImportCell;
    target: ApplicationImportCell;
  }[];
  alreadyImported: boolean;
  restoreAuthority?: RestoreAuthority;
}
interface PlannedTable {
  name: string;
  columns: TargetColumn[];
  rows: ApplicationImportCell[][];
  order: string[];
}
interface ImportPlan {
  snapshot: ApplicationStorageSnapshot;
  sourceDigest: string;
  schemaDigest: string;
  planDigest: string;
  tables: PlannedTable[];
  artifacts: OfflineArtifactMapping[];
  transformations: ApplicationImportReceipt["transformations"];
  restoreAuthority?: RestoreAuthority;
}
const controlTables = new Set([
  "schema_migrations",
  "routing_imports",
  "application_import_receipts",
]);
const integerTypes = new Set(["smallint", "integer", "bigint"]);
const logicalTables = new Set<string>([
  ...APPLICATION_IMPORT_TABLES,
  ...APPLICATION_IMPORT_NATIVE_ONLY,
]);
const fileContentTables = new Set<string>(APPLICATION_IMPORT_NATIVE_FILE_TABLES);
const allTables = new Set<string>([...logicalTables, ...fileContentTables]);

async function readTargetSchema(transaction: PostgresTransaction): Promise<TargetSchema> {
  const columns = await transaction.query<{
    table: string;
    column: TargetColumn;
  }>(`SELECT c.relname AS "table",
    json_build_object('name',a.attname,'type',pg_catalog.format_type(a.atttypid,a.atttypmod),
      'nullable',NOT a.attnotnull,'defaultExpression',pg_get_expr(d.adbin,d.adrelid),
      'identity',a.attidentity,'generated',a.attgenerated) AS "column"
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    WHERE n.nspname='omniroute' AND c.relkind='r' ORDER BY c.relname COLLATE "C",a.attnum`);
  const keys = await transaction.query<{
    table: string;
    primary: string[];
    parents: string[];
  }>(`SELECT c.relname AS "table",
    COALESCE((SELECT array_agg(a.attname ORDER BY ord.position) FROM pg_catalog.pg_index i
      CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY ord(number,position)
      JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ord.number
      WHERE i.indrelid=c.oid AND i.indisprimary),'{}'::name[])::text[] AS "primary",
    COALESCE((SELECT array_agg(DISTINCT parent.relname ORDER BY parent.relname) FROM pg_catalog.pg_constraint f
      JOIN pg_catalog.pg_class parent ON parent.oid=f.confrelid WHERE f.conrelid=c.oid AND f.contype='f'), '{}'::name[])::text[] AS parents
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='omniroute' AND c.relkind='r' ORDER BY c.relname COLLATE "C"`);
  const grouped = new Map<string, TargetTable>();
  for (const row of columns.rows) {
    if (!allTables.has(row.table)) throw new ApplicationImportError("schema", row.table);
    importIdentifier(row.table);
    importIdentifier(row.column.name);
    const key = keys.rows.find((value) => value.table === row.table);
    if (!key) throw new ApplicationImportError("schema", row.table);
    const table = grouped.get(row.table) ?? {
      name: row.table,
      columns: [],
      primary: key.primary,
      parents: key.parents,
    };
    table.columns.push(row.column);
    grouped.set(row.table, table);
  }
  if ([...allTables].some((name) => !grouped.has(name))) throw new ApplicationImportError("schema");
  const tables = [...grouped.values()];
  const authority = await transaction.query<
    Record<string, unknown>
  >(`SELECT 'migration' AS kind,version::text AS name,checksum AS definition FROM omniroute.schema_migrations
    UNION ALL SELECT 'constraint',c.conname,pg_get_constraintdef(c.oid) FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='omniroute'
    UNION ALL SELECT 'trigger',t.tgname,pg_get_triggerdef(t.oid) FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='omniroute' AND NOT t.tgisinternal
    ORDER BY kind,name,definition`);
  return { tables, digest: canonical({ tables, authority: authority.rows }) };
}

function validUsageTimestamp(value: string): boolean {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?)?$/i.exec(
      value
    );
  if (!match) return false;
  const zone = /[+-](\d{2}):(\d{2})$/.exec(value);
  if ((zone && (Number(zone[1]) > 14 || Number(zone[2]) > 59)) || (match[7] && match[7].length > 4))
    return false;
  const year = Number(match[1]),
    month = Number(match[2]),
    day = Number(match[3]);
  const calendar = new Date(0);
  calendar.setUTCFullYear(year, month, 0);
  if (year < 1 || month < 1 || month > 12 || day < 1 || day > calendar.getUTCDate()) return false;
  const hour = Number(match[4] ?? 0),
    minute = Number(match[5] ?? 0),
    second = Number(match[6] ?? 0);
  if (
    minute > 59 ||
    second > 59 ||
    hour > 24 ||
    (hour === 24 && (minute !== 0 || second !== 0 || Number(match[7] ?? 0) !== 0))
  )
    return false;
  return Number.isFinite(
    Date.parse(/(?:Z|[+-]\d{2}:\d{2})$/i.test(value) ? value : `${value.replace(" ", "T")}Z`)
  );
}

function checkedCell(
  cell: ApplicationImportCell,
  column: TargetColumn,
  table: string
): ApplicationImportCell {
  if (cell === null) {
    if (!column.nullable) throw new ApplicationImportError("value", table, column.name);
    return null;
  }
  if (integerTypes.has(column.type)) {
    const value =
      typeof cell === "object" && cell.kind === "integer"
        ? cell.value
        : typeof cell === "number" && Number.isSafeInteger(cell)
          ? String(cell)
          : null;
    if (value === null) throw new ApplicationImportError("value", table, column.name);
    const number = BigInt(value),
      bound =
        column.type === "smallint"
          ? 32767n
          : column.type === "integer"
            ? 2147483647n
            : 9223372036854775807n;
    if (number < -bound - 1n || number > bound)
      throw new ApplicationImportError("value", table, column.name);
    if (
      /(?:^tokens_|_tokens$|token_count|prompt_tokens|completion_tokens)/.test(column.name) &&
      (number < 0n || number > BigInt(Number.MAX_SAFE_INTEGER))
    )
      throw new ApplicationImportError("value", table, column.name);
    return { kind: "integer", value };
  }
  if (column.type === "double precision" || column.type === "real" || column.type === "numeric") {
    const value =
      typeof cell === "number"
        ? cell
        : typeof cell === "object" && cell.kind === "integer"
          ? Number(cell.value)
          : NaN;
    if (
      !Number.isFinite(value) ||
      (typeof cell === "object" && cell.kind === "integer" && BigInt(value) !== BigInt(cell.value))
    )
      throw new ApplicationImportError("value", table, column.name);
    return column.type === "real" ? Math.fround(value) : value;
  }
  if (column.type === "bytea") {
    if (
      typeof cell !== "object" ||
      (cell.kind !== "bytes" &&
        !(cell.kind === "blob-reference" && table === "files" && column.name === "content"))
    )
      throw new ApplicationImportError("value", table, column.name);
    return cell;
  }
  if (column.type === "real[]") {
    if (
      typeof cell !== "object" ||
      cell.kind !== "vector" ||
      cell.value.some((value) => Math.fround(value) !== value)
    )
      throw new ApplicationImportError("value", table, column.name);
    return cell;
  }
  if (column.type === "boolean") {
    if (typeof cell === "boolean") return cell;
    if (
      typeof cell === "object" &&
      cell.kind === "integer" &&
      (cell.value === "0" || cell.value === "1")
    )
      return cell.value === "1";
    throw new ApplicationImportError("value", table, column.name);
  }
  if (
    column.type === "text" ||
    column.type === "uuid" ||
    column.type === "timestamp with time zone"
  ) {
    if (typeof cell !== "string" || cell.includes("\0"))
      throw new ApplicationImportError("value", table, column.name);
    if (
      [
        "usage_history",
        "quota_snapshots",
        "call_logs",
        "call_logs_v1_legacy",
        "proxy_logs",
        "request_detail_logs",
        "daily_usage_summary",
        "hourly_usage_summary",
      ].includes(table) &&
      ["timestamp", "created_at", "date", "date_hour"].includes(column.name) &&
      !validUsageTimestamp(cell)
    )
      throw new ApplicationImportError("value", table, column.name);
    if (
      (table === "quota_snapshots" && column.name === "created_at") ||
      ([
        "usage_history",
        "call_logs",
        "call_logs_v1_legacy",
        "proxy_logs",
        "request_detail_logs",
      ].includes(table) &&
        column.name === "timestamp")
    ) {
      return new Date(
        /(?:Z|[+-]\d{2}:\d{2})$/i.test(cell) ? cell : `${cell.replace(" ", "T")}Z`
      ).toISOString();
    }
    return cell;
  }
  throw new ApplicationImportError("schema", table, column.name);
}
function bind(cell: ApplicationImportCell): unknown {
  if (cell && typeof cell === "object" && cell.kind === "blob-reference")
    throw new ApplicationImportError("schema", "files", "content");
  if (cell && typeof cell === "object")
    return cell.kind === "integer"
      ? cell.value
      : cell.kind === "bytes"
        ? Buffer.from(cell.value, "base64")
        : cell.kind === "vector"
          ? cell.value
          : undefined;
  return cell;
}
function columnOrder(table: TargetTable): string[] {
  if (table.columns.some((column) => column.name === "insertion_order")) return ["insertion_order"];
  if (table.primary.length) return table.primary;
  throw new ApplicationImportError("schema", table.name);
}
function buildPlan(
  snapshot: ApplicationStorageSnapshot,
  schema: TargetSchema,
  artifacts: OfflineArtifactMapping[],
  restoreAuthority?: RestoreAuthority
): ImportPlan {
  const planned: PlannedTable[] = [],
    transformations: ApplicationImportReceipt["transformations"] = [];
  for (const source of snapshot.tables) {
    if (
      !logicalTables.has(source.name) ||
      controlTables.has(source.name) ||
      (snapshot.source === "sqlite" &&
        (APPLICATION_IMPORT_NATIVE_ONLY as readonly string[]).includes(source.name) &&
        source.name !== "memory_vectors")
    )
      throw new ApplicationImportError("schema", source.name);
    const target = schema.tables.find((table) => table.name === source.name);
    if (!target) throw new ApplicationImportError("schema", source.name);
    const columns = source.columns.map((name) => {
      const column = target.columns.find((value) => value.name === name && !value.generated);
      if (!column) throw new ApplicationImportError("schema", source.name, name);
      return column;
    });
    const insertion = target.columns.find(
      (column) => column.name === "insertion_order" && !source.columns.includes(column.name)
    );
    if (insertion) columns.push(insertion);
    for (const column of target.columns)
      if (
        !column.generated &&
        !column.nullable &&
        !column.defaultExpression &&
        !column.identity &&
        !columns.some((value) => value.name === column.name) &&
        source.rows.length
      )
        throw new ApplicationImportError("schema", source.name, column.name);
    const rows = source.rows.map((row, index) => {
      const values = [...row.values];
      if (insertion) values.push({ kind: "integer", value: row.rowid ?? String(index + 1) });
      if (source.name === "api_key_policy_epoch" && snapshot.source === "postgres") {
        if (!restoreAuthority) throw new ApplicationImportError("schema", source.name);
        const authority = source.columns.indexOf("authority_id"),
          generation = source.columns.indexOf("generation");
        if (
          authority < 0 ||
          generation < 0 ||
          source.rows.length !== 1 ||
          values[authority] !== restoreAuthority.sourceAuthority ||
          !values[generation] ||
          typeof values[generation] !== "object" ||
          values[generation].kind !== "integer" ||
          values[generation].value !== restoreAuthority.sourceGeneration
        )
          throw new ApplicationImportError("schema", source.name);
        values[authority] = restoreAuthority.targetAuthority;
        values[generation] = { kind: "integer", value: restoreAuthority.targetGeneration };
      }
      const path = source.columns.indexOf("artifact_relpath");
      if (
        ["call_logs", "call_logs_v1_legacy"].includes(source.name) &&
        path >= 0 &&
        typeof values[path] === "string" &&
        values[path] !== ""
      ) {
        const mapped = artifacts.find((value) => value.sourceReference === values[path]);
        if (!mapped) throw new ApplicationImportError("artifact", source.name, "artifact_relpath");
        values[path] = mapped.targetReference;
        for (const [name, value] of [
          ["artifact_sha256", mapped.checksum],
          ["artifact_size_bytes", { kind: "integer", value: String(mapped.sizeBytes) }],
        ] as const) {
          const at = columns.findIndex((column) => column.name === name);
          if (at >= 0) values[at] = value;
        }
      }
      return values.map((cell, at) => {
        const checked = checkedCell(cell, columns[at], source.name);
        const original = at < row.values.length ? row.values[at] : cell;
        if (canonical(original) !== canonical(checked))
          transformations.push({
            table: source.name,
            row: index,
            column: columns[at].name,
            source: original,
            target: checked,
          });
        return checked;
      });
    });
    const order = columnOrder(target);
    rows.sort((left, right) => {
      for (const name of order) {
        const at = columns.findIndex((column) => column.name === name);
        if (at < 0) throw new ApplicationImportError("schema", source.name, name);
        const a = left[at],
          b = right[at];
        let compare = 0;
        if (
          a &&
          b &&
          typeof a === "object" &&
          typeof b === "object" &&
          a.kind === "integer" &&
          b.kind === "integer"
        )
          compare =
            BigInt(a.value) < BigInt(b.value) ? -1 : BigInt(a.value) > BigInt(b.value) ? 1 : 0;
        else if (typeof a === "number" && typeof b === "number") compare = a - b;
        else compare = Buffer.compare(Buffer.from(String(a)), Buffer.from(String(b)));
        if (compare) return compare;
      }
      return 0;
    });
    planned.push({ name: source.name, columns, rows, order });
  }
  addArtifactPublicationPlan(planned, schema, artifacts, transformations);
  const sourceDigest = applicationSnapshotDigest(snapshot);
  const planDigest = canonical({
    sourceDigest,
    schemaDigest: schema.digest,
    tables: planned,
    artifacts,
    transformations,
    restoreAuthority,
  });
  return {
    snapshot,
    sourceDigest,
    schemaDigest: schema.digest,
    planDigest,
    tables: planned,
    artifacts,
    transformations,
    restoreAuthority,
  };
}
/** Keep scoped lifecycle ownership in the same atomic import and receipt as its log rows. */
function addArtifactPublicationPlan(
  planned: PlannedTable[],
  schema: TargetSchema,
  artifacts: OfflineArtifactMapping[],
  transformations: ApplicationImportReceipt["transformations"]
): void {
  const index = planned.findIndex((table) => table.name === "artifact_deletion_queue");
  if (artifacts.length === 0 && index < 0) return;
  const target = schema.tables.find((table) => table.name === "artifact_deletion_queue");
  if (!target) throw new ApplicationImportError("schema", "artifact_deletion_queue");
  const columns = target.columns.filter((column) => !column.generated);
  const old = index < 0 ? undefined : planned[index];
  const entries = (old?.rows ?? []).map((row, rowIndex) => {
    const values: Record<string, ApplicationImportCell> = Object.fromEntries(
      old!.columns.map((column, at) => [column.name, row[at]])
    );
    values.publication_state ??= "delete_pending";
    values.upload_state ??= "acknowledged";
    values.publication_owner ??= null;
    values.store_scope ??= null;
    values.expected_size_bytes ??= null;
    values.expected_sha256 ??= null;
    values.next_attempt_at ??= values.created_at;
    const mapped = artifacts.find((entry) => entry.sourceReference === values.relpath);
    if (
      mapped &&
      values.publication_state === "attached" &&
      values.upload_state === "acknowledged"
    ) {
      for (const [name, value] of Object.entries(artifactPublicationValues(mapped))) {
        if (canonical(values[name] ?? null) !== canonical(value))
          transformations.push({
            table: "artifact_deletion_queue",
            row: rowIndex,
            column: name,
            source: values[name] ?? null,
            target: value,
          });
        values[name] = value;
      }
    }
    return values;
  });
  for (const mapping of artifacts) {
    if (!entries.some((entry) => entry.relpath === mapping.targetReference))
      entries.push(artifactPublicationValues(mapping));
  }
  const identities = entries.map((entry) => entry.relpath);
  if (new Set(identities).size !== identities.length)
    throw new ApplicationImportError("artifact", "artifact_deletion_queue", "relpath");
  const table: PlannedTable = {
    name: target.name,
    columns,
    order: columnOrder(target),
    rows: entries.map((entry) =>
      columns.map((column) => checkedCell(entry[column.name] ?? null, column, target.name))
    ),
  };
  table.rows.sort((left, right) => {
    const at = columns.findIndex((column) => column.name === "relpath");
    return Buffer.compare(Buffer.from(String(left[at])), Buffer.from(String(right[at])));
  });
  if (index < 0) planned.push(table);
  else planned[index] = table;
}
function artifactPublicationValues(
  mapping: OfflineArtifactMapping
): Record<string, ApplicationImportCell> {
  if (
    !mapping.storageScope ||
    !/^[0-9a-f]{64}$/.test(mapping.storageScope) ||
    !mapping.preparedAt ||
    !Number.isFinite(Date.parse(mapping.preparedAt))
  )
    throw new ApplicationImportError("artifact", "artifact_deletion_queue");
  return {
    relpath: mapping.targetReference,
    created_at: mapping.preparedAt,
    publication_state: "attached",
    upload_state: mapping.singleAttempt ? "acknowledged" : "legacy_unknown",
    publication_owner: null,
    store_scope: mapping.storageScope,
    expected_size_bytes: { kind: "integer", value: String(mapping.sizeBytes) },
    expected_sha256: mapping.checksum,
    next_attempt_at: mapping.preparedAt,
  };
}
function insertionOrder(plan: ImportPlan, schema: TargetSchema): PlannedTable[] {
  const result: PlannedTable[] = [],
    pending = [...plan.tables],
    done = new Set<string>();
  while (pending.length) {
    const at = pending.findIndex((table) =>
      schema.tables
        .find((value) => value.name === table.name)!
        .parents.every(
          (parent) =>
            parent === table.name ||
            done.has(parent) ||
            !pending.some((value) => value.name === parent)
        )
    );
    if (at < 0) throw new ApplicationImportError("schema");
    const [table] = pending.splice(at, 1);
    result.push(table);
    done.add(table.name);
  }
  return result;
}

function pristineSeed(table: string, row: Record<string, unknown>): boolean {
  const number = (key: string) => Number(row[key]);
  if (table === "quota_groups") return row.id === "group-demo" && row.name === "GroupDemo";
  if (table === "radar_settings")
    return number("id") === 1 && number("opt_in") === 0 && row.supporter_key_encrypted === null;
  if (table === "memory_vec_meta")
    return (
      number("id") === 1 &&
      row.active_dim === null &&
      row.embedding_signature === null &&
      row.last_reset_at === null &&
      number("vec_loaded") === 0
    );
  if (table === "cache_metrics")
    return (
      ["hits", "misses", "tokens_saved"].includes(String(row.key)) &&
      number("value") === 0 &&
      row.updated_at === ""
    );
  if (table === "compression_combos")
    return (
      row.id === "default-caveman" &&
      row.name === "Standard Savings" &&
      row.description === "Default lossless dedup and whitespace compression" &&
      row.pipeline === '[{"engine":"session-dedup"},{"engine":"lite"}]' &&
      row.language_packs === '["en"]' &&
      number("is_default") === 1 &&
      number("output_mode") === 0 &&
      row.output_mode_intensity === "full"
    );
  if (table === "version_manager") {
    const seeds: Record<string, [number, number, number]> = {
      "9router": [20130, 1, 1],
      mux: [8322, 0, 0],
      bifrost: [8080, 1, 1],
      openwa: [8323, 0, 0],
      llmlingua: [20135, 0, 0],
    };
    const seed = seeds[String(row.tool)];
    return Boolean(
      seed &&
      row.status === "not_installed" &&
      number("port") === seed[0] &&
      number("auto_update") === seed[1] &&
      number("provider_expose") === seed[2] &&
      number("auto_start") === 0 &&
      number("auto_restart_adopted") === 0 &&
      row.health_status === "unknown" &&
      [
        "current_version",
        "installed_version",
        "pinned_version",
        "binary_path",
        "pid",
        "api_key",
        "management_key",
        "last_health_check",
        "last_update_check",
        "config_overrides",
        "error_message",
        "logs_buffer_path",
        "last_sync_at",
      ].every((key) => row[key] === null)
    );
  }
  return false;
}
async function assertEmptyTarget(
  transaction: PostgresTransaction,
  schema: TargetSchema,
  plan: ImportPlan
): Promise<void> {
  for (const table of schema.tables) {
    if (controlTables.has(table.name)) continue;
    if (table.name === "api_key_policy_epoch") {
      const rows = (
        await transaction.query<{ singleton: boolean; generation: string }>(
          "SELECT singleton,generation::text AS generation FROM omniroute.api_key_policy_epoch"
        )
      ).rows;
      if (rows.length !== 1 || !rows[0].singleton || rows[0].generation !== "0")
        throw new ApplicationImportError("target_not_empty", table.name);
      if (plan.snapshot.source === "postgres")
        await transaction.query("DELETE FROM omniroute.api_key_policy_epoch");
      continue;
    }
    if (table.name === "files" || fileContentTables.has(table.name)) {
      const occupied = await transaction.query(
        `SELECT 1 FROM omniroute.${importIdentifier(table.name)} LIMIT 1`
      );
      if (occupied.rows.length) throw new ApplicationImportError("target_not_empty", table.name);
      continue;
    }
    const { rows } = await transaction.query<Record<string, unknown>>(
      `SELECT * FROM omniroute.${importIdentifier(table.name)} LIMIT 7`
    );
    if (rows.some((row) => !pristineSeed(table.name, row)) || rows.length > 5)
      throw new ApplicationImportError("target_not_empty", table.name);
    if (rows.length && plan.tables.some((value) => value.name === table.name))
      await transaction.query(`DELETE FROM omniroute.${importIdentifier(table.name)}`);
  }
}
async function readSnapshotFileBytes(content: StoredFileContent): Promise<Buffer> {
  const size = content.sizeBytes;
  if (size === null || size > APPLICATION_SNAPSHOT_LIMITS.blobBytes)
    throw new ApplicationImportError("limits", "files", "content");
  if (size === 0) return Buffer.alloc(0);
  const bytes = await content.readChunk(0);
  if (!Buffer.isBuffer(bytes) || bytes.byteLength !== size)
    throw new ApplicationImportError("verification", "files", "content");
  return bytes;
}

async function verifyPlan(transaction: PostgresTransaction, plan: ImportPlan): Promise<void> {
  await getStoredFileContentBounds(transaction);
  for (const table of plan.tables) {
    const count = (
      await transaction.query<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM omniroute.${importIdentifier(table.name)}`
      )
    ).rows[0];
    if (BigInt(count?.count ?? "-1") !== BigInt(table.rows.length))
      throw new ApplicationImportError("verification", table.name);
    const fileContent = table.name === "files";
    const from = `FROM omniroute.${importIdentifier(table.name)} ORDER BY ${table.order.map(importIdentifier).join(",")}`;
    const projection = table.columns
      .map((column) =>
        fileContent && column.name === "content"
          ? `NULL AS "content"`
          : importIdentifier(column.name)
      )
      .join(",");
    for (let start = 0; start < table.rows.length; start += APPLICATION_SNAPSHOT_LIMITS.chunkRows) {
      const { rows } = await transaction.query<Record<string, unknown>>(
        `SELECT ${projection} ${from} LIMIT $1 OFFSET $2`,
        [APPLICATION_SNAPSHOT_LIMITS.chunkRows, start]
      );
      if (
        rows.length !== Math.min(APPLICATION_SNAPSHOT_LIMITS.chunkRows, table.rows.length - start)
      )
        throw new ApplicationImportError("verification", table.name);
      for (let index = 0; index < rows.length; index++) {
        const actual: unknown[] = [];
        for (let at = 0; at < table.columns.length; at++) {
          const column = table.columns[at],
            expected = table.rows[start + index][at];
          let stored = rows[index][column.name];
          if (fileContent && column.name === "content") {
            const id = rows[index].id;
            if (typeof id !== "string") throw new ApplicationImportError("verification", "files");
            const content = await openStoredFileContent(transaction, id);
            if (!content) throw new ApplicationImportError("verification", "files");
            if (expected && typeof expected === "object" && expected.kind === "blob-reference") {
              if (
                content.sizeBytes !== expected.sizeBytes ||
                (content.checksum !== null && content.checksum !== expected.checksum)
              )
                throw new ApplicationImportError("verification", table.name, column.name);
              await verifyNativeBlob(expected, {
                readChunk: (_id, chunk) => content.readChunk(chunk),
              });
              actual.push(expected);
              continue;
            }
            if (expected === null) {
              if (content.sizeBytes !== null)
                throw new ApplicationImportError("verification", table.name, column.name);
              actual.push(null);
              continue;
            }
            if (content.sizeBytes === null)
              throw new ApplicationImportError("verification", table.name, column.name);
            stored = await readSnapshotFileBytes(content);
          }
          const value = stored instanceof Date ? stored.toISOString() : stored;
          actual.push(
            integerTypes.has(column.type) && value !== null
              ? { kind: "integer", value: String(value) }
              : column.type === "bytea" && value instanceof Uint8Array
                ? { kind: "bytes", value: Buffer.from(value).toString("base64") }
                : column.type === "real[]" && Array.isArray(value)
                  ? { kind: "vector", value }
                  : value
          );
        }
        if (canonical(actual) !== canonical(table.rows[start + index]))
          throw new ApplicationImportError("verification", table.name);
      }
    }
  }
}
function freshRestoreAuthority(snapshot: ApplicationStorageSnapshot): RestoreAuthority | undefined {
  if (snapshot.source !== "postgres") return undefined;
  const table = snapshot.tables.find((value) => value.name === "api_key_policy_epoch");
  if (!table || table.rows.length !== 1)
    throw new ApplicationImportError("schema", "api_key_policy_epoch");
  const authority = table.rows[0].values[table.columns.indexOf("authority_id")],
    generation = table.rows[0].values[table.columns.indexOf("generation")];
  if (
    typeof authority !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(authority) ||
    !generation ||
    typeof generation !== "object" ||
    generation.kind !== "integer" ||
    BigInt(generation.value) < 0n ||
    BigInt(generation.value) > 9223372036854775807n
  )
    throw new ApplicationImportError("schema", table.name);
  return {
    sourceAuthority: authority,
    sourceGeneration: generation.value,
    targetAuthority: randomUUID(),
    targetGeneration: "0",
  };
}
function blobReferences(snapshot: ApplicationStorageSnapshot): NativeBlobReference[] {
  const references = new Map<string, NativeBlobReference>();
  for (const table of snapshot.tables)
    for (const row of table.rows)
      for (const cell of row.values)
        if (cell && typeof cell === "object" && cell.kind === "blob-reference")
          references.set(cell.id, cell);
  return [...references.values()];
}
async function lockedSchema(transaction: PostgresTransaction): Promise<TargetSchema> {
  await transaction.query("SELECT pg_advisory_xact_lock(18765, 1)");
  await transaction.query("SELECT pg_advisory_xact_lock(18765, 31)");
  const schema = await readTargetSchema(transaction);
  // Offline import excludes writers for every known durable target, including initial seed rows.
  await transaction.query(
    `LOCK TABLE ${schema.tables.map((table) => `omniroute.${importIdentifier(table.name)}`).join(",")} IN ACCESS EXCLUSIVE MODE`
  );
  return schema;
}

/** Verify the explicit target key can read existing ciphertext without changing stored bytes. */
export function validateApplicationSnapshotCredentials(input: unknown): void {
  requireExplicitStorageEncryptionKey();
  const snapshot = checkedApplicationSnapshot(input);
  const protectedFields = new Set([
    "api_key",
    "api_key_encrypted",
    "access_token",
    "refresh_token",
    "id_token",
    "encrypted_api_key",
    "control_secret_enc",
    "supporter_key_encrypted",
  ]);
  const check = (value: unknown, table: string, column: string) => {
    if (
      typeof value === "string" &&
      value.startsWith("enc:v1:") &&
      decrypt(value, { quiet: true }) === null
    )
      throw new ApplicationImportError("value", table, column);
  };
  for (const table of snapshot.tables)
    for (const row of table.rows) {
      for (let index = 0; index < table.columns.length; index++)
        if (protectedFields.has(table.columns[index]))
          check(row.values[index], table.name, table.columns[index]);
      if (table.name === "key_value") {
        const namespace = row.values[table.columns.indexOf("namespace")],
          key = row.values[table.columns.indexOf("key")],
          raw = row.values[table.columns.indexOf("value")];
        if (namespace === "secrets" || (namespace === "settings" && key === "oidcClientSecret")) {
          let value: unknown;
          try {
            value = typeof raw === "string" ? JSON.parse(raw) : undefined;
          } catch {
            if (namespace === "secrets")
              throw new ApplicationImportError("value", table.name, "value");
            continue;
          }
          if (namespace === "secrets" && (typeof value !== "string" || !value.trim()))
            throw new ApplicationImportError("value", table.name, "value");
          check(value, table.name, "value");
        }
      }
    }
}

/** One acknowledged COMMIT; a receipt is reconciled and all rows rechecked on an explicit rerun. */
export async function importApplicationSnapshot(
  database: PostgresDatabase,
  input: unknown,
  artifacts: OfflineArtifactMapping[] = [],
  store?: Pick<NativeArtifactStore, "get" | "storageScope">,
  blobs?: ApplicationBlobImportOptions
): Promise<ApplicationImportReceipt> {
  validateApplicationSnapshotCredentials(input);
  const snapshot = checkedApplicationSnapshot(input);
  const hasArtifacts = snapshot.tables.some(
    (table) =>
      ["call_logs", "call_logs_v1_legacy"].includes(table.name) &&
      table.columns.includes("artifact_relpath") &&
      table.rows.some(
        (row) =>
          typeof row.values[table.columns.indexOf("artifact_relpath")] === "string" &&
          row.values[table.columns.indexOf("artifact_relpath")] !== ""
      )
  );
  if (hasArtifacts && !store) throw new ApplicationImportError("artifact");
  if (store) {
    assertApplicationArtifactPublicationScope(snapshot, store);
    await verifyApplicationArtifacts(artifacts, store);
  }
  const references = blobReferences(snapshot);
  if (
    references.length &&
    (!blobs ||
      !Number.isSafeInteger(blobs.memoryBudgetBytes) ||
      blobs.memoryBudgetBytes < NATIVE_BLOB_LIMITS.importMemoryBudgetBytes)
  )
    throw new ApplicationImportError("limits", "files", "content");
  if (blobs) for (const reference of references) await verifyNativeBlob(reference, blobs.source);
  const receipt = await database.transaction(async (transaction) => {
    const schema = await lockedSchema(transaction),
      sourceDigest = applicationSnapshotDigest(snapshot);
    const { rows: previous } = await transaction.query<{ payload: ApplicationImportReceipt }>(
      "SELECT payload FROM omniroute.application_import_receipts WHERE source_digest=$1",
      [sourceDigest]
    );
    if (previous.length) {
      const receipt = previous[0].payload;
      if (receipt.digestAlgorithm !== APPLICATION_IMPORT_DIGEST_ALGORITHM)
        throw new ApplicationImportError("different_import");
      const plan = buildPlan(snapshot, schema, receipt.artifacts, receipt.restoreAuthority);
      if (receipt.planDigest !== plan.planDigest || receipt.schemaDigest !== schema.digest)
        throw new ApplicationImportError("different_import");
      await verifyPlan(transaction, plan);
      return { ...receipt, alreadyImported: true };
    }
    const occupied = await transaction.query(
      "SELECT source_digest FROM omniroute.application_import_receipts LIMIT 1"
    );
    if (occupied.rows.length) throw new ApplicationImportError("different_import");
    const old = await transaction.query("SELECT digest FROM omniroute.routing_imports LIMIT 1");
    if (old.rows.length) throw new ApplicationImportError("target_not_empty", "routing_imports");
    const plan = buildPlan(snapshot, schema, artifacts, freshRestoreAuthority(snapshot));
    await assertEmptyTarget(transaction, schema, plan);
    for (const table of insertionOrder(plan, schema)) {
      const carried = table.rows.some((row) =>
        row.some((cell) => cell && typeof cell === "object" && cell.kind === "blob-reference")
      );
      const fileContentAt =
        table.name === "files"
          ? table.columns.findIndex((column) => column.name === "content")
          : -1;
      const fileIdAt =
        table.name === "files" ? table.columns.findIndex((column) => column.name === "id") : -1;
      const batchSize = carried ? 1 : APPLICATION_SNAPSHOT_LIMITS.chunkRows;
      for (let start = 0; start < table.rows.length; start += batchSize) {
        const rows = table.rows.slice(start, start + batchSize),
          values: unknown[] = [];
        for (const row of rows)
          for (let column = 0; column < row.length; column++)
            values.push(column === fileContentAt ? null : bind(row[column]));
        await transaction.query(
          `INSERT INTO omniroute.${importIdentifier(table.name)} (${table.columns.map((column) => importIdentifier(column.name)).join(",")}) OVERRIDING SYSTEM VALUE VALUES ${rows.map((_, at) => `(${table.columns.map((_, column) => `$${at * table.columns.length + column + 1}`).join(",")})`).join(",")}`,
          values
        );
        if (fileContentAt >= 0) {
          for (const row of rows) {
            const id = row[fileIdAt],
              cell = row[fileContentAt];
            if (typeof id !== "string") throw new ApplicationImportError("schema", "files", "id");
            const content =
              cell && typeof cell === "object" && cell.kind === "blob-reference"
                ? cell
                : cell && typeof cell === "object" && cell.kind === "bytes"
                  ? Buffer.from(cell.value, "base64")
                  : null;
            await replaceStoredFileContent(transaction, id, content, blobs?.source);
          }
        }
      }
      for (const column of table.columns.filter((column) => column.identity)) {
        const sequence = await transaction.query<{ name: string }>(
          "SELECT pg_get_serial_sequence($1,$2) AS name",
          [`omniroute.${table.name}`, column.name]
        );
        if (!sequence.rows[0]?.name)
          throw new ApplicationImportError("schema", table.name, column.name);
        const high = (
          await transaction.query<{ maximum: string | null }>(
            `SELECT MAX(${importIdentifier(column.name)})::text AS maximum FROM omniroute.${importIdentifier(table.name)}`
          )
        ).rows[0]?.maximum;
        const sourceSequence = snapshot.metadataTables.find(
          (value) => value.name === "sqlite_sequence"
        );
        const original = sourceSequence?.rows.find(
          (row) => row.values[sourceSequence.columns.indexOf("name")] === table.name
        )?.values[sourceSequence.columns.indexOf("seq")];
        const prior =
          original && typeof original === "object" && original.kind === "integer"
            ? BigInt(original.value)
            : 0n;
        const nativeState = snapshot.metadataTables.find(
          (table) => table.name === "postgres_sequence_state"
        );
        const nativeRow = nativeState?.rows.find(
          (row) => row.values[0] === table.name && row.values[1] === column.name
        );
        const native = nativeRow?.values[2];
        const nativeMaximum =
          native && typeof native === "object" && native.kind === "integer"
            ? BigInt(native.value) - (nativeRow.values[3] === false ? 1n : 0n)
            : 0n;
        const maximum = [BigInt(high ?? "0"), prior, nativeMaximum, 0n].reduce((a, b) =>
          a > b ? a : b
        );
        if (maximum >= 9223372036854775807n)
          throw new ApplicationImportError("limits", table.name, column.name);
        const qualified = sequence.rows[0].name.split(".").map(importIdentifier).join(".");
        // RESTART is transactional; setval would survive rollback and produce a partial restore.
        await transaction.query(`ALTER SEQUENCE ${qualified} RESTART WITH ${maximum + 1n}`);
      }
    }
    await verifyPlan(transaction, plan);
    const tableCounts = Object.fromEntries(
      plan.tables.map((table) => [table.name, table.rows.length])
    );
    const receipt: ApplicationImportReceipt = {
      format: "omniroute-application-import-receipt-v1",
      digestAlgorithm: APPLICATION_IMPORT_DIGEST_ALGORITHM,
      sourceDigest: plan.sourceDigest,
      planDigest: plan.planDigest,
      schemaDigest: schema.digest,
      rowCount: Object.values(tableCounts).reduce((sum, value) => sum + value, 0),
      tableCounts,
      artifacts: plan.artifacts,
      transformations: plan.transformations,
      alreadyImported: false,
      restoreAuthority: plan.restoreAuthority,
    };
    await transaction.query(
      "INSERT INTO omniroute.application_import_receipts(source_digest,plan_digest,schema_digest,row_count,payload) VALUES($1,$2,$3,$4,$5::jsonb)",
      [
        receipt.sourceDigest,
        receipt.planDigest,
        receipt.schemaDigest,
        receipt.rowCount,
        JSON.stringify({
          ...receipt,
          sourceMetadata: snapshot.metadataTables,
          sourceDefinitions: snapshot.definitions,
          excluded: snapshot.excluded,
        }),
      ]
    );
    return receipt;
  });
  if (store) {
    try {
      await verifyApplicationArtifacts(receipt.artifacts, store);
    } catch {
      throw new ApplicationImportEffectsError();
    }
  }
  return receipt;
}

export class ApplicationImportEffectsError extends Error {
  readonly committed = true;
  readonly failedEffect = "artifact_readback";
  constructor() {
    super("Application storage import committed; artifact readback failed");
    this.name = "ApplicationImportEffectsError";
  }
}

/** Read-only verification rejects data drift; it never repairs rows or repeats import writes. */
export async function verifyApplicationImport(
  database: PostgresDatabase,
  input: unknown
): Promise<ApplicationImportReceipt> {
  const snapshot = checkedApplicationSnapshot(input);
  return database.transaction(async (transaction) => {
    const schema = await lockedSchema(transaction),
      sourceDigest = applicationSnapshotDigest(snapshot);
    const { rows } = await transaction.query<{ payload: ApplicationImportReceipt }>(
      "SELECT payload FROM omniroute.application_import_receipts WHERE source_digest=$1",
      [sourceDigest]
    );
    if (rows.length !== 1) throw new ApplicationImportError("verification");
    const receipt = rows[0].payload;
    if (receipt.digestAlgorithm !== APPLICATION_IMPORT_DIGEST_ALGORITHM)
      throw new ApplicationImportError("different_import");
    const plan = buildPlan(snapshot, schema, receipt.artifacts, receipt.restoreAuthority);
    if (receipt.planDigest !== plan.planDigest) throw new ApplicationImportError("verification");
    await verifyPlan(transaction, plan);
    return { ...receipt, alreadyImported: true };
  });
}

/** Logical offline restore source. PostgreSQL generated index/search columns are regenerated. */
export async function capturePostgresApplicationSnapshot(
  database: PostgresDatabase,
  options: { blobSink?: NativeBlobSink } = {}
): Promise<ApplicationStorageSnapshot> {
  return database.transaction(async (transaction) => {
    const schema = await lockedSchema(transaction);
    const snapshot: ApplicationStorageSnapshot = {
      format: "omniroute-application-storage-v1",
      source: "postgres",
      tables: [],
      metadataTables: [],
      definitions: [],
      excluded: [],
    };
    const budget = { rows: 0, bytes: 0 };
    const sequences: ApplicationSnapshotTable = {
      name: "postgres_sequence_state",
      columns: ["table_name", "column_name", "last_value", "is_called"],
      rowOrder: "native",
      rows: [],
    };
    for (const table of schema.tables) {
      if (fileContentTables.has(table.name)) {
        snapshot.excluded.push({
          name: table.name,
          reason: "native file representation verified and reconstructed as logical files.content",
        });
        continue;
      }
      if (controlTables.has(table.name)) {
        const bound = (
          await transaction.query<{ count: string; bytes: string }>(
            `SELECT COUNT(*)::text AS count,COALESCE(SUM(6*octet_length(to_jsonb(t)::text)+64),0)::text AS bytes FROM omniroute.${importIdentifier(table.name)} t`
          )
        ).rows[0];
        if (
          BigInt(bound.count) > BigInt(APPLICATION_SNAPSHOT_LIMITS.rows - budget.rows) ||
          BigInt(bound.bytes) > BigInt(APPLICATION_SNAPSHOT_LIMITS.bytes - budget.bytes)
        )
          throw new ApplicationImportError("limits", table.name);
        budget.rows += Number(bound.count);
        budget.bytes += Number(bound.bytes);
        const control: Record<string, unknown>[] = [];
        for (let start = 0; ; start += APPLICATION_SNAPSHOT_LIMITS.chunkRows) {
          const batch = await transaction.query<Record<string, unknown>>(
            `SELECT * FROM omniroute.${importIdentifier(table.name)} ORDER BY ${table.primary.map(importIdentifier).join(",")} LIMIT $1 OFFSET $2`,
            [APPLICATION_SNAPSHOT_LIMITS.chunkRows, start]
          );
          control.push(...batch.rows);
          if (batch.rows.length < APPLICATION_SNAPSHOT_LIMITS.chunkRows) break;
        }
        snapshot.metadataTables.push({
          name: table.name,
          columns: ["record_json"],
          rowOrder: "native",
          rows: control.map((row) => ({ rowid: null, values: [JSON.stringify(row)] })),
        });
        snapshot.excluded.push({
          name: table.name,
          reason: "native migration/import control lineage preserved as source metadata",
        });
        continue;
      }
      for (const column of table.columns.filter((column) => column.identity)) {
        const sequence = (
          await transaction.query<{ name: string }>(
            "SELECT pg_get_serial_sequence($1,$2) AS name",
            [`omniroute.${table.name}`, column.name]
          )
        ).rows[0]?.name;
        if (!sequence) throw new ApplicationImportError("schema", table.name, column.name);
        const state = (
          await transaction.query<{ last_value: string; is_called: boolean }>(
            `SELECT last_value::text AS last_value,is_called FROM ${sequence.split(".").map(importIdentifier).join(".")}`
          )
        ).rows[0];
        sequences.rows.push({
          rowid: null,
          values: [
            table.name,
            column.name,
            { kind: "integer", value: state.last_value },
            state.is_called,
          ],
        });
      }
      const columns = table.columns.filter((column) => !column.generated),
        order = columnOrder(table),
        rows: ApplicationSnapshotTable["rows"] = [];
      const fileContent = (column: TargetColumn) =>
        table.name === "files" && column.name === "content";
      const lengths = columns.map((column) =>
        fileContent(column)
          ? "0"
          : column.type === "bytea"
            ? `4*((COALESCE(octet_length(${importIdentifier(column.name)}),0)+2)/3)`
            : `6*COALESCE(octet_length(${importIdentifier(column.name)}::text),0)`
      );
      const blobs = columns
        .filter((column) => column.type === "bytea" && !fileContent(column))
        .map((column) => `COALESCE(octet_length(${importIdentifier(column.name)}),0)`);
      const bound = (
        await transaction.query<{ count: string; bytes: string; blob: number }>(
          `SELECT COUNT(*)::text AS count,COALESCE(SUM(${lengths.join("+")}+${64 + columns.length * 64}),0)::text AS bytes,COALESCE(MAX(GREATEST(0${blobs.length ? "," + blobs.join(",") : ""})),0) AS blob FROM omniroute.${importIdentifier(table.name)}`
        )
      ).rows[0];
      if (
        BigInt(bound.count) > BigInt(APPLICATION_SNAPSHOT_LIMITS.rows - budget.rows) ||
        BigInt(bound.bytes) > BigInt(APPLICATION_SNAPSHOT_LIMITS.bytes - budget.bytes) ||
        bound.blob > APPLICATION_SNAPSHOT_LIMITS.blobBytes
      )
        throw new ApplicationImportError("limits", table.name);
      budget.rows += Number(bound.count);
      budget.bytes += Number(bound.bytes);
      if (table.name === "files") {
        const contentBounds = await getStoredFileContentBounds(transaction);
        if (
          contentBounds.maximum >
            (options.blobSink
              ? NATIVE_BLOB_LIMITS.fileBytes
              : APPLICATION_SNAPSHOT_LIMITS.blobBytes) ||
          (options.blobSink &&
            BigInt(contentBounds.total) > BigInt(NATIVE_BLOB_LIMITS.carrierBytes))
        )
          throw new ApplicationImportError("limits", "files", "content");
      }
      for (let start = 0; ; start += APPLICATION_SNAPSHOT_LIMITS.chunkRows) {
        const result = await transaction.query<Record<string, unknown>>(
          `SELECT ${columns.map((column) => (fileContent(column) ? `NULL AS ${importIdentifier(column.name)}` : importIdentifier(column.name))).join(",")} FROM omniroute.${importIdentifier(table.name)} ORDER BY ${order.map(importIdentifier).join(",")} LIMIT $1 OFFSET $2`,
          [APPLICATION_SNAPSHOT_LIMITS.chunkRows, start]
        );
        for (let index = 0; index < result.rows.length; index++) {
          const row = result.rows[index],
            cells: ApplicationImportCell[] = [];
          for (const column of columns) {
            if (fileContent(column)) {
              if (typeof row.id !== "string")
                throw new ApplicationImportError("schema", "files", "id");
              const content = await openStoredFileContent(transaction, row.id);
              if (!content) throw new ApplicationImportError("verification", "files");
              if (content.sizeBytes === null) {
                cells.push(null);
              } else {
                budget.bytes += options.blobSink ? 16384 : 4 * Math.ceil(content.sizeBytes / 3);
                if (budget.bytes > APPLICATION_SNAPSHOT_LIMITS.bytes)
                  throw new ApplicationImportError("limits", "files", "content");
                if (options.blobSink) {
                  const reference = await captureNativeBlobAsync(
                    content.sizeBytes,
                    (offset) => content.readChunk(offset / NATIVE_BLOB_LIMITS.chunkBytes),
                    options.blobSink
                  );
                  if (content.checksum !== null && content.checksum !== reference.checksum)
                    throw new ApplicationImportError("verification", "files", "content");
                  cells.push(reference);
                } else {
                  const bytes = await readSnapshotFileBytes(content);
                  cells.push({ kind: "bytes", value: bytes.toString("base64") });
                }
              }
              continue;
            }
            cells.push(
              (() => {
                const stored = row[column.name];
                const value = stored instanceof Date ? stored.toISOString() : stored;
                if (integerTypes.has(column.type) && value !== null)
                  return { kind: "integer", value: String(value) };
                if (column.type === "bytea" && value instanceof Uint8Array)
                  return { kind: "bytes", value: Buffer.from(value).toString("base64") };
                if (column.type === "real[]" && Array.isArray(value))
                  return { kind: "vector", value };
                return checkedCell(value as ApplicationImportCell, column, table.name);
              })()
            );
          }
          rows.push({ rowid: null, values: cells });
        }
        if (result.rows.length < APPLICATION_SNAPSHOT_LIMITS.chunkRows) break;
        if (rows.length > APPLICATION_SNAPSHOT_LIMITS.rows)
          throw new ApplicationImportError("limits");
      }
      snapshot.tables.push({
        name: table.name,
        columns: columns.map((column) => column.name),
        rowOrder: "native",
        rows,
      });
    }
    snapshot.metadataTables.push(sequences);
    return checkedApplicationSnapshot(snapshot);
  });
}

/** A preliminary read lets the CLI reconcile receipt/object references before any new upload. */
export async function readApplicationImportReceipt(
  database: PostgresDatabase,
  input: unknown
): Promise<ApplicationImportReceipt | undefined> {
  const sourceDigest = applicationSnapshotDigest(input);
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<{ payload: ApplicationImportReceipt }>(
      "SELECT payload FROM omniroute.application_import_receipts WHERE source_digest=$1",
      [sourceDigest]
    );
    return rows[0]?.payload;
  });
}
