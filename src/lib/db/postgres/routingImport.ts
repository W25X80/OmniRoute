import { createHash } from "node:crypto";
import { z } from "zod";
import type { SqliteAdapter } from "../adapters/types";
import type { PostgresDatabase } from "./database";
import { acquirePostgresLock } from "./locks";

// This manifest is the declared routing slice, not a complete OmniRoute backup.
const tables = {
  combos: [
    "id",
    "name",
    "data",
    "sort_order",
    "created_at",
    "updated_at",
    "system_message",
    "tool_filter_regex",
    "context_cache_protection",
  ],
  model_combo_mappings: [
    "id",
    "pattern",
    "combo_id",
    "priority",
    "enabled",
    "description",
    "created_at",
    "updated_at",
  ],
  key_value: ["namespace", "key", "value"],
  proxy_registry: [
    "id",
    "name",
    "type",
    "host",
    "port",
    "username",
    "password",
    "region",
    "notes",
    "status",
    "created_at",
    "updated_at",
    "source",
    "quality_score",
    "latency_ms",
    "anonymity",
    "google_access",
    "last_validated",
    "country_code",
    "family",
    "subscription_id",
  ],
  proxy_assignments: [
    "id",
    "proxy_id",
    "scope",
    "scope_id",
    "position",
    "created_at",
    "updated_at",
  ],
  proxy_scope_rotation: [
    "scope",
    "scope_id",
    "strategy",
    "cursor",
    "sticky_window_minutes",
    "rotated_at",
    "updated_at",
  ],
} as const;

type TableName = keyof typeof tables;
const tableNames = Object.keys(tables) as TableName[];
const scalar = z.union([
  z.string(),
  z.number().finite().min(-Number.MAX_SAFE_INTEGER).max(Number.MAX_SAFE_INTEGER),
  z.null(),
]);
const row = z.record(z.string(), scalar);
const snapshotSchema = z
  .object({
    format: z.literal("omniroute-routing-v1"),
    proxyAssignmentSequence: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    tables: z
      .object({
        combos: z.array(row),
        model_combo_mappings: z.array(row),
        key_value: z.array(row),
        proxy_registry: z.array(row),
        proxy_assignments: z.array(row),
        proxy_scope_rotation: z.array(row),
      })
      .strict(),
  })
  .strict();

export type RoutingSnapshot = z.infer<typeof snapshotSchema>;
const MAX_ROWS = 50_000;
const MAX_BYTES = 32 * 1024 * 1024;

function checkedSnapshot(input: unknown): RoutingSnapshot {
  const parsed = snapshotSchema.safeParse(input);
  if (!parsed.success) throw new Error("Invalid routing snapshot");
  let count = 0;
  for (const name of tableNames) {
    count += parsed.data.tables[name].length;
    for (const record of parsed.data.tables[name]) {
      const columns: readonly string[] = tables[name];
      if (
        Object.keys(record).length !== columns.length ||
        Object.keys(record).some((key) => !columns.includes(key))
      ) {
        throw new Error("Routing snapshot column mismatch");
      }
    }
  }
  if (count > MAX_ROWS || Buffer.byteLength(JSON.stringify(parsed.data)) > MAX_BYTES) {
    throw new Error("Routing snapshot exceeds the import limit");
  }
  if (
    parsed.data.tables.proxy_assignments.some(
      (record) =>
        typeof record.id !== "number" ||
        !Number.isSafeInteger(record.id) ||
        record.id > parsed.data.proxyAssignmentSequence
    )
  ) {
    throw new Error("Routing snapshot identity sequence mismatch");
  }
  return parsed.data;
}

/**
 * Offline only. The caller supplies a read-only SQLite handle to an existing database.
 * Capture a consistent source snapshot before starting network work; never hold a SQLite
 * transaction across an await. Do not use getDbInstance(), startup migrations or repair here.
 */
export function captureRoutingSnapshot(source: SqliteAdapter): RoutingSnapshot {
  return source.transaction(() => {
    const data = {} as RoutingSnapshot["tables"];
    for (const name of tableNames) {
      const actual = source.pragma(`table_info(${name})`) as { name: string }[];
      const expected: readonly string[] = tables[name];
      if (
        actual.length !== expected.length ||
        actual.some((column) => !expected.includes(column.name))
      ) {
        throw new Error("SQLite routing schema does not match the import manifest");
      }
      // SQLite rowid orders equal sort/priority ties; preserve source insertion order.
      data[name] = source
        .prepare(`SELECT ${expected.join(",")} FROM ${name} ORDER BY rowid LIMIT ?`)
        .all(MAX_ROWS + 1) as RoutingSnapshot["tables"][TableName];
    }
    const sequence = source
      .prepare("SELECT seq FROM sqlite_sequence WHERE name = 'proxy_assignments'")
      .get() as { seq: number } | undefined;
    return checkedSnapshot({
      format: "omniroute-routing-v1",
      tables: data,
      proxyAssignmentSequence: sequence?.seq ?? 0,
    });
  })();
}

function digest(snapshot: RoutingSnapshot): string {
  // Fixed table and column order makes object-key order irrelevant without changing row order.
  return createHash("sha256")
    .update(
      JSON.stringify([
        snapshot.proxyAssignmentSequence,
        tableNames.map((name) => [
          name,
          snapshot.tables[name].map((record) => tables[name].map((column) => record[column])),
        ]),
      ])
    )
    .digest("hex");
}

export interface RoutingImportReceipt {
  digest: string;
  rowCount: number;
  alreadyImported: boolean;
}

/** Atomic import into an empty routing target. A matching receipt reconciles a lost COMMIT reply. */
export async function importRoutingSnapshot(
  database: PostgresDatabase,
  input: unknown
): Promise<RoutingImportReceipt> {
  const snapshot = checkedSnapshot(input);
  const sourceDigest = digest(snapshot);
  const rowCount = tableNames.reduce((sum, name) => sum + snapshot.tables[name].length, 0);
  return database.transaction(async (transaction) => {
    await acquirePostgresLock(transaction, "routing");
    const { rows: previous } = await transaction.query<{ digest: string; row_count: number }>(
      "SELECT digest,row_count FROM omniroute.routing_imports"
    );
    if (previous.length > 0) {
      if (
        previous.length !== 1 ||
        previous[0].digest !== sourceDigest ||
        previous[0].row_count !== rowCount
      ) {
        throw new Error("PostgreSQL routing target contains a different import");
      }
      return { digest: sourceDigest, rowCount, alreadyImported: true };
    }
    for (const name of tableNames) {
      const { rows } = await transaction.query<{ occupied: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM omniroute.${name}) AS occupied`
      );
      if (rows[0].occupied) throw new Error("PostgreSQL routing import requires an empty target");
    }
    // Parent rows precede mappings and assignments. Names/columns come only from the static manifest.
    const order: TableName[] = [
      "combos",
      "key_value",
      "proxy_registry",
      "model_combo_mappings",
      "proxy_assignments",
      "proxy_scope_rotation",
    ];
    for (const name of order) {
      const columns = tables[name];
      for (let start = 0; start < snapshot.tables[name].length; start += 100) {
        const batch = snapshot.tables[name].slice(start, start + 100);
        await transaction.query(
          `INSERT INTO omniroute.${name} (${columns.join(",")}) VALUES ${batch
            .map(
              (_, row) =>
                `(${columns.map((_, column) => `$${row * columns.length + column + 1}`).join(",")})`
            )
            .join(",")}`,
          batch.flatMap((record) => columns.map((column) => record[column]))
        );
      }
    }
    // Imported assignment ids must not collide with the next generated identity.
    await transaction.query(
      `SELECT setval(pg_get_serial_sequence('omniroute.proxy_assignments','id'),
      GREATEST($1::bigint,1),$1::bigint > 0)`,
      [snapshot.proxyAssignmentSequence]
    );
    await transaction.query(
      "INSERT INTO omniroute.routing_imports(digest,row_count) VALUES($1,$2)",
      [sourceDigest, rowCount]
    );
    return { digest: sourceDigest, rowCount, alreadyImported: false };
  });
}
