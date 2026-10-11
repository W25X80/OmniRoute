import {
  APPLICATION_IMPORT_TABLES,
  APPLICATION_IMPORT_NATIVE_ONLY,
} from "../../../src/lib/db/postgres/applicationImportManifest.ts";
import {
  captureNativeBlob,
  NATIVE_BLOB_LIMITS,
  NativeBlobCarrierError,
} from "../../../src/lib/db/postgres/nativeBlobCarrier.ts";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import test, { after } from "node:test";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../../src/lib/db/adapters/betterSqliteAdapter.ts";
import {
  ApplicationImportError,
  captureSqliteApplicationSnapshot,
  applicationSnapshotDigest,
  checkedApplicationSnapshot,
  type ApplicationStorageSnapshot,
} from "../../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  importApplicationSnapshot,
  verifyApplicationImport,
} from "../../../src/lib/db/postgres/applicationImport.ts";
import {
  prepareApplicationArtifacts,
  verifyApplicationArtifacts,
} from "../../../src/lib/db/postgres/applicationImportArtifacts.ts";
import {
  PostgresDatabase,
  type PostgresClient,
  type PostgresResult,
} from "../../../src/lib/db/postgres/database.ts";

const priorEncryption = process.env.STORAGE_ENCRYPTION_KEY;
after(() => {
  if (priorEncryption === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
  else process.env.STORAGE_ENCRYPTION_KEY = priorEncryption;
});

function source() {
  const raw = new Database(":memory:");
  const adapter = createBetterSqliteAdapter(raw);
  adapter.exec(
    "CREATE TABLE key_value(namespace TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(namespace,key)); CREATE TABLE db_meta(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE _omniroute_migrations(version TEXT PRIMARY KEY,name TEXT)"
  );
  return { raw, adapter };
}
const kind = (value: string) => (error: unknown) =>
  error instanceof ApplicationImportError && error.kind === value;
function snapshot(): ApplicationStorageSnapshot {
  return {
    format: "omniroute-application-storage-v1",
    source: "sqlite",
    tables: [
      {
        name: "key_value",
        columns: ["namespace", "key", "value"],
        rowOrder: "rowid",
        rows: [
          { rowid: "2", values: ["settings", "z", "opaque-ciphertext"] },
          { rowid: "3", values: ["settings", "a", '"default"'] },
        ],
      },
    ],
    metadataTables: [],
    definitions: [],
    excluded: [],
  };
}

// A JSONB roundtrip may reorder every object while preserving arrays and values.
function reorderedJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reorderedJson);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => right.localeCompare(left))
        .map(([key, nested]) => [key, reorderedJson(nested)])
    );
  return value;
}

function fixture(initial: Record<string, unknown>[] = []) {
  const calls: { text: string; values?: unknown[] }[] = [],
    rows = [...initial];
  let epoch: Record<string, unknown> | undefined = {
    singleton: true,
    authority_id: "91ae17e6-f411-4c61-8a17-7538d11fb860",
    generation: "0",
  };
  let receipt: Record<string, unknown> | undefined;
  const events = new EventEmitter();
  const columns = [
    ...new Set([...APPLICATION_IMPORT_TABLES, ...APPLICATION_IMPORT_NATIVE_ONLY]),
  ].map((name) => ({
    name,
    columns:
      name === "key_value"
        ? ["namespace", "key", "value"]
        : name === "api_key_policy_epoch"
          ? ["singleton", "authority_id", "generation"]
          : ["id"],
  }));
  const answer = (text: string, values?: unknown[]): PostgresResult => {
    let data: Record<string, unknown>[] = [];
    if (text.includes("json_build_object('name'"))
      data = columns.flatMap((table) =>
        table.columns.map((name) => ({
          table: table.name,
          column: {
            name,
            type:
              table.name === "api_key_policy_epoch"
                ? name === "singleton"
                  ? "boolean"
                  : name === "generation"
                    ? "bigint"
                    : "uuid"
                : "text",
            nullable: false,
            defaultExpression: null,
            identity: "",
            generated: "",
          },
        }))
      );
    else if (text.includes('AS "primary"'))
      data = columns.map((table) => ({
        table: table.name,
        primary: table.name === "key_value" ? ["namespace", "key"] : table.columns,
        parents: [],
      }));
    else if (text.startsWith("SELECT singleton,generation")) data = epoch ? [epoch] : [];
    else if (text.startsWith("DELETE FROM omniroute.api_key_policy_epoch")) epoch = undefined;
    else if (text.startsWith('INSERT INTO omniroute."api_key_policy_epoch"'))
      epoch = { singleton: values![0], authority_id: values![1], generation: values![2] };
    else if (
      text.startsWith('SELECT COUNT(*)::text AS count FROM omniroute."api_key_policy_epoch"')
    )
      data = [{ count: epoch ? "1" : "0" }];
    else if (text.startsWith('SELECT "singleton","authority_id","generation"'))
      data = epoch ? [epoch] : [];
    else if (text.startsWith("SELECT payload")) data = receipt ? [{ payload: receipt }] : [];
    else if (text.startsWith("SELECT source_digest"))
      data = receipt ? [{ source_digest: receipt.sourceDigest }] : [];
    else if (text.startsWith('SELECT * FROM omniroute."key_value"')) data = rows.slice(0, 7);
    else if (text.startsWith('INSERT INTO omniroute."key_value"')) {
      for (let i = 0; i < values!.length; i += 3)
        rows.push({ namespace: values![i], key: values![i + 1], value: values![i + 2] });
    } else if (text.startsWith("INSERT INTO omniroute.application_import_receipts"))
      receipt = reorderedJson(JSON.parse(values![4] as string)) as Record<string, unknown>;
    else if (text.startsWith('SELECT COUNT(*)::text AS count FROM omniroute."key_value"'))
      data = [{ count: String(rows.length) }];
    else if (text.startsWith('SELECT "namespace","key","value"'))
      data = [...rows]
        .sort(
          (a, b) =>
            String(a.namespace).localeCompare(String(b.namespace)) ||
            String(a.key).localeCompare(String(b.key))
        )
        .slice(Number(values![1]), Number(values![1]) + Number(values![0]));
    return {
      command: text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK" ? text : "SELECT",
      rows: data,
      rowCount: data.length,
    };
  };
  const client: PostgresClient = {
    async query<Row>(request: { text: string; values?: unknown[]; queryMode: "extended" }) {
      assert.equal(request.queryMode, "extended");
      calls.push(request);
      return answer(request.text, request.values) as PostgresResult<Row>;
    },
    release() {},
    on: events.on.bind(events),
    removeListener: events.removeListener.bind(events),
  };
  const database = new PostgresDatabase(
    {
      async connect() {
        return client;
      },
      async end() {},
      on: events.on.bind(events),
    },
    { maxConnections: 1, transactionTimeoutMs: 500 },
    { warn() {} }
  );
  return { database, calls, rows, getReceipt: () => receipt, getEpoch: () => epoch };
}

test("read-only capture preserves integer strings, BLOB bytes and source schema provenance", () => {
  const { adapter, raw } = source();
  try {
    adapter.exec("CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,content BLOB)");
    raw
      .prepare("INSERT INTO files VALUES(?,?,?)")
      .run("f", 9007199254740993n, Buffer.from([0, 1, 255]));
    adapter.pragma("query_only=ON");
    const result = captureSqliteApplicationSnapshot(adapter),
      file = result.tables.find((table) => table.name === "files")!;
    assert.deepEqual(file.rows[0].values, [
      "f",
      { kind: "integer", value: "9007199254740993" },
      { kind: "bytes", value: "AAH/" },
    ]);
    assert.equal(adapter.inTransaction, false);
    assert.ok(
      result.definitions.some(
        (value) => value.name === "files" && value.sql?.includes("content BLOB")
      )
    );
    assert.equal(
      applicationSnapshotDigest(result),
      applicationSnapshotDigest(checkedApplicationSnapshot(JSON.parse(JSON.stringify(result))))
    );
  } finally {
    adapter.close();
  }
});
test("unknown durable tables fail even when empty; read-write source is rejected", () => {
  const { adapter } = source();
  try {
    assert.throws(() => captureSqliteApplicationSnapshot(adapter), kind("source_not_readonly"));
    adapter.exec("CREATE TABLE unexpected_owner(id TEXT)");
    adapter.pragma("query_only=ON");
    assert.throws(() => captureSqliteApplicationSnapshot(adapter), kind("schema"));
  } finally {
    adapter.close();
  }
});
test("FTS external-content parent and shadow metadata are evidenced exclusions", () => {
  const { adapter } = source();
  try {
    adapter.exec(
      "CREATE TABLE files(id TEXT PRIMARY KEY,content TEXT); CREATE VIRTUAL TABLE generated_search USING fts5(content,content='files')"
    );
    adapter.pragma("query_only=ON");
    const result = captureSqliteApplicationSnapshot(adapter);
    assert.ok(
      result.excluded.some((row) => row.name === "generated_search" && row.reason.includes("files"))
    );
    assert.ok(
      result.excluded.some(
        (row) => row.name === "generated_search_data" && row.reason.includes("shadow")
      )
    );
    assert.ok(result.tables.some((row) => row.name === "files"));
  } finally {
    adapter.close();
  }
});
test("unowned standalone FTS data is not silently discarded", () => {
  const { adapter } = source();
  try {
    adapter.exec("CREATE VIRTUAL TABLE standalone_search USING fts5(content)");
    adapter.pragma("query_only=ON");
    assert.throws(() => captureSqliteApplicationSnapshot(adapter), kind("schema"));
  } finally {
    adapter.close();
  }
});
test("native import is parameterized, verified before COMMIT and receipt reconciliation does not replay writes", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  const f = fixture();
  try {
    const first = await importApplicationSnapshot(f.database, snapshot());
    assert.equal(first.alreadyImported, false);
    assert.equal(first.rowCount, 2);
    const insert = f.calls.find((call) =>
      call.text.startsWith('INSERT INTO omniroute."key_value"')
    )!;
    assert.ok(!insert.text.includes("opaque-ciphertext"));
    assert.equal(insert.values?.length, 6);
    const before = f.calls.filter((call) => call.text.startsWith("INSERT")).length;
    assert.equal((await importApplicationSnapshot(f.database, snapshot())).alreadyImported, true);
    assert.equal(f.calls.filter((call) => call.text.startsWith("INSERT")).length, before);
    assert.equal(
      (await verifyApplicationImport(f.database, snapshot())).sourceDigest,
      first.sourceDigest
    );
    f.rows[0].value = "drift";
    await assert.rejects(verifyApplicationImport(f.database, snapshot()), kind("verification"));
  } finally {
    await f.database.close();
  }
});
test("occupied target and unmapped source columns reject before import DML", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  for (const occupied of [false, true]) {
    const f = fixture(
      occupied ? [{ namespace: "settings", key: "operator", value: "retained" }] : []
    );
    try {
      const input = snapshot();
      if (!occupied) {
        input.tables[0].columns.push("unmapped");
        for (const row of input.tables[0].rows) row.values.push("must-not-lose");
      }
      await assert.rejects(
        importApplicationSnapshot(f.database, input),
        kind(occupied ? "target_not_empty" : "schema")
      );
      assert.ok(f.calls.every((call) => !call.text.startsWith("INSERT")));
      assert.equal(f.calls.at(-1)?.text, "ROLLBACK");
    } finally {
      await f.database.close();
    }
  }
});
function artifactSnapshot(): ApplicationStorageSnapshot {
  const value = snapshot();
  value.tables.push({
    name: "call_logs",
    columns: ["id", "artifact_relpath", "artifact_size_bytes", "artifact_sha256"],
    rowOrder: "rowid",
    rows: [
      { rowid: "1", values: ["one", "legacy/shared.json", null, null] },
      { rowid: "2", values: ["two", "legacy/shared.json", null, null] },
    ],
  });
  return value;
}
test("shared artifact upload is one acknowledged object; unknown put is reconciled by exact readback", async () => {
  const bytes = Buffer.from('{"requestBody":"opaque"}'),
    checksum = createHash("sha256").update(bytes).digest("hex");
  let reads = 0,
    puts = 0,
    checkpoints = 0;
  const target = {
    async put() {
      puts++;
      throw { kind: "put_unknown", reference: "reconciled-reference" };
    },
    async get() {
      reads++;
      return bytes;
    },
  };
  const mappings = await prepareApplicationArtifacts(artifactSnapshot(), {
    async readSource() {
      return bytes;
    },
    target,
    async checkpoint() {
      checkpoints++;
    },
  });
  assert.deepEqual(mappings, [
    {
      sourceReference: "legacy/shared.json",
      targetReference: "reconciled-reference",
      sizeBytes: bytes.byteLength,
      checksum,
    },
  ]);
  assert.equal(puts, 1);
  assert.equal(checkpoints, 1);
  assert.equal(reads, 2);
  await verifyApplicationArtifacts(mappings, target);
  await prepareApplicationArtifacts(
    artifactSnapshot(),
    {
      async readSource() {
        return bytes;
      },
      target,
    },
    mappings
  );
  assert.equal(puts, 1);
});
test("artifact checksum/length mismatch blocks upload and mutated readback blocks qualification", async () => {
  const input = artifactSnapshot();
  input.tables.at(-1)!.rows[0].values[2] = { kind: "integer", value: "999" };
  let puts = 0;
  const target = {
    async put(bytes: Uint8Array) {
      puts++;
      return {
        reference: "ref",
        sizeBytes: bytes.byteLength,
        checksum: createHash("sha256").update(bytes).digest("hex"),
      };
    },
    async get() {
      return Buffer.from("corrupt");
    },
  };
  await assert.rejects(
    prepareApplicationArtifacts(input, {
      async readSource() {
        return Buffer.from("body");
      },
      target,
    }),
    kind("artifact")
  );
  assert.equal(puts, 0);
  await assert.rejects(
    prepareApplicationArtifacts(artifactSnapshot(), {
      async readSource() {
        return Buffer.from("body");
      },
      target,
    }),
    kind("artifact")
  );
  assert.equal(puts, 1);
});

test("a missing binary chunk rejects before SQL and produces no durable import receipt", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  const reference = captureNativeBlob(5, () => Buffer.from("bytes"), { writeChunk() {} });
  const input = snapshot();
  input.tables.push({
    name: "files",
    columns: ["id", "content"],
    rowOrder: "rowid",
    rows: [{ rowid: "1", values: ["f", reference] }],
  });
  const { database, calls, getReceipt } = fixture();
  try {
    await assert.rejects(
      importApplicationSnapshot(database, input, [], undefined, {
        memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes,
        source: {
          async readChunk() {
            return null;
          },
        },
      }),
      (error) => error instanceof NativeBlobCarrierError && error.kind === "verification"
    );
    assert.equal(calls.length, 0);
    assert.equal(getReceipt(), undefined);
  } finally {
    await database.close();
  }
});

test("historical native restore creates a new authority timeline and receipt reconciliation keeps it stable", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  const input = snapshot();
  input.source = "postgres";
  const sourceAuthority = "947b9b14-7366-47d3-ac5d-0450a5d83abd";
  input.tables.push({
    name: "api_key_policy_epoch",
    columns: ["singleton", "authority_id", "generation"],
    rowOrder: "native",
    rows: [{ rowid: null, values: [true, sourceAuthority, { kind: "integer", value: "37" }] }],
  });
  const { database, getEpoch } = fixture();
  try {
    const receipt = await importApplicationSnapshot(database, input);
    assert.equal(receipt.restoreAuthority?.sourceAuthority, sourceAuthority);
    assert.equal(receipt.restoreAuthority?.sourceGeneration, "37");
    assert.equal(receipt.restoreAuthority?.targetGeneration, "0");
    assert.notEqual(receipt.restoreAuthority?.targetAuthority, sourceAuthority);
    assert.equal(getEpoch()?.authority_id, receipt.restoreAuthority?.targetAuthority);
    assert.equal(getEpoch()?.generation, "0");
    assert.ok(
      receipt.transformations.some(
        (value) =>
          value.table === "api_key_policy_epoch" &&
          value.column === "authority_id" &&
          value.source === sourceAuthority
      )
    );
    const repeated = await importApplicationSnapshot(database, input);
    assert.equal(repeated.alreadyImported, true);
    assert.equal(repeated.planDigest, receipt.planDigest);
    assert.deepEqual(repeated.restoreAuthority, receipt.restoreAuthority);
    assert.equal((await verifyApplicationImport(database, input)).planDigest, receipt.planDigest);
  } finally {
    await database.close();
  }
});

test("unversioned candidate-era receipts fail closed without replaying import writes", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  const { database, getReceipt, calls } = fixture();
  try {
    await importApplicationSnapshot(database, snapshot());
    const stored = getReceipt()!;
    assert.equal(stored.digestAlgorithm, "sha256-canonical-json-v1");
    delete stored.digestAlgorithm;
    const before = calls.filter((value) => value.text.startsWith("INSERT")).length;
    await assert.rejects(importApplicationSnapshot(database, snapshot()), kind("different_import"));
    await assert.rejects(verifyApplicationImport(database, snapshot()), kind("different_import"));
    assert.equal(calls.filter((value) => value.text.startsWith("INSERT")).length, before);
  } finally {
    await database.close();
  }
});
