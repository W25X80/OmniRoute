import {
  APPLICATION_IMPORT_TABLES,
  APPLICATION_IMPORT_NATIVE_ONLY,
  APPLICATION_IMPORT_NATIVE_FILE_TABLES,
  APPLICATION_IMPORT_DERIVED_TABLES,
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
import { ArtifactStorageError } from "../../../src/domain/persistence/artifactStore.ts";
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
  type OfflineArtifactCheckpoint,
  type OfflineArtifactIO,
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
    managementGeneration = "4a5be1c0-7bd8-4e51-8db6-7c0a79d0a95b",
    rows = [
      {
        namespace: "settings",
        key: "managementAuthGeneration",
        value: JSON.stringify(managementGeneration),
      },
      ...initial,
    ];
  let epoch: Record<string, unknown> | undefined = {
    singleton: true,
    authority_id: "91ae17e6-f411-4c61-8a17-7538d11fb860",
    generation: "0",
  };
  let receipt: Record<string, unknown> | undefined;
  let invalidContent = false;
  const events = new EventEmitter();
  const columns = [
    ...new Set([
      ...APPLICATION_IMPORT_TABLES,
      ...APPLICATION_IMPORT_NATIVE_ONLY,
      ...APPLICATION_IMPORT_NATIVE_FILE_TABLES,
      ...APPLICATION_IMPORT_DERIVED_TABLES,
    ]),
  ].map((name) => ({
    name,
    columns:
      name === "key_value"
        ? ["namespace", "key", "value"]
        : name === "api_key_policy_epoch"
          ? ["singleton", "authority_id", "generation"]
          : name === "conversation_node_counts"
            ? ["conversation_id", "node_count"]
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
                : table.name === "conversation_node_counts" && name === "node_count"
                  ? "bigint"
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
        primary:
          table.name === "key_value"
            ? ["namespace", "key"]
            : table.name === "conversation_node_counts"
              ? ["conversation_id"]
              : table.columns,
        parents: [],
      }));
    else if (text.includes("conversation_node_counts_nonnegative")) {
      assert.match(text, /count\(\*\)=3 AND count\(DISTINCT a\.attcollation\)=1/);
      assert.match(text, /convalidated/);
      assert.match(text, /pg_get_expr\(conbin,conrelid\)='\(node_count >= 0\)'/);
      data = [{ compatible: true }];
    } else if (
      text.startsWith(
        "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
      )
    )
      data = [{ value: JSON.stringify(managementGeneration) }];
    else if (text.startsWith("WITH headers AS"))
      data = [{ maximum: 0, total: "0", invalid: invalidContent }];
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
    else if (text === 'DELETE FROM omniroute."key_value"') rows.length = 0;
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
  return {
    database,
    calls,
    rows,
    managementGeneration,
    getReceipt: () => receipt,
    getEpoch: () => epoch,
    invalidateContent() {
      invalidContent = true;
    },
  };
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
    assert.equal(first.rowCount, 3);
    assert.deepEqual(first.tableCounts, { key_value: 3 });
    const payloadRows = f.rows
      .filter((row) => row.namespace === "settings" && (row.key === "a" || row.key === "z"))
      .map((row) => [row.key, row.value])
      .sort(([left], [right]) => String(left).localeCompare(String(right)));
    assert.deepEqual(payloadRows, [
      ["a", '"default"'],
      ["z", "opaque-ciphertext"],
    ]);
    const generationRows = f.rows.filter(
      (row) => row.namespace === "settings" && row.key === "managementAuthGeneration"
    );
    assert.equal(generationRows.length, 1);
    assert.equal(generationRows[0].value, JSON.stringify(f.managementGeneration));
    assert.deepEqual(first.transformations, [
      {
        table: "key_value",
        row: 2,
        column: "value",
        source: null,
        target: JSON.stringify(f.managementGeneration),
      },
    ]);
    const insert = f.calls.find((call) =>
      call.text.startsWith('INSERT INTO omniroute."key_value"')
    )!;
    assert.ok(!insert.text.includes("opaque-ciphertext"));
    assert.equal(insert.values?.length, 9);
    const before = f.calls.filter((call) => call.text.startsWith("INSERT")).length;
    const repeated = await importApplicationSnapshot(f.database, snapshot());
    assert.equal(repeated.alreadyImported, true);
    assert.equal(repeated.planDigest, first.planDigest);
    assert.deepEqual(repeated.tableCounts, first.tableCounts);
    assert.deepEqual(repeated.transformations, first.transformations);
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
function artifactIOFixture(bytes: Buffer, failure?: "put_unknown" | "readback") {
  const checksum = createHash("sha256").update(bytes).digest("hex");
  const prepared = Object.freeze({
    reference: `s3/00000000-0000-4000-8000-000000000001_${checksum}.json`,
    sizeBytes: bytes.byteLength,
    checksum,
    storageScope: "a".repeat(64),
    singleAttempt: true as const,
  });
  const events: string[] = [];
  const checkpoints: OfflineArtifactCheckpoint[][] = [];
  let object: Buffer | null = null;
  const io: OfflineArtifactIO = {
    async readSource(reference) {
      assert.equal(reference, "legacy/shared.json");
      events.push("source-read");
      return bytes;
    },
    target: {
      storageScope: prepared.storageScope,
      async put(input, options) {
        assert.deepEqual(Buffer.from(input), bytes);
        const onPrepared = options?.onPrepared;
        assert.ok(onPrepared);
        events.push("prepare");
        await onPrepared(prepared);
        assert.equal(checkpoints.at(-1)?.[0].targetReference, prepared.reference);
        events.push("put");
        object = Buffer.from(input);
        if (failure === "put_unknown") {
          throw new ArtifactStorageError("put_unknown", prepared.reference, true);
        }
        return {
          reference: prepared.reference,
          sizeBytes: prepared.sizeBytes,
          checksum: prepared.checksum,
        };
      },
      async get(reference) {
        assert.equal(reference, prepared.reference);
        events.push("get");
        assert.ok(object, "readback must follow the simulated remote PUT effect");
        const readback = Buffer.from(object);
        if (failure === "readback") {
          assert.ok(readback.length > 0);
          readback[0] ^= 0xff;
        }
        return readback;
      },
    },
    async checkpoint(entries) {
      assert.equal(entries.length, 1);
      checkpoints.push(structuredClone([...entries]));
      events.push(`checkpoint:${entries[0].state}:${entries[0].uploadState}`);
    },
  };
  return { io, prepared, events, checkpoints };
}

test("shared artifact upload is one acknowledged object; unknown put is reconciled by exact readback", async () => {
  const bytes = Buffer.from('{"requestBody":"opaque"}'),
    checksum = createHash("sha256").update(bytes).digest("hex");
  const f = artifactIOFixture(bytes, "put_unknown");
  const mappings = await prepareApplicationArtifacts(artifactSnapshot(), f.io);
  assert.deepEqual(f.events, [
    "source-read",
    "prepare",
    "checkpoint:pending:uncertain",
    "put",
    "get",
    "checkpoint:ready:acknowledged",
  ]);
  assert.equal(f.checkpoints.length, 2);
  assert.equal(f.checkpoints[0][0].state, "pending");
  assert.equal(f.checkpoints[0][0].uploadState, "uncertain");
  assert.equal(f.checkpoints[1][0].state, "ready");
  assert.equal(f.checkpoints[1][0].uploadState, "acknowledged");
  const preparedAt = f.checkpoints[0][0].preparedAt;
  assert.ok(Number.isFinite(Date.parse(preparedAt)));
  assert.deepEqual(mappings, [
    {
      sourceReference: "legacy/shared.json",
      targetReference: f.prepared.reference,
      sizeBytes: bytes.byteLength,
      checksum,
      storageScope: f.prepared.storageScope,
      preparedAt,
      singleAttempt: true,
    },
  ]);
  await verifyApplicationArtifacts(mappings, f.io.target);
  const repeated = await prepareApplicationArtifacts(
    artifactSnapshot(),
    {
      readSource: f.io.readSource,
      target: f.io.target,
    },
    mappings
  );
  assert.deepEqual(repeated, mappings);
  assert.equal(f.events.filter((event) => event === "put").length, 1);
  assert.equal(f.events.filter((event) => event === "get").length, 3);
  assert.equal(f.checkpoints.length, 2);
});

test("artifact checksum/length mismatch blocks upload and mutated readback blocks qualification", async () => {
  for (const mismatch of ["length", "checksum"] as const) {
    const input = artifactSnapshot();
    input.tables.at(-1)!.rows[0].values[mismatch === "length" ? 2 : 3] =
      mismatch === "length" ? { kind: "integer", value: "999" } : "0".repeat(64);
    const f = artifactIOFixture(Buffer.from("body"));
    await assert.rejects(prepareApplicationArtifacts(input, f.io), kind("artifact"));
    assert.deepEqual(f.events, ["source-read"]);
    assert.equal(f.checkpoints.length, 0);
  }

  const f = artifactIOFixture(Buffer.from("body"), "readback");
  await assert.rejects(prepareApplicationArtifacts(artifactSnapshot(), f.io), kind("artifact"));
  assert.deepEqual(f.events, [
    "source-read",
    "prepare",
    "checkpoint:pending:uncertain",
    "put",
    "checkpoint:pending:acknowledged",
    "get",
  ]);
  assert.equal(f.checkpoints.length, 2);
  assert.equal(f.checkpoints[1][0].state, "pending");
  assert.equal(f.checkpoints[1][0].uploadState, "acknowledged");
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

test("private file tables cannot be imported as portable logical owner records", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  for (const table of APPLICATION_IMPORT_NATIVE_FILE_TABLES) {
    const input = snapshot();
    input.tables.push({ name: table, columns: ["id"], rowOrder: "rowid", rows: [] });
    const { database, calls, getReceipt } = fixture();
    try {
      await assert.rejects(importApplicationSnapshot(database, input), kind("schema"));
      assert.equal(
        calls.some((call) => call.text.startsWith("INSERT")),
        false
      );
      assert.equal(getReceipt(), undefined);
    } finally {
      await database.close();
    }
  }
});

test("physical file integrity is checked on explicit verification and receipt reconciliation", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  const f = fixture();
  try {
    await importApplicationSnapshot(f.database, snapshot());
    const inserts = f.calls.filter((call) => call.text.startsWith("INSERT")).length;
    f.invalidateContent();
    const incomplete = (error: unknown) =>
      error instanceof Error && error.message === "Stored file content is incomplete or invalid";
    await assert.rejects(verifyApplicationImport(f.database, snapshot()), incomplete);
    await assert.rejects(importApplicationSnapshot(f.database, snapshot()), incomplete);
    assert.equal(f.calls.filter((call) => call.text.startsWith("INSERT")).length, inserts);
  } finally {
    await f.database.close();
  }
});

test("a receipt from a different physical schema cannot authorize import replay", async () => {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-unit-key";
  const f = fixture();
  try {
    await importApplicationSnapshot(f.database, snapshot());
    f.getReceipt()!.schemaDigest = "previous-physical-schema";
    const inserts = f.calls.filter((call) => call.text.startsWith("INSERT")).length;
    await assert.rejects(
      importApplicationSnapshot(f.database, snapshot()),
      kind("different_import")
    );
    assert.equal(f.calls.filter((call) => call.text.startsWith("INSERT")).length, inserts);
  } finally {
    await f.database.close();
  }
});
