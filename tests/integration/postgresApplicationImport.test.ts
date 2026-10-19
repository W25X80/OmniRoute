import { createHash } from "node:crypto";
import {
  NATIVE_BLOB_LIMITS,
  NativeBlobCarrierError,
} from "../../src/lib/db/postgres/nativeBlobCarrier.ts";
import assert from "node:assert/strict";
import test from "node:test";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../src/lib/db/adapters/betterSqliteAdapter.ts";
import { encrypt } from "../../src/lib/db/encryption.ts";
import {
  captureSqliteApplicationSnapshot,
  applicationSnapshotDigest,
  ApplicationImportError,
} from "../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  importApplicationSnapshot,
  verifyApplicationImport,
  capturePostgresApplicationSnapshot,
  type ApplicationImportReceipt,
} from "../../src/lib/db/postgres/applicationImport.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { openStoredFileContent } from "../../src/lib/db/postgres/serviceFileContent.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const AUTH_GENERATION_SETTING = "managementAuthGeneration";

async function readManagementGeneration(database: PostgresDatabase): Promise<string> {
  const { rows } = await database.transaction((transaction) =>
    transaction.query<{ value: string }>(
      "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key=$1",
      [AUTH_GENERATION_SETTING]
    )
  );
  assert.equal(rows.length, 1);
  return rows[0].value;
}

async function assertLegacyGenerationReceipt(
  database: PostgresDatabase,
  receipt: ApplicationImportReceipt,
  targetGeneration: string
): Promise<void> {
  assert.equal(receipt.tableCounts.key_value, 1);
  assert.equal(
    receipt.rowCount,
    Object.values(receipt.tableCounts).reduce((sum, count) => sum + count, 0)
  );
  assert.deepEqual(
    receipt.transformations.filter(
      (change) => change.table === "key_value" && change.column === "value"
    ),
    [
      {
        table: "key_value",
        row: 0,
        column: "value",
        source: null,
        target: targetGeneration,
      },
    ]
  );
  const { rows } = await database.transaction((transaction) =>
    transaction.query<{ namespace: string; key: string; value: string }>(
      "SELECT namespace,key,value FROM omniroute.key_value WHERE namespace='settings' AND key=$1",
      [AUTH_GENERATION_SETTING]
    )
  );
  assert.deepEqual(rows, [
    { namespace: "settings", key: AUTH_GENERATION_SETTING, value: targetGeneration },
  ]);
}

async function readLogicalFileContent(transaction: PostgresTransaction, id: string) {
  const content = await openStoredFileContent(transaction, id);
  assert.ok(content, "the logical file must exist");
  if (content.sizeBytes === null) return null;
  const chunks: Buffer[] = [];
  for (
    let index = 0;
    index < Math.ceil(content.sizeBytes / NATIVE_BLOB_LIMITS.chunkBytes);
    index++
  ) {
    const bytes = await content.readChunk(index);
    assert.ok(Buffer.isBuffer(bytes));
    chunks.push(bytes);
  }
  return Buffer.concat(chunks, content.sizeBytes);
}

test(
  "native offline import and logical restore preserve selected durable owner records",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const prior = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-integration-key";
    const database = await createTestPostgresDatabase({ transactionTimeoutMs: 300000 });
    const raw = new Database(":memory:"),
      source = createBetterSqliteAdapter(raw);
    try {
      source.exec(`CREATE TABLE key_value(namespace TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(namespace,key));
      CREATE TABLE db_meta(key TEXT PRIMARY KEY,value TEXT);
      CREATE TABLE provider_connections(id TEXT PRIMARY KEY,provider TEXT NOT NULL,auth_type TEXT,name TEXT,api_key TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
      CREATE TABLE api_keys(id TEXT PRIMARY KEY,name TEXT NOT NULL,key TEXT NOT NULL,created_at TEXT NOT NULL);
      CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,created_at INTEGER,filename TEXT,purpose TEXT,content BLOB);
      CREATE TABLE usage_history(id INTEGER PRIMARY KEY AUTOINCREMENT,provider TEXT,model TEXT,timestamp TEXT,tokens_input INTEGER,latency_ms REAL,ttft_ms REAL);
      CREATE TABLE session_model_history(id INTEGER PRIMARY KEY AUTOINCREMENT,session_id TEXT,combo_name TEXT,model_str TEXT,provider TEXT,connection_id TEXT,used_at TEXT);
      CREATE TABLE user_levels(api_key_id TEXT PRIMARY KEY,total_xp REAL,current_level REAL,updated_at TEXT);
      CREATE TABLE quota_groups(id TEXT PRIMARY KEY,name TEXT,created_at TEXT);
      CREATE TABLE compression_analytics(id INTEGER PRIMARY KEY,timestamp TEXT NOT NULL,mode TEXT NOT NULL,original_tokens INTEGER NOT NULL,compressed_tokens INTEGER NOT NULL,tokens_saved INTEGER NOT NULL,duration_ms INTEGER);
      CREATE TABLE compression_engine_breakdown(id INTEGER PRIMARY KEY,timestamp TEXT NOT NULL,engine TEXT NOT NULL,original_tokens INTEGER,compressed_tokens INTEGER,tokens_saved INTEGER,duration_ms INTEGER);`);
      const ciphertext = encrypt("isolated-provider-credential")!;
      source
        .prepare(
          "INSERT INTO provider_connections(rowid,id,provider,auth_type,name,api_key,created_at,updated_at) VALUES(17,?,?,?,?,?,?,?)"
        )
        .run(
          "provider-fixture",
          "openai",
          "apikey",
          "Provider fixture",
          ciphertext,
          "2026-10-11 03:00:00",
          "2026-10-11 03:00:00"
        );
      source
        .prepare("INSERT INTO api_keys(rowid,id,name,key,created_at) VALUES(11,?,?,?,?)")
        .run("key-fixture", "Key fixture", "opaque-import-key", "2026-10-11 03:00:00");
      source.prepare("INSERT INTO key_value VALUES(?,?,?)").run("settings", "requireLogin", "true");
      source
        .prepare("INSERT INTO key_value VALUES(?,?,?)")
        .run("secrets", "jwtSecret", JSON.stringify(encrypt("isolated-jwt-import-secret")));
      source
        .prepare("INSERT INTO files VALUES(?,?,?,?,?,?)")
        .run("file-fixture", 3, 1000, "fixture.jsonl", "batch", Buffer.from([0, 1, 255]));
      source
        .prepare(
          "INSERT INTO usage_history(id,provider,model,timestamp,tokens_input,latency_ms,ttft_ms) VALUES(?,?,?,?,?,?,?)"
        )
        .run(41, "openai", "model", "2026-10-11 03:00:00", 12, 10.25, 1.5);
      source
        .prepare("INSERT INTO session_model_history VALUES(?,?,?,?,?,?,?)")
        .run(99, "s", "combo", "openai/model", "openai", "provider-fixture", "2026-10-11 03:00:00");
      source
        .prepare("INSERT INTO user_levels VALUES(?,?,?,?)")
        .run("key-fixture", 1.25, 1, "2026-10-11 03:00:00");
      source
        .prepare("INSERT INTO quota_groups VALUES(?,?,?)")
        .run("group-demo", "Imported Demo", "2026-10-11 03:00:00");
      source
        .prepare("INSERT INTO compression_analytics VALUES(?,?,?,?,?,?,?)")
        .run(21, "2026-10-11T03:00:00Z", "stacked", 100, 70, 30, 1.23);
      source
        .prepare("INSERT INTO compression_engine_breakdown VALUES(?,?,?,?,?,?,?)")
        .run(31, "2026-10-11T03:00:00Z", "lite", 100, 70, 30, 0.45);
      const sourceDurationType = source
        .prepare("SELECT typeof(duration_ms) AS type FROM compression_analytics")
        .get() as { type: string };
      assert.equal(
        sourceDurationType.type,
        "real",
        "the source INTEGER-affinity column retains the existing fractional producer value"
      );
      source.pragma("query_only=ON");
      const snapshot = captureSqliteApplicationSnapshot(source),
        sourceDigest = applicationSnapshotDigest(snapshot);
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await migratePostgresSchema(database);
      await t.test(
        "ciphertext, IDs, source row order, BLOB and fractional latency survive native INSERT",
        async () => {
          const receipt = await importApplicationSnapshot(database, snapshot);
          assert.equal(receipt.sourceDigest, sourceDigest);
          assert.equal(receipt.rowCount, 12);
          const durationValues = await database.transaction(async (tx) => ({
            aggregate: (
              await tx.query("SELECT duration_ms FROM omniroute.compression_analytics WHERE id=21")
            ).rows[0].duration_ms,
            breakdown: (
              await tx.query(
                "SELECT duration_ms FROM omniroute.compression_engine_breakdown WHERE id=31"
              )
            ).rows[0].duration_ms,
          }));
          assert.deepEqual(durationValues, { aggregate: 1.23, breakdown: 0.45 });
          const result = await database.transaction(async (tx) => {
            const provider = (
              await tx.query<{ api_key: string; insertion_order: string }>(
                "SELECT api_key,insertion_order::text AS insertion_order FROM omniroute.provider_connections WHERE id='provider-fixture'"
              )
            ).rows[0];
            const file = { content: await readLogicalFileContent(tx, "file-fixture") };
            const usage = (
              await tx.query<{ timestamp: string; latency_ms: number; ttft_ms: number }>(
                "SELECT timestamp,latency_ms,ttft_ms FROM omniroute.usage_history WHERE id=41"
              )
            ).rows[0];
            const history = (
              await tx.query<{ used_at: string }>(
                "SELECT used_at FROM omniroute.session_model_history WHERE id=99"
              )
            ).rows[0];
            return { provider, file, usage, history };
          });
          assert.equal(result.provider.api_key, ciphertext);
          assert.equal(result.provider.insertion_order, "17");
          assert.deepEqual(result.file.content, Buffer.from([0, 1, 255]));
          assert.deepEqual(result.usage, {
            timestamp: "2026-10-11T03:00:00.000Z",
            latency_ms: 10.25,
            ttft_ms: 1.5,
          });
          assert.equal(result.history.used_at, "2026-10-11 03:00:00");
          assert.ok(
            receipt.transformations.some(
              (change) =>
                change.table === "usage_history" &&
                change.column === "timestamp" &&
                change.source === "2026-10-11 03:00:00"
            )
          );
          assert.equal((await importApplicationSnapshot(database, snapshot)).alreadyImported, true);
          await verifyApplicationImport(database, snapshot);
        }
      );
      await t.test(
        "logical export restores into a fresh schema and retains source-control provenance",
        async () => {
          const restored = capturePostgresApplicationSnapshot(database);
          const native = await restored;
          assert.ok(
            native.metadataTables.some((table) => table.name === "application_import_receipts")
          );
          await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
          await migratePostgresSchema(database);
          const receipt = await importApplicationSnapshot(database, native);
          assert.equal(receipt.sourceDigest, applicationSnapshotDigest(native));
          await verifyApplicationImport(database, native);
          const sourceEpoch = native.tables.find((value) => value.name === "api_key_policy_epoch")!;
          assert.equal(
            receipt.restoreAuthority?.sourceAuthority,
            sourceEpoch.rows[0].values[sourceEpoch.columns.indexOf("authority_id")]
          );
          assert.notEqual(
            receipt.restoreAuthority?.targetAuthority,
            receipt.restoreAuthority?.sourceAuthority
          );
          assert.equal(receipt.restoreAuthority?.targetGeneration, "0");
          const next = await database.transaction(
            async (tx) =>
              (
                await tx.query<{ id: string }>(
                  "INSERT INTO omniroute.session_model_history(session_id,combo_name,model_str,provider,used_at) VALUES('new','combo','openai/model','openai','2026-10-11 04:00:00') RETURNING id::text AS id"
                )
              ).rows[0]
          );
          assert.equal(next.id, "100");
          await assert.rejects(
            verifyApplicationImport(database, native),
            (error) => error instanceof ApplicationImportError && error.kind === "verification"
          );
        }
      );
      await t.test(
        "invalid/fractional token source values fail before any durable import receipt",
        async () => {
          await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
          await migratePostgresSchema(database);
          const invalid = structuredClone(snapshot),
            usage = invalid.tables.find((table) => table.name === "usage_history")!;
          usage.rows[0].values[usage.columns.indexOf("tokens_input")] = 1.25;
          await assert.rejects(
            importApplicationSnapshot(database, invalid),
            (error) => error instanceof ApplicationImportError && error.kind === "value"
          );
          const count = await database.transaction(
            async (tx) =>
              (
                await tx.query<{ count: string }>(
                  "SELECT COUNT(*)::text AS count FROM omniroute.application_import_receipts"
                )
              ).rows[0]
          );
          assert.equal(count.count, "0");
        }
      );
      await t.test(
        "binary carrier import and native logical restore preserve a multi-chunk file",
        async () => {
          await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
          await migratePostgresSchema(database);
          const targetGeneration = await readManagementGeneration(database);
          const fileRaw = new Database(":memory:"),
            fileSource = createBetterSqliteAdapter(fileRaw);
          const chunks = new Map<string, Uint8Array>();
          const sink = {
            writeChunk(id: string, index: number, bytes: Uint8Array) {
              chunks.set(`${id}/${index}`, Buffer.from(bytes));
            },
          };
          const reader = {
            async readChunk(id: string, index: number) {
              return chunks.get(`${id}/${index}`) ?? null;
            },
          };
          const content = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 17, 0x51);
          content[content.length - 1] = 0xff;
          try {
            fileSource.exec(
              "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,created_at INTEGER,filename TEXT,purpose TEXT,content BLOB)"
            );
            fileSource
              .prepare("INSERT INTO files VALUES(?,?,?,?,?,?)")
              .run("carrier-file", content.length, 1000, "carrier.jsonl", "batch", content);
            fileSource
              .prepare("INSERT INTO files VALUES(?,?,?,?,?,?)")
              .run("carrier-file-two", 5, 1000, "second.jsonl", "batch", Buffer.from("other"));
            fileSource.pragma("query_only=ON");
            const fileSnapshot = captureSqliteApplicationSnapshot(fileSource, { blobSink: sink });
            const chunk = [...chunks.entries()][1];
            chunks.delete(chunk[0]);
            await assert.rejects(
              importApplicationSnapshot(database, fileSnapshot, [], undefined, {
                source: reader,
                memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes,
              }),
              (error) => error instanceof NativeBlobCarrierError && error.kind === "verification"
            );
            const count = await database.transaction(
              async (tx) =>
                (
                  await tx.query<{ count: string }>(
                    "SELECT COUNT(*)::text AS count FROM omniroute.application_import_receipts"
                  )
                ).rows[0]
            );
            assert.equal(count.count, "0");
            chunks.set(chunk[0], chunk[1]);
            const secondReference = fileSnapshot.tables.find((value) => value.name === "files")!
              .rows[1].values[5];
            assert.ok(
              secondReference &&
                typeof secondReference === "object" &&
                secondReference.kind === "blob-reference"
            );
            let secondReads = 0;
            await assert.rejects(
              importApplicationSnapshot(database, fileSnapshot, [], undefined, {
                memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes,
                source: {
                  async readChunk(id, index) {
                    const bytes = await reader.readChunk(id, index);
                    return id === secondReference.id && ++secondReads > 1
                      ? Buffer.from("wrong")
                      : bytes;
                  },
                },
              }),
              (error) => error instanceof NativeBlobCarrierError && error.kind === "verification"
            );
            const rollback = await database.transaction(async (tx) => {
              const files = (
                await tx.query<{ count: string }>(
                  "SELECT COUNT(*)::text AS count FROM omniroute.files"
                )
              ).rows[0];
              const receipts = (
                await tx.query<{ count: string }>(
                  "SELECT COUNT(*)::text AS count FROM omniroute.application_import_receipts"
                )
              ).rows[0];
              const physical = (
                await tx.query<{ states: string; chunks: string }>(
                  "SELECT (SELECT COUNT(*) FROM omniroute.files_content_state)::text AS states,(SELECT COUNT(*) FROM omniroute.files_content_chunks)::text AS chunks"
                )
              ).rows[0];
              return { files: files.count, receipts: receipts.count, ...physical };
            });
            assert.deepEqual(rollback, { files: "0", receipts: "0", states: "0", chunks: "0" });
            const imported = await importApplicationSnapshot(
              database,
              fileSnapshot,
              [],
              undefined,
              { source: reader, memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes }
            );
            assert.equal(imported.rowCount, 3);
            assert.equal(imported.tableCounts.files, 2);
            await assertLegacyGenerationReceipt(database, imported, targetGeneration);
            await verifyApplicationImport(database, fileSnapshot);
            const native = await capturePostgresApplicationSnapshot(database, { blobSink: sink });
            assert.ok(JSON.stringify(native).length < 65536);
            await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
            await migratePostgresSchema(database);
            const restored = await importApplicationSnapshot(database, native, [], undefined, {
              source: reader,
              memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes,
            });
            assert.notEqual(
              restored.restoreAuthority?.sourceAuthority,
              restored.restoreAuthority?.targetAuthority
            );
            await verifyApplicationImport(database, native);
            const actual = await database.transaction((tx) =>
              readLogicalFileContent(tx, "carrier-file")
            );
            assert.ok(Buffer.isBuffer(actual));
            assert.equal(actual.length, content.length);
            assert.equal(
              createHash("sha256").update(actual).digest("hex"),
              createHash("sha256").update(content).digest("hex")
            );
          } finally {
            fileSource.close();
          }
        }
      );
      await t.test(
        "logical maintenance preserves NULL, empty, tombstones and legacy inline fallback",
        async () => {
          await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
          await migratePostgresSchema(database);
          const targetGeneration = await readManagementGeneration(database);
          const rawFiles = new Database(":memory:");
          const sourceFiles = createBetterSqliteAdapter(rawFiles);
          const expected = new Map<string, Buffer | null>([
            ["null-file", null],
            ["empty-file", Buffer.alloc(0)],
            ["live-file", Buffer.from([0, 0xff, 2])],
            ["deleted-file", Buffer.from("deleted content")],
          ]);
          const chunks = new Map<string, Uint8Array>();
          const sink = {
            writeChunk(id: string, index: number, bytes: Uint8Array) {
              chunks.set(`${id}/${index}`, Buffer.from(bytes));
            },
          };
          const reader = {
            async readChunk(id: string, index: number) {
              return chunks.get(`${id}/${index}`) ?? null;
            },
          };
          try {
            sourceFiles.exec(
              "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,created_at INTEGER,filename TEXT,purpose TEXT,deleted_at INTEGER,content BLOB)"
            );
            for (const [id, content] of expected)
              sourceFiles
                .prepare("INSERT INTO files VALUES(?,?,?,?,?,?,?)")
                .run(
                  id,
                  41,
                  1000,
                  `${id}.bin`,
                  "batch",
                  id === "deleted-file" ? 2000 : null,
                  content
                );
            sourceFiles.pragma("query_only=ON");
            const logical = captureSqliteApplicationSnapshot(sourceFiles);
            const receipt = await importApplicationSnapshot(database, logical);
            assert.equal(receipt.rowCount, 5);
            assert.equal(receipt.tableCounts.files, 4);
            await assertLegacyGenerationReceipt(database, receipt, targetGeneration);
            await verifyApplicationImport(database, logical);
            const observed = await database.transaction(async (tx) => {
              const rows = (
                await tx.query<{ id: string; bytes: string; deleted_at: string | null }>(
                  "SELECT id,bytes::text AS bytes,deleted_at::text AS deleted_at FROM omniroute.files ORDER BY id"
                )
              ).rows;
              const contents: { id: string; content: Buffer | null }[] = [];
              for (const row of rows)
                contents.push({ id: row.id, content: await readLogicalFileContent(tx, row.id) });
              const states = (
                await tx.query<{ count: string }>(
                  "SELECT COUNT(*)::text AS count FROM omniroute.files_content_state"
                )
              ).rows[0].count;
              return { rows, contents, states };
            });
            assert.equal(observed.states, "3");
            for (const row of observed.rows) {
              assert.equal(row.bytes, "41", "files.bytes is source metadata, not a content length");
              assert.equal(row.deleted_at, row.id === "deleted-file" ? "2000" : null);
            }
            for (const file of observed.contents)
              assert.deepEqual(file.content, expected.get(file.id));
            await database.transaction(async (tx) => {
              await tx.query("DELETE FROM omniroute.files_content_state WHERE file_id='live-file'");
              await tx.query("UPDATE omniroute.files SET content=$1 WHERE id='live-file'", [
                expected.get("live-file"),
              ]);
            });
            await verifyApplicationImport(database, logical);
            assert.equal(
              (await importApplicationSnapshot(database, logical)).alreadyImported,
              true
            );
            const native = await capturePostgresApplicationSnapshot(database, { blobSink: sink });
            assert.equal(native.format, "omniroute-application-storage-v1");
            for (const table of ["files_content_state", "files_content_chunks"]) {
              assert.equal(
                native.tables.some((row) => row.name === table),
                false
              );
              assert.ok(
                native.excluded.some(
                  (row) => row.name === table && row.reason.includes("files.content")
                )
              );
            }
            const exportedFiles = native.tables.find((table) => table.name === "files")!;
            const contentAt = exportedFiles.columns.indexOf("content");
            const nullRow = exportedFiles.rows.find(
              (row) => row.values[exportedFiles.columns.indexOf("id")] === "null-file"
            )!;
            assert.equal(nullRow.values[contentAt], null);
            await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
            await migratePostgresSchema(database);
            const restored = await importApplicationSnapshot(database, native, [], undefined, {
              source: reader,
              memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes,
            });
            assert.notEqual(
              restored.restoreAuthority?.sourceAuthority,
              restored.restoreAuthority?.targetAuthority
            );
            await verifyApplicationImport(database, native);
            for (const [id, content] of expected)
              assert.deepEqual(
                await database.transaction((tx) => readLogicalFileContent(tx, id)),
                content
              );
            await database.transaction((tx) =>
              tx.query(
                "UPDATE omniroute.files_content_chunks SET checksum=$1 WHERE file_id='deleted-file'",
                ["a".repeat(64)]
              )
            );
            const incomplete = (error: unknown) =>
              error instanceof Error &&
              error.message === "Stored file content is incomplete or invalid";
            await assert.rejects(verifyApplicationImport(database, native), incomplete);
            await assert.rejects(
              capturePostgresApplicationSnapshot(database, { blobSink: sink }),
              incomplete
            );
            await database.transaction((tx) =>
              tx.query(
                "UPDATE omniroute.files_content_chunks SET checksum=$1 WHERE file_id='deleted-file'",
                [createHash("sha256").update(expected.get("deleted-file")!).digest("hex")]
              )
            );
            await verifyApplicationImport(database, native);
          } finally {
            sourceFiles.close();
          }
        }
      );
    } finally {
      source.close();
      await database.close();
      if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = prior;
    }
  }
);
