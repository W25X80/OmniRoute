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
} from "../../src/lib/db/postgres/applicationImport.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

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
      CREATE TABLE quota_groups(id TEXT PRIMARY KEY,name TEXT,created_at TEXT);`);
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
          assert.equal(receipt.rowCount, 9);
          const result = await database.transaction(async (tx) => {
            const provider = (
              await tx.query<{ api_key: string; insertion_order: string }>(
                "SELECT api_key,insertion_order::text AS insertion_order FROM omniroute.provider_connections WHERE id='provider-fixture'"
              )
            ).rows[0];
            const file = (
              await tx.query<{ content: Buffer }>(
                "SELECT content FROM omniroute.files WHERE id='file-fixture'"
              )
            ).rows[0];
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
              return { files: files.count, receipts: receipts.count };
            });
            assert.deepEqual(rollback, { files: "0", receipts: "0" });
            const imported = await importApplicationSnapshot(
              database,
              fileSnapshot,
              [],
              undefined,
              { source: reader, memoryBudgetBytes: NATIVE_BLOB_LIMITS.importMemoryBudgetBytes }
            );
            assert.equal(imported.rowCount, 2);
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
            const actual = await database.transaction(
              async (tx) =>
                (
                  await tx.query<{ content: Buffer }>(
                    "SELECT content FROM omniroute.files WHERE id='carrier-file'"
                  )
                ).rows[0].content
            );
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
    } finally {
      source.close();
      await database.close();
      if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = prior;
    }
  }
);
