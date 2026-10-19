import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import type { SqliteAdapter } from "../../src/lib/db/adapters/types.ts";
import {
  captureSqliteApplicationSnapshot,
  openOfflineSqliteSource,
} from "../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  importApplicationSnapshot,
  verifyApplicationImport,
  capturePostgresApplicationSnapshot,
} from "../../src/lib/db/postgres/applicationImport.ts";
import {
  NATIVE_BLOB_LIMITS,
  type NativeBlobSink,
  type NativeBlobSource,
} from "../../src/lib/db/postgres/nativeBlobCarrier.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "actual 512 MiB SQLite file imports and logically restores through a disk-backed carrier",
  {
    skip:
      !postgresTestsEnabled || process.env.RUN_PG_IMPORT_MAX_BLOB !== "1"
        ? "requires a serial isolated PostgreSQL target, explicit memory budget and at least 2 GiB scratch disk"
        : false,
  },
  async () => {
    const prior = process.env.STORAGE_ENCRYPTION_KEY;
    let folder: string | undefined;
    let database: Awaited<ReturnType<typeof createTestPostgresDatabase>> | undefined;
    let source: SqliteAdapter | undefined;
    try {
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-application-import-integration-key";
      folder = mkdtempSync(path.join(os.tmpdir(), "omniroute-pg-max-carrier-"));
      const filename = path.join(folder, "source.sqlite"),
        blobRoot = path.join(folder, "blobs");
      mkdirSync(blobRoot, { mode: 0o700 });
      // Offline maintenance has an explicit budget; gateway deadlines stay unchanged.
      database = await createTestPostgresDatabase({
        statementTimeoutMs: 60000,
        queryTimeoutMs: 90000,
        idleInTransactionTimeoutMs: 90000,
        transactionTimeoutMs: 300000,
      });
      // The native writer creates the true boundary fixture. The reader is the
      // explicit offline source used by CLI --large-files, without lowered row limits.
      const writer = new DatabaseSync(filename);
      try {
        writer.exec(
          "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,created_at INTEGER,filename TEXT,purpose TEXT,content BLOB)"
        );
        writer
          .prepare("INSERT INTO files VALUES('maximum',?,1000,'maximum.jsonl','batch',zeroblob(?))")
          .run(NATIVE_BLOB_LIMITS.fileBytes, NATIVE_BLOB_LIMITS.fileBytes);
      } finally {
        writer.close();
      }
      source = openOfflineSqliteSource(filename, { largeFiles: true });
      const written = new Set<string>();
      const sink: NativeBlobSink = {
        writeChunk(id, index, bytes) {
          const directory = path.join(blobRoot, id);
          if (!written.has(id)) {
            mkdirSync(directory, { mode: 0o700 });
            written.add(id);
          }
          writeFileSync(path.join(directory, `${index}.bin`), bytes, { mode: 0o600, flag: "wx" });
        },
      };
      const reader: NativeBlobSource = {
        async readChunk(id, index) {
          const filename = path.join(blobRoot, id, `${index}.bin`);
          if ((await stat(filename)).size > NATIVE_BLOB_LIMITS.chunkBytes)
            throw new Error("oversized carrier chunk");
          return readFile(filename);
        },
      };
      const blobs = { source: reader, memoryBudgetBytes: 4 * 1024 * 1024 * 1024 };
      const snapshot = captureSqliteApplicationSnapshot(source, { blobSink: sink });
      assert.ok(JSON.stringify(snapshot).length < 16384);
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await migratePostgresSchema(database);
      const generationRows = await database.transaction((tx) =>
        tx.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
        )
      );
      assert.equal(generationRows.rows.length, 1);
      const targetGeneration = generationRows.rows[0].value;
      const imported = await importApplicationSnapshot(database, snapshot, [], undefined, blobs);
      assert.equal(imported.rowCount, 2);
      assert.equal(imported.tableCounts.files, 1);
      assert.equal(imported.tableCounts.key_value, 1);
      assert.equal(
        imported.rowCount,
        Object.values(imported.tableCounts).reduce((sum, count) => sum + count, 0)
      );
      assert.deepEqual(
        imported.transformations.filter(
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
      assert.deepEqual(
        (
          await database.transaction((tx) =>
            tx.query<{ namespace: string; key: string; value: string }>(
              "SELECT namespace,key,value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
            )
          )
        ).rows,
        [
          {
            namespace: "settings",
            key: "managementAuthGeneration",
            value: targetGeneration,
          },
        ]
      );
      await verifyApplicationImport(database, snapshot);
      const exported = await capturePostgresApplicationSnapshot(database, { blobSink: sink });
      assert.ok(JSON.stringify(exported).length < 65536);
      await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      await migratePostgresSchema(database);
      const restored = await importApplicationSnapshot(database, exported, [], undefined, blobs);
      assert.notEqual(
        restored.restoreAuthority?.sourceAuthority,
        restored.restoreAuthority?.targetAuthority
      );
      await verifyApplicationImport(database, exported);
    } finally {
      source?.close();
      try {
        await database?.close();
      } finally {
        if (folder) rmSync(folder, { recursive: true, force: true });
        if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
        else process.env.STORAGE_ENCRYPTION_KEY = prior;
      }
    }
  }
);
