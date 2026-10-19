import assert from "node:assert/strict";
import test from "node:test";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../src/lib/db/adapters/betterSqliteAdapter.ts";
import { captureSqliteApplicationSnapshot } from "../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  importApplicationSnapshot,
  verifyApplicationImport,
} from "../../src/lib/db/postgres/applicationImport.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "external history rows share atomic import, receipt reconciliation and drift detection",
  {
    skip: !postgresTestsEnabled,
  },
  async () => {
    const prior = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-row-carrier-encryption";
    const db = await createTestPostgresDatabase({ transactionTimeoutMs: 300000 });
    const raw = new Database(":memory:"),
      source = createBetterSqliteAdapter(raw);
    const chunks = new Map<number, Buffer>();
    try {
      await db.transaction(async (tx) => {
        await tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE");
      });
      await migratePostgresSchema(db);
      const generationRows = await db.transaction((tx) =>
        tx.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
        )
      );
      assert.equal(generationRows.rows.length, 1);
      const targetGeneration = generationRows.rows[0].value;
      source.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT);
      CREATE TABLE conversation_turn_nodes(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,parent_id TEXT,
        role TEXT NOT NULL,content_hash TEXT NOT NULL DEFAULT '',last_correlation_id TEXT,
        first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL);
      WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<5002)
      INSERT INTO conversation_turn_nodes(rowid,id,conversation_id,role,first_seen_at,last_seen_at)
        SELECT v*3-10,printf('node-%d',v),'c','user','2026-10-12','2026-10-12' FROM n;`);
      source.pragma("query_only=ON");
      const snapshot = captureSqliteApplicationSnapshot(source, {
        rowSink: {
          writeChunk(_id, index, bytes) {
            chunks.set(index, Buffer.from(bytes));
          },
        },
      });
      const bytes = {
        async readChunk(_id: string, index: number) {
          return chunks.get(index) ?? null;
        },
      };
      const receipt = await importApplicationSnapshot(
        db,
        snapshot,
        [],
        undefined,
        undefined,
        bytes
      );
      assert.equal(receipt.tableCounts.conversation_turn_nodes, 5002);
      assert.equal(receipt.tableCounts.key_value, 1);
      assert.equal(receipt.rowCount, 5003);
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
      assert.deepEqual(
        (
          await db.transaction((tx) =>
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
      assert.equal(
        (await verifyApplicationImport(db, snapshot, bytes)).planDigest,
        receipt.planDigest
      );
      assert.equal(
        (await importApplicationSnapshot(db, snapshot, [], undefined, undefined, bytes))
          .alreadyImported,
        true
      );
      await db.transaction(async (tx) => {
        const rows = (
          await tx.query<{ id: string; insertion_order: string }>(
            "SELECT id,insertion_order FROM omniroute.conversation_turn_nodes ORDER BY insertion_order LIMIT 1"
          )
        ).rows;
        assert.deepEqual(rows, [{ id: "node-1", insertion_order: "-7" }]);
        const next = (
          await tx.query<{ insertion_order: string }>(
            "INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at) VALUES('new','c','user','2026-10-12','2026-10-12') RETURNING insertion_order"
          )
        ).rows[0];
        assert.equal(next.insertion_order, String(5002 * 3 - 10 + 1));
        await tx.query("DELETE FROM omniroute.conversation_turn_nodes WHERE id='new'");
        await tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET content_hash='changed' WHERE id='node-5002'"
        );
      });
      await assert.rejects(verifyApplicationImport(db, snapshot, bytes));
      await assert.rejects(
        importApplicationSnapshot(db, snapshot, [], undefined, undefined, bytes)
      );
    } finally {
      source.close();
      await db.close();
      if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = prior;
    }
  }
);

test(
  "late external-history corruption rolls back rows and receipt together",
  { skip: !postgresTestsEnabled },
  async () => {
    const prior = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-row-carrier-encryption";
    const db = await createTestPostgresDatabase({ transactionTimeoutMs: 300000 });
    const raw = new Database(":memory:"),
      source = createBetterSqliteAdapter(raw);
    let body: Buffer;
    let reads = 0;
    try {
      await db.transaction(async (tx) => {
        await tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE");
      });
      await migratePostgresSchema(db);
      source.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT);
      CREATE TABLE conversation_turn_nodes(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,parent_id TEXT,
        role TEXT NOT NULL,content_hash TEXT NOT NULL DEFAULT '',last_correlation_id TEXT,first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL);
      INSERT INTO conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at)VALUES('one','c','user','2026-10-12','2026-10-12');`);
      source.pragma("query_only=ON");
      const snapshot = captureSqliteApplicationSnapshot(source, {
        rowSink: {
          writeChunk(_id, _index, bytes) {
            body = Buffer.from(bytes);
          },
        },
      });
      await assert.rejects(
        importApplicationSnapshot(db, snapshot, [], undefined, undefined, {
          async readChunk() {
            reads++;
            return reads === 3 ? Buffer.from(body).fill(32) : body;
          },
        })
      );
      await db.transaction(async (tx) => {
        assert.equal(
          (
            await tx.query<{ count: string }>(
              "SELECT count(*) AS count FROM omniroute.conversation_turn_nodes"
            )
          ).rows[0].count,
          "0"
        );
        assert.equal(
          (
            await tx.query<{ count: string }>(
              "SELECT count(*) AS count FROM omniroute.application_import_receipts"
            )
          ).rows[0].count,
          "0"
        );
      });
    } finally {
      source.close();
      await db.close();
      if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = prior;
    }
  }
);
