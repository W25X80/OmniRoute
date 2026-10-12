import assert from "node:assert/strict";
import test from "node:test";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../src/lib/db/adapters/betterSqliteAdapter.ts";
import {
  capturePostgresApplicationSnapshot,
  importApplicationSnapshot,
  verifyApplicationImport,
} from "../../src/lib/db/postgres/applicationImport.ts";
import { captureSqliteApplicationSnapshot } from "../../src/lib/db/postgres/applicationSnapshot.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "import and logical restore compare text primary keys in byte order under a non-C collation",
  { skip: !postgresTestsEnabled, timeout: 180000 },
  async () => {
    const prior = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-import-ordering-fixture-key";
    const database = await createTestPostgresDatabase({ transactionTimeoutMs: 300000 });
    const source = createBetterSqliteAdapter(new Database(":memory:"));
    const prepareTarget = async () => {
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await migratePostgresSchema(database);
      await database.transaction(async (tx) => {
        const collation = await tx.query<{ present: boolean }>(
          `SELECT EXISTS(SELECT 1 FROM pg_collation
          WHERE collnamespace='pg_catalog'::regnamespace AND collname='en-x-icu') AS present`
        );
        assert.equal(collation.rows[0].present, true, "the non-C oracle requires en-x-icu");
        // Alter both sides of the existing FK, then restore the original constraint.
        await tx.query(
          "ALTER TABLE omniroute.middleware_logs DROP CONSTRAINT middleware_logs_hook_name_fkey"
        );
        await tx.query(
          'ALTER TABLE omniroute.middleware_hooks ALTER COLUMN name TYPE text COLLATE pg_catalog."en-x-icu"'
        );
        await tx.query(
          'ALTER TABLE omniroute.middleware_logs ALTER COLUMN hook_name TYPE text COLLATE pg_catalog."en-x-icu"'
        );
        await tx.query(
          `ALTER TABLE omniroute.middleware_logs ADD CONSTRAINT middleware_logs_hook_name_fkey
          FOREIGN KEY (hook_name) REFERENCES omniroute.middleware_hooks(name) ON DELETE CASCADE`
        );
        const order = await tx.query<{ value: string }>(
          `SELECT value FROM (VALUES ('Z'::text),('a'::text)) AS sample(value)
          ORDER BY value COLLATE pg_catalog."en-x-icu"`
        );
        assert.deepEqual(
          order.rows.map((row) => row.value),
          ["a", "Z"]
        );
      });
    };
    try {
      source.exec(`CREATE TABLE key_value(
        namespace TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(namespace,key));
        CREATE TABLE middleware_hooks(
        name TEXT PRIMARY KEY,code TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);`);
      for (const name of ["Z", "a"]) {
        source
          .prepare("INSERT INTO middleware_hooks VALUES(?,?,?,?)")
          .run(name, "return context;", "2026-10-12T00:00:00.000Z", "2026-10-12T00:00:00.000Z");
      }
      source.pragma("query_only=ON");
      const snapshot = captureSqliteApplicationSnapshot(source);
      await prepareTarget();
      const imported = await importApplicationSnapshot(database, snapshot);
      assert.equal(imported.tableCounts.middleware_hooks, 2);
      await verifyApplicationImport(database, snapshot);
      const databaseOrder = await database.transaction((tx) =>
        tx.query<{ name: string }>("SELECT name FROM omniroute.middleware_hooks ORDER BY name")
      );
      assert.deepEqual(
        databaseOrder.rows.map((row) => row.name),
        ["a", "Z"]
      );
      const exported = await capturePostgresApplicationSnapshot(database);
      const hooks = exported.tables.find((table) => table.name === "middleware_hooks");
      assert.ok(hooks);
      const nameColumn = hooks.columns.indexOf("name");
      assert.deepEqual(
        hooks.rows.map((row) => row.values[nameColumn]),
        ["Z", "a"]
      );
      await prepareTarget();
      const restored = await importApplicationSnapshot(database, exported);
      assert.equal(restored.tableCounts.middleware_hooks, 2);
      await verifyApplicationImport(database, exported);
    } finally {
      source.close();
      await database.close();
      if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = prior;
    }
  }
);
