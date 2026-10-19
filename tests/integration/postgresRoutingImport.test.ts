import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { promisify } from "node:util";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { createPostgresComboRepository } from "../../src/lib/db/postgres/comboRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  captureRoutingSnapshot,
  importRoutingSnapshot,
  type RoutingSnapshot,
} from "../../src/lib/db/postgres/routingImport.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
  PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";
import { tryOpenSync } from "../../src/lib/db/adapters/driverFactory.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";

const AUTH_GENERATION_SETTING = "managementAuthGeneration";

type ObservedQuery = { text: string; values?: unknown[] };

function observeQueries(database: PostgresDatabase) {
  const queries: ObservedQuery[] = [];
  const observedDatabase = new Proxy(database, {
    get(target, property, receiver) {
      if (property !== "transaction") return Reflect.get(target, property, receiver);
      return <T>(
        body: (transaction: PostgresTransaction) => Promise<T>,
        options?: PostgresTransactionOptions
      ) =>
        target.transaction(
          (transaction) =>
            body({
              noteAdvisoryLock: transaction.noteAdvisoryLock?.bind(transaction),
              query<Row>(text: string, values?: unknown[]) {
                queries.push({ text, values });
                return transaction.query<Row>(text, values);
              },
            }),
          options
        );
    },
  });
  return { database: observedDatabase, queries };
}

function sourceGenerationRows(snapshot: RoutingSnapshot) {
  return snapshot.tables.key_value.filter(
    (record) => record.namespace === "settings" && record.key === AUTH_GENERATION_SETTING
  );
}

function sourceRowCount(snapshot: RoutingSnapshot): number {
  return Object.values(snapshot.tables).reduce((sum, rows) => sum + rows.length, 0);
}

async function targetGenerationValues(database: PostgresDatabase): Promise<string[]> {
  const { rows } = await database.transaction((transaction) =>
    transaction.query<{ value: string }>(
      "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key=$1",
      [AUTH_GENERATION_SETTING]
    )
  );
  return rows.map((row) => row.value);
}

async function assertNoRoutingImport(
  database: PostgresDatabase,
  expectedKeyValueRows: number
): Promise<void> {
  const { rows } = await database.transaction((transaction) =>
    transaction.query<{
      combos: string;
      mappings: string;
      keyValues: string;
      proxies: string;
      assignments: string;
      rotations: string;
      receipts: string;
    }>(`SELECT
      (SELECT count(*)::text FROM omniroute.combos) AS combos,
      (SELECT count(*)::text FROM omniroute.model_combo_mappings) AS mappings,
      (SELECT count(*)::text FROM omniroute.key_value) AS "keyValues",
      (SELECT count(*)::text FROM omniroute.proxy_registry) AS proxies,
      (SELECT count(*)::text FROM omniroute.proxy_assignments) AS assignments,
      (SELECT count(*)::text FROM omniroute.proxy_scope_rotation) AS rotations,
      (SELECT count(*)::text FROM omniroute.routing_imports) AS receipts`)
  );
  assert.deepEqual(rows, [
    {
      combos: "0",
      mappings: "0",
      keyValues: String(expectedKeyValueRows),
      proxies: "0",
      assignments: "0",
      rotations: "0",
      receipts: "0",
    },
  ]);
}

async function resetRoutingTarget(database: PostgresDatabase): Promise<void> {
  await database.transaction((transaction) =>
    transaction.query("DROP SCHEMA IF EXISTS omniroute CASCADE")
  );
  await migratePostgresSchema(database);
}

test(
  "offline SQLite routing snapshot imports atomically, preserves source and reconciles receipts",
  { skip: !postgresTestsEnabled, timeout: 30_000 },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "omniroute-routing-import-"));
    const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
    const restoreBackend = () => {
      if (previousBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = previousBackend;
    };
    // Only the disposable test fixture invokes SQLite startup migrations.
    process.env.DATA_DIR = directory;
    process.env.APP_LOG_TO_FILE = "false";
    process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
    const core = await import("../../src/lib/db/core.ts");
    const db = await createTestPostgresDatabase();
    const effects: string[] = [];
    const combos = createPostgresComboRepository(db, {
      bumpProxyRegistryGeneration() {
        effects.push("proxy");
      },
      invalidateCachedLKGP(key) {
        effects.push(key);
      },
    });
    try {
      await db.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await migratePostgresSchema(db);
      process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
      const sqlite = core.getDbInstance();
      sqlite
        .prepare(
          "INSERT INTO combos(id,name,data,sort_order,created_at,updated_at) VALUES (?,?,?,?,?,?)"
        )
        .run(
          "combo",
          "Imported",
          JSON.stringify({ id: "wrong", name: "Imported", models: [] }),
          1.5,
          "source-time",
          "source-time"
        );
      sqlite
        .prepare(
          "INSERT INTO model_combo_mappings(id,pattern,combo_id,created_at,updated_at) VALUES (?,?,?,?,?)"
        )
        .run("map", "gpt-*", "combo", "source-time", "source-time");
      sqlite
        .prepare("INSERT INTO key_value(namespace,key,value) VALUES ('lkgp',?,?)")
        .run("Imported:gpt-4", "opaque-encrypted-sentinel");
      sqlite
        .prepare("INSERT INTO key_value(namespace,key,value) VALUES ('settings',?,?)")
        .run("managementAuthGenerationExtra", "opaque-nearby-sentinel");
      const insertBulk = sqlite.prepare(
        "INSERT INTO key_value(namespace,key,value) VALUES ('bulk',?,?)"
      );
      for (let index = 0; index < 201; index++) insertBulk.run(`key-${index}`, `value-${index}`);
      sqlite
        .prepare("INSERT INTO proxy_registry(id,name,type,host,port) VALUES (?,?,?,?,?)")
        .run("proxy", "proxy", "http", "127.0.0.1", 8080);
      sqlite
        .prepare("INSERT INTO proxy_assignments(id,proxy_id,scope,scope_id) VALUES (?,?,?,?)")
        .run(41, "proxy", "combo", "combo");
      sqlite
        .prepare("INSERT INTO proxy_assignments(id,proxy_id,scope,scope_id) VALUES (?,?,?,?)")
        .run(120, "proxy", "account", "deleted-high-water");
      sqlite.prepare("DELETE FROM proxy_assignments WHERE id = ?").run(120);
      sqlite
        .prepare("INSERT INTO proxy_scope_rotation(scope,scope_id) VALUES (?,?)")
        .run("combo", "combo");
      core.resetDbInstance();
      restoreBackend();
      const source = tryOpenSync(join(directory, "storage.sqlite"), {
        readonly: true,
        fileMustExist: true,
      });
      assert.ok(source);
      let snapshot: RoutingSnapshot;
      try {
        snapshot = captureRoutingSnapshot(source);
      } finally {
        source.close();
      }
      const before = JSON.stringify(snapshot);
      const sourceGeneration = sourceGenerationRows(snapshot);
      const targetGeneration = await targetGenerationValues(db);
      assert.equal(sourceGeneration.length, 1);
      assert.equal(targetGeneration.length, 1);
      assert.notEqual(sourceGeneration[0].value, targetGeneration[0]);
      const observed = observeQueries(db);
      const receipt = await importRoutingSnapshot(observed.database, snapshot);
      assert.equal(receipt.alreadyImported, false);
      assert.match(receipt.digest, /^[0-9a-f]{64}$/);
      assert.equal(receipt.rowCount, sourceRowCount(snapshot));
      assert.equal(JSON.stringify(snapshot), before);
      const keyValueWrites = observed.queries.filter(({ text }) =>
        /\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b[\s\S]*\bomniroute\.key_value\b/i.test(text)
      );
      assert.ok(keyValueWrites.length > 0);
      assert.ok(
        keyValueWrites.every(({ text }) => /^\s*INSERT INTO omniroute\.key_value\b/i.test(text))
      );
      const insertedKeyValues = keyValueWrites.flatMap(({ values }) => {
        const bound = values ?? [];
        const rows: unknown[][] = [];
        for (let index = 0; index < bound.length; index += 3)
          rows.push(bound.slice(index, index + 3));
        return rows;
      });
      assert.equal(insertedKeyValues.length, snapshot.tables.key_value.length - 1);
      assert.equal(
        insertedKeyValues.some(
          ([namespace, key, value]) =>
            namespace === "settings" &&
            key === AUTH_GENERATION_SETTING &&
            value === sourceGeneration[0].value
        ),
        false
      );
      assert.deepEqual(await targetGenerationValues(db), targetGeneration);
      assert.deepEqual(await importRoutingSnapshot(db, snapshot), {
        ...receipt,
        alreadyImported: true,
      });
      const cli = await promisify(execFile)(
        process.execPath,
        [
          "--import",
          "tsx/esm",
          "scripts/ops/postgres-import-routing.ts",
          "--source",
          join(directory, "storage.sqlite"),
        ],
        {
          timeout: 15_000,
          env: {
            ...process.env,
            OMNIROUTE_POSTGRES_HOST: "localhost",
            OMNIROUTE_POSTGRES_PORT: process.env.OMNIROUTE_TEST_POSTGRES_PORT,
            OMNIROUTE_POSTGRES_DATABASE: "omniroute_test",
            OMNIROUTE_POSTGRES_USER: "omniroute_test",
            OMNIROUTE_POSTGRES_PASSWORD: "omniroute_test",
            OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
          },
        }
      );
      assert.deepEqual(JSON.parse(cli.stdout), { ...receipt, alreadyImported: true });
      assert.doesNotMatch(cli.stderr, /opaque-encrypted-sentinel/);
      const reorderedKeys = JSON.parse(JSON.stringify(snapshot)) as RoutingSnapshot;
      reorderedKeys.tables.combos[0] = Object.fromEntries(
        Object.entries(reorderedKeys.tables.combos[0]).reverse()
      );
      assert.equal((await importRoutingSnapshot(db, reorderedKeys)).digest, receipt.digest);
      const imported = await combos.findById("combo");
      assert.equal(imported?.id, "combo");
      assert.equal(imported?.sortOrder, 1.5);
      const { rows } = await db.transaction((tx) =>
        tx.query(
          "SELECT value FROM omniroute.key_value WHERE namespace='lkgp' AND key='Imported:gpt-4'"
        )
      );
      assert.deepEqual(rows, [{ value: "opaque-encrypted-sentinel" }]);
      assert.deepEqual(
        (
          await db.transaction((tx) =>
            tx.query(
              "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGenerationExtra'"
            )
          )
        ).rows,
        [{ value: "opaque-nearby-sentinel" }]
      );
      assert.equal(
        (
          await db.transaction((tx) =>
            tx.query("SELECT count(*) AS n FROM omniroute.key_value WHERE namespace='bulk'")
          )
        ).rows[0].n,
        "201"
      );
      const identity = await db.transaction((tx) =>
        tx.query<{ id: string }>(
          "INSERT INTO omniroute.proxy_assignments(proxy_id,scope,scope_id,created_at,updated_at) VALUES('proxy','account','account','t','t') RETURNING id"
        )
      );
      assert.equal(snapshot.proxyAssignmentSequence, 120);
      assert.equal(identity.rows[0].id, "121");
      assert.equal(await combos.deleteById("combo"), true);
      assert.deepEqual(effects, ["proxy", "Imported:gpt-4"]);
      for (const table of ["combos", "model_combo_mappings", "proxy_scope_rotation"]) {
        assert.equal(
          (await db.transaction((tx) => tx.query(`SELECT count(*) AS n FROM omniroute.${table}`)))
            .rows[0].n,
          "0"
        );
      }
      const reread = tryOpenSync(join(directory, "storage.sqlite"), {
        readonly: true,
        fileMustExist: true,
      });
      assert.ok(reread);
      try {
        assert.equal(JSON.stringify(captureRoutingSnapshot(reread)), before);
      } finally {
        reread.close();
      }

      const laterTargetGeneration = JSON.stringify("00000000-0000-4000-8000-000000000001");
      assert.notEqual(laterTargetGeneration, targetGeneration[0]);
      await db.transaction(async (tx) => {
        await tx.query(
          "UPDATE omniroute.key_value SET value=$1 WHERE namespace='settings' AND key=$2",
          [laterTargetGeneration, AUTH_GENERATION_SETTING]
        );
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('lkgp','later-target-row','retained')"
        );
      });
      assert.deepEqual(await importRoutingSnapshot(db, snapshot), {
        ...receipt,
        alreadyImported: true,
      });
      assert.deepEqual(await targetGenerationValues(db), [laterTargetGeneration]);
      const sourceOnlyGenerationChange = JSON.parse(before) as RoutingSnapshot;
      sourceGenerationRows(sourceOnlyGenerationChange)[0].value = JSON.stringify(
        "00000000-0000-4000-8000-000000000002"
      );
      await assert.rejects(
        importRoutingSnapshot(db, sourceOnlyGenerationChange),
        /different import/
      );

      const changed = JSON.parse(before) as RoutingSnapshot;
      changed.tables.combos[0].name = "different";
      await assert.rejects(importRoutingSnapshot(db, changed), /different import/);
      await db.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      await migratePostgresSchema(db);
      changed.tables.model_combo_mappings[0].combo_id = "missing-parent";
      await assert.rejects(importRoutingSnapshot(db, changed));
      assert.equal(
        (await db.transaction((tx) => tx.query("SELECT count(*) AS n FROM omniroute.combos")))
          .rows[0].n,
        "0"
      );
      assert.equal(
        (
          await db.transaction((tx) =>
            tx.query("SELECT count(*) AS n FROM omniroute.routing_imports")
          )
        ).rows[0].n,
        "0"
      );
      await assertNoRoutingImport(db, 1);
      const pristineGeneration = await targetGenerationValues(db);
      assert.equal(pristineGeneration.length, 1);
      for (const malformed of [
        (input: RoutingSnapshot) => {
          sourceGenerationRows(input)[0].value = "not-json";
        },
        (input: RoutingSnapshot) => {
          sourceGenerationRows(input)[0].value = JSON.stringify("not-a-uuid");
        },
        (input: RoutingSnapshot) => {
          sourceGenerationRows(input)[0].value = JSON.stringify(null);
        },
        (input: RoutingSnapshot) => {
          input.tables.key_value.push({ ...sourceGenerationRows(input)[0] });
        },
      ]) {
        const invalid = JSON.parse(before) as RoutingSnapshot;
        malformed(invalid);
        await assert.rejects(importRoutingSnapshot(db, invalid), /Invalid routing snapshot/);
        assert.deepEqual(await targetGenerationValues(db), pristineGeneration);
        await assertNoRoutingImport(db, 1);
      }
      await db.transaction((tx) =>
        tx.query(
          "UPDATE omniroute.key_value SET value='not-json' WHERE namespace='settings' AND key=$1",
          [AUTH_GENERATION_SETTING]
        )
      );
      await assert.rejects(
        importRoutingSnapshot(db, snapshot),
        /invalid management generation seed/
      );
      await assertNoRoutingImport(db, 1);
      assert.deepEqual(await targetGenerationValues(db), ["not-json"]);
      await db.transaction((tx) =>
        tx.query("UPDATE omniroute.key_value SET value=$1 WHERE namespace='settings' AND key=$2", [
          pristineGeneration[0],
          AUTH_GENERATION_SETTING,
        ])
      );
      await db.transaction((tx) =>
        tx.query("DELETE FROM omniroute.key_value WHERE namespace='settings' AND key=$1", [
          AUTH_GENERATION_SETTING,
        ])
      );
      await assert.rejects(importRoutingSnapshot(db, snapshot), /requires an empty target/);
      await assertNoRoutingImport(db, 0);
      await db.transaction((tx) =>
        tx.query("INSERT INTO omniroute.key_value(namespace,key,value) VALUES('settings',$1,$2)", [
          AUTH_GENERATION_SETTING,
          pristineGeneration[0],
        ])
      );
      await db.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('settings','operator-setting','retained')"
        )
      );
      await assert.rejects(importRoutingSnapshot(db, snapshot), /empty target/);
      await assertNoRoutingImport(db, 2);
      assert.equal((await targetGenerationValues(db))[0], pristineGeneration[0]);
      assert.deepEqual(
        (
          await db.transaction((tx) =>
            tx.query("SELECT value FROM omniroute.key_value WHERE key='operator-setting'")
          )
        ).rows,
        [{ value: "retained" }]
      );
      await db.transaction((tx) =>
        tx.query("DELETE FROM omniroute.key_value WHERE key='operator-setting'")
      );
      await combos.create({ name: "existing", models: [] });
      await assert.rejects(importRoutingSnapshot(db, snapshot), /empty target/);
      const extra = JSON.parse(before) as RoutingSnapshot;
      extra.tables.combos[0].unmapped_field = "must not lose";
      await assert.rejects(importRoutingSnapshot(db, extra), /column mismatch/);
      await resetRoutingTarget(db);
      const legacy = JSON.parse(before) as RoutingSnapshot;
      legacy.tables.key_value = legacy.tables.key_value.filter(
        (record) => record.namespace !== "settings" || record.key !== AUTH_GENERATION_SETTING
      );
      const legacyTargetGeneration = await targetGenerationValues(db);
      const legacyReceipt = await importRoutingSnapshot(db, legacy);
      assert.equal(legacyReceipt.alreadyImported, false);
      assert.equal(legacyReceipt.rowCount, sourceRowCount(legacy));
      assert.equal(legacyReceipt.rowCount, sourceRowCount(snapshot) - 1);
      assert.deepEqual(await targetGenerationValues(db), legacyTargetGeneration);
      assert.deepEqual(await importRoutingSnapshot(db, legacy), {
        ...legacyReceipt,
        alreadyImported: true,
      });

      await resetRoutingTarget(db);
      const equalTargetGeneration = await targetGenerationValues(db);
      const equalEpoch = JSON.parse(before) as RoutingSnapshot;
      sourceGenerationRows(equalEpoch)[0].value = equalTargetGeneration[0];
      const equalObservation = observeQueries(db);
      const equalReceipt = await importRoutingSnapshot(equalObservation.database, equalEpoch);
      assert.equal(equalReceipt.alreadyImported, false);
      assert.equal(equalReceipt.rowCount, sourceRowCount(snapshot));
      assert.deepEqual(await targetGenerationValues(db), equalTargetGeneration);
      const equalGenerationWrites = equalObservation.queries.filter(({ text }) =>
        /^\s*(?:INSERT|UPDATE|DELETE|TRUNCATE)\b[\s\S]*\bomniroute\.key_value\b/i.test(text)
      );
      const equalInsertedRows = equalGenerationWrites.flatMap(({ values }) => {
        const bound = values ?? [];
        const rows: unknown[][] = [];
        for (let index = 0; index < bound.length; index += 3)
          rows.push(bound.slice(index, index + 3));
        return rows;
      });
      assert.equal(
        equalInsertedRows.some(
          ([namespace, key]) => namespace === "settings" && key === AUTH_GENERATION_SETTING
        ),
        false
      );

      await resetRoutingTarget(db);
      const replyLossGeneration = await targetGenerationValues(db);
      assert.equal(replyLossGeneration.length, 1);
      const proxy = await createPostgresReplyLossProxy({
        upstreamPort: Number(process.env.OMNIROUTE_TEST_POSTGRES_PORT),
        certificate: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_CA!, "utf8"),
        key: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_KEY!, "utf8"),
        dropCommit: 2,
      });
      try {
        await assert.rejects(
          promisify(execFile)(
            process.execPath,
            [
              "--import",
              "tsx/esm",
              "scripts/ops/postgres-import-routing.ts",
              "--source",
              join(directory, "storage.sqlite"),
            ],
            {
              timeout: 15_000,
              env: {
                ...process.env,
                OMNIROUTE_POSTGRES_HOST: "localhost",
                OMNIROUTE_POSTGRES_PORT: String(proxy.port),
                OMNIROUTE_POSTGRES_DATABASE: "omniroute_test",
                OMNIROUTE_POSTGRES_USER: "omniroute_test",
                OMNIROUTE_POSTGRES_PASSWORD: "omniroute_test",
                OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
              },
            }
          ),
          (error: Error & { stderr: string }) => {
            assert.match(error.stderr, /commit outcome is unknown/);
            assert.match(error.stderr, /same offline source/);
            assert.doesNotMatch(error.stderr, /opaque-encrypted-sentinel/);
            return true;
          }
        );
        assert.equal(proxy.dropped(), true);
        assert.deepEqual(await importRoutingSnapshot(db, snapshot), {
          ...receipt,
          alreadyImported: true,
        });
        assert.deepEqual(await targetGenerationValues(db), replyLossGeneration);
        assert.equal(
          (
            await db.transaction((tx) =>
              tx.query<{ row_count: number }>("SELECT row_count FROM omniroute.routing_imports")
            )
          ).rows[0].row_count,
          sourceRowCount(snapshot)
        );
        assert.equal(
          (
            await db.transaction((tx) =>
              tx.query("SELECT count(*) AS n FROM omniroute.key_value WHERE namespace='bulk'")
            )
          ).rows[0].n,
          "201"
        );
      } finally {
        await proxy.close();
      }
    } finally {
      core.resetDbInstance();
      restoreBackend();
      await db.close();
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
