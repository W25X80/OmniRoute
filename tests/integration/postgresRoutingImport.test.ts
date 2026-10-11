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
import { tryOpenSync } from "../../src/lib/db/adapters/driverFactory.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";

test(
  "offline SQLite routing snapshot imports atomically, preserves source and reconciles receipts",
  { skip: !postgresTestsEnabled, timeout: 30_000 },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "omniroute-routing-import-"));
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
      const receipt = await importRoutingSnapshot(db, snapshot);
      assert.equal(receipt.alreadyImported, false);
      assert.match(receipt.digest, /^[0-9a-f]{64}$/);
      assert.equal(JSON.stringify(snapshot), before);
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
      await combos.create({ name: "existing", models: [] });
      await assert.rejects(importRoutingSnapshot(db, snapshot), /empty target/);
      const extra = JSON.parse(before) as RoutingSnapshot;
      extra.tables.combos[0].unmapped_field = "must not lose";
      await assert.rejects(importRoutingSnapshot(db, extra), /column mismatch/);
      await db.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      await migratePostgresSchema(db);
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
      await db.close();
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
