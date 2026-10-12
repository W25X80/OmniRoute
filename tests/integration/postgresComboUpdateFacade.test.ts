import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function deferred() {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

test(
  "native combo facade commits model changes and session-pin cleanup together",
  { skip: !postgresTestsEnabled, timeout: 60_000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "combo-update-postgres-"));
    const values = {
      DATA_DIR: directory,
      OMNIROUTE_DATABASE_BACKEND: "postgres",
      STORAGE_ENCRYPTION_KEY: "test-only-combo-update-encryption-key",
      JWT_SECRET: "test-only-combo-update-jwt-secret",
      API_KEY_SECRET: "test-only-combo-update-api-secret",
    };
    const previous = new Map(Object.keys(values).map((name) => [name, process.env[name]]));
    Object.assign(process.env, values);
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { withPostgresCatalogSnapshot, catalogGeneration } =
      await import("../../src/lib/db/postgres/catalogSnapshot.ts");
    const { createCombo, updateCombo } = await import("../../src/lib/db/combos.ts");
    const { recordSessionModelUsageAsync } = await import("../../src/lib/db/contextHandoffs.ts");
    const { getCombosCacheVersion, getModelCatalogCacheVersion } =
      await import("../../src/lib/db/readCache.ts");
    const observer = await createTestPostgresDatabase({ maxConnections: 4 });
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;

    const selected = <T>(body: (binding: PostgresControlPlaneBinding) => Promise<T>) =>
      plane!.run((binding) => withPostgresCatalogSnapshot(binding.database!, () => body(binding)));
    const seed = (id: string, name: string) =>
      selected(() =>
        createCombo({ id, name, models: ["openai/gpt-4.1"], context_cache_protection: true })
      );
    const pin = (session: string, name: string, model = "openai/gpt-4.1") =>
      selected(() => recordSessionModelUsageAsync(session, name, model, "openai"));
    const snapshot = (id: string, names: string[]) =>
      observer.transaction(async (tx) => {
        const { rows } = await tx.query<{ name: string; data: string; epoch: string }>(
          `SELECT name,data,
           (SELECT generation::text FROM omniroute.api_key_policy_epoch WHERE singleton) AS epoch
           FROM omniroute.combos WHERE id=$1`,
          [id]
        );
        const pins = await tx.query<{ session_id: string; combo_name: string; model_str: string }>(
          `SELECT session_id,combo_name,model_str FROM omniroute.session_model_history
           WHERE combo_name=ANY($1::text[]) ORDER BY id`,
          [names]
        );
        return { combo: rows[0], pins: pins.rows };
      });
    async function waitForLock(id: number) {
      for (let attempt = 0; attempt < 100; attempt++) {
        const { rows } = await observer.transaction((tx) =>
          tx.query<{ count: string }>(
            `SELECT count(*) FROM pg_locks WHERE locktype='advisory'
             AND classid=18765 AND objid=$1 AND mode='ExclusiveLock' AND NOT granted`,
            [id]
          )
        );
        if (Number(rows[0].count) > 0) return;
        await delay(10);
      }
      assert.fail(`No real PostgreSQL waiter reached advisory lock ${id}`);
    }

    try {
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 6, lockTimeoutMs: 3_000 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });

      await t.test("a cleanup rejection rolls back combo, pins and epoch", async () => {
        const id = "df001-rollback";
        const names = ["df001-rename-source", "df001-rename-target"];
        await seed(id, names[0]);
        await pin("df001-old", names[0]);
        await pin("df001-target", names[1]);
        const before = await snapshot(id, names);
        const comboVersion = getCombosCacheVersion();
        const catalogVersion = getModelCatalogCacheVersion();
        await observer.transaction(async (tx) => {
          await tx.query(`CREATE FUNCTION omniroute.reject_combo_pin_update_test()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
              IF OLD.combo_name='df001-rename-target' THEN
                RAISE EXCEPTION 'isolated pin cleanup rejection' USING ERRCODE='23514';
              END IF;
              RETURN OLD;
            END $$`);
          await tx.query(`CREATE TRIGGER reject_combo_pin_update_test BEFORE DELETE
            ON omniroute.session_model_history FOR EACH ROW
            EXECUTE FUNCTION omniroute.reject_combo_pin_update_test()`);
        });
        try {
          await selected(async () => {
            const generation = catalogGeneration();
            await assert.rejects(
              updateCombo(id, { name: names[1], models: ["openai/gpt-4.1-mini"] }),
              (error: PostgresError) => error.kind === "query_failed" && error.sqlState === "23514"
            );
            assert.equal(catalogGeneration(), generation, "failed write cannot refresh the view");
          });
          assert.deepEqual(await snapshot(id, names), before);
          assert.equal(getCombosCacheVersion(), comboVersion);
          assert.equal(getModelCatalogCacheVersion(), catalogVersion);
        } finally {
          await observer.transaction(async (tx) => {
            await tx.query(
              "DROP TRIGGER reject_combo_pin_update_test ON omniroute.session_model_history"
            );
            await tx.query("DROP FUNCTION omniroute.reject_combo_pin_update_test()");
          });
        }
      });

      await t.test("metadata and rename cleanup retain source semantics", async () => {
        const id = "df001-preserve";
        const names = ["df001-preserve-source", "df001-preserve-target"];
        await seed(id, names[0]);
        await pin("df001-preserve-old", names[0]);
        await pin("df001-preserve-new", names[1]);
        const before = await snapshot(id, names);
        await selected(() => updateCombo(id, { description: "metadata only" }));
        assert.deepEqual((await snapshot(id, names)).pins, before.pins);
        await selected(async () => {
          const generation = BigInt(catalogGeneration());
          await updateCombo(id, { name: names[1], models: null });
          assert.equal(BigInt(catalogGeneration()), generation + 1n);
        });
        assert.deepEqual((await snapshot(id, names)).pins, []);

        const emptyId = "df001-empty-source";
        const emptyNames = ["df001-empty-old", "df001-empty-target"];
        await seed(emptyId, emptyNames[0]);
        await pin("df001-target-only", emptyNames[1]);
        const targetOnly = (await snapshot(emptyId, emptyNames)).pins;
        await selected(() => updateCombo(emptyId, { name: emptyNames[1], models: null }));
        assert.deepEqual((await snapshot(emptyId, emptyNames)).pins, targetOnly);
      });

      await t.test("a concurrent pin waits for cleanup and survives commit", async () => {
        const id = "df001-concurrent";
        const name = "df001-concurrent-combo";
        await seed(id, name);
        await pin("df001-concurrent-old", name);
        const before = await snapshot(id, [name]);
        await observer.transaction(async (tx) => {
          await tx.query(`CREATE FUNCTION omniroute.pause_combo_pin_update_test()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
              PERFORM pg_advisory_xact_lock(18765,90);
              RETURN OLD;
            END $$`);
          await tx.query(`CREATE TRIGGER pause_combo_pin_update_test BEFORE DELETE
            ON omniroute.session_model_history FOR EACH ROW
            EXECUTE FUNCTION omniroute.pause_combo_pin_update_test()`);
        });
        const ready = deferred(),
          release = deferred();
        const held = observer.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,90)");
          ready.resolve();
          await release.promise;
        });
        void held.catch(ready.reject);
        let updating: Promise<unknown> | undefined;
        let recording: Promise<void> | undefined;
        try {
          await ready.promise;
          updating = selected(() => updateCombo(id, { models: ["openai/gpt-4.1-mini"] }));
          void updating.catch(() => {});
          await waitForLock(90);
          assert.deepEqual(await snapshot(id, [name]), before, "neither state is committed yet");
          recording = pin("df001-concurrent-new", name, "openai/gpt-4.1-mini");
          void recording.catch(() => {});
          await waitForLock(13);
          release.resolve();
          await Promise.all([held, updating, recording]);
          const after = await snapshot(id, [name]);
          assert.equal(BigInt(after.combo.epoch), BigInt(before.combo.epoch) + 1n);
          assert.deepEqual(after.pins, [
            {
              session_id: "df001-concurrent-new",
              combo_name: name,
              model_str: "openai/gpt-4.1-mini",
            },
          ]);
          assert.notEqual(after.combo.data, before.combo.data);
        } finally {
          release.resolve();
          await Promise.allSettled([held, updating, recording]);
          await observer.transaction(async (tx) => {
            await tx.query(
              "DROP TRIGGER pause_combo_pin_update_test ON omniroute.session_model_history"
            );
            await tx.query("DROP FUNCTION omniroute.pause_combo_pin_update_test()");
          });
        }
      });
    } finally {
      await plane?.close();
      await observer.close();
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      await rm(directory, { recursive: true, force: true });
    }
  }
);
