import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import type {
  ComboRepository,
  ModelComboMappingRepository,
} from "../../src/domain/persistence/comboRepositories.ts";
import { ComboInvariantError } from "../../src/lib/combos/invariants.ts";
import {
  createPostgresComboRepository,
  PostgresComboEffectsError,
  type PostgresComboEffects,
} from "../../src/lib/db/postgres/comboRepository.ts";
import { PostgresError, type PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresModelComboMappingRepository } from "../../src/lib/db/postgres/modelComboMappingRepository.ts";
import { registerComboRepositoryConformance } from "../helpers/persistence/comboRepositoryConformance.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

// The runner supplies a disposable TLS server; these tests never use application credentials.
// Application cache callbacks must be configured by its composition root. Tests use explicit
// inert callbacks except where committed-state and rollback behavior are the asserted oracle.
const noEffects: PostgresComboEffects = {
  bumpProxyRegistryGeneration() {},
  invalidateCachedLKGP() {},
};
let database: PostgresDatabase;
let combos: ComboRepository;
let mappings: ModelComboMappingRepository;

async function resetStorage(): Promise<void> {
  await database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 2)");
    await transaction.query(`TRUNCATE omniroute.reasoning_routing_rules, omniroute.model_combo_mappings, omniroute.combos,
      omniroute.key_value, omniroute.proxy_assignments, omniroute.proxy_scope_rotation,
      omniroute.proxy_registry RESTART IDENTITY`);
  });
}

async function putRawCombo(id: string, name: string, data: string, order = 1, contextCache = 0) {
  const now = "2026-10-11T01:02:03.456Z";
  await database.transaction((transaction) =>
    transaction.query(
      `INSERT INTO omniroute.combos
     (id,name,data,sort_order,created_at,updated_at,context_cache_protection)
     VALUES($1,$2,$3,$4,$5,$5,$6)`,
      [id, name, data, order, now, contextCache]
    )
  );
}

async function seedDeletionState(comboId: string, name: string) {
  await mappings.create({ pattern: "*", comboId });
  await database.transaction(async (transaction) => {
    await transaction.query(`INSERT INTO omniroute.proxy_registry
      (id,name,type,host,port,created_at,updated_at)
      VALUES ('proxy','proxy','http','127.0.0.1',8080,'created','updated')`);
    await transaction.query(
      `INSERT INTO omniroute.proxy_assignments
      (proxy_id,scope,scope_id,created_at,updated_at)
      VALUES ('proxy','combo',$1,'created','updated'),
             ('proxy','provider','survivor','created','updated')`,
      [comboId]
    );
    await transaction.query(
      `INSERT INTO omniroute.proxy_scope_rotation
      (scope,scope_id,updated_at) VALUES ('combo',$1,'updated'),('provider','survivor','updated')`,
      [comboId]
    );
    await transaction.query(
      `INSERT INTO omniroute.key_value(namespace,key,value)
      VALUES ('lkgp',$1,'value'),('lkgp',$2,'value'),('lkgp',$3,'value'),('other',$1,'value')`,
      [`${name}:model-a`, `${name.toUpperCase()}:model-b`, `${name}x:model-a`]
    );
  });
}

async function deletionSnapshot(comboId: string) {
  return (
    await database.transaction((transaction) =>
      transaction.query<{
        combos: string;
        mappings: string;
        assignments: string;
        rotations: string;
      }>(
        `SELECT
    (SELECT count(*) FROM omniroute.combos WHERE id=$1) AS combos,
    (SELECT count(*) FROM omniroute.model_combo_mappings WHERE combo_id=$1) AS mappings,
    (SELECT count(*) FROM omniroute.proxy_assignments WHERE scope='combo' AND scope_id=$1) AS assignments,
    (SELECT count(*) FROM omniroute.proxy_scope_rotation WHERE scope='combo' AND scope_id=$1) AS rotations`,
        [comboId]
      )
    )
  ).rows[0];
}

async function withQueuedMutations<T>(
  operations: () => Promise<T>[],
  expected: number
): Promise<T[]> {
  let release!: () => void;
  let acquired!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    acquired = resolve;
  });
  const owner = database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 2)");
    acquired();
    await gate;
  });
  let pending: Promise<T>[] = [];
  try {
    await ready;
    pending = operations();
    let blocked = 0;
    for (let attempt = 0; attempt < 50 && blocked < expected; attempt++) {
      const result = await database.transaction((transaction) =>
        transaction.query<{ count: string }>(
          `SELECT count(*) AS count FROM pg_locks
         WHERE locktype='advisory' AND classid=18765 AND objid=2
         AND mode='ExclusiveLock' AND NOT granted`
        )
      );
      blocked = Number(result.rows[0].count);
      if (blocked < expected) await delay(10);
    }
    assert.ok(blocked >= expected, "all contenders must reach the real PostgreSQL lock barrier");
    release();
    await owner;
    return await Promise.all(pending);
  } finally {
    release();
    await owner;
    await Promise.allSettled(pending);
  }
}

if (!postgresTestsEnabled) {
  test(
    "PostgreSQL combo repositories require the isolated integration runner",
    { skip: true },
    () => {}
  );
} else {
  test.before(async () => {
    database = await createTestPostgresDatabase({ maxConnections: 6, lockTimeoutMs: 2_000 });
    await migratePostgresSchema(database);
    combos = createPostgresComboRepository(database, noEffects);
    mappings = createPostgresModelComboMappingRepository(database);
  });
  test.after(async () => {
    await database?.close();
  });

  registerComboRepositoryConformance(async () => ({
    combos,
    mappings,
    reset: resetStorage,
    async corruptComboPayload(comboId) {
      await database.transaction((transaction) =>
        transaction.query("UPDATE omniroute.combos SET data=$1 WHERE id=$2", ["", comboId])
      );
    },
  }));

  test("PostgreSQL constraints and invariant failures preserve the previous routing state", async () => {
    await resetStorage();
    await assert.rejects(
      combos.create({
        name: "invalid",
        allowedProviders: ["openai"],
        models: ["anthropic/claude-sonnet"],
      }),
      ComboInvariantError
    );
    assert.equal(await combos.count(), 0);
    const combo = await combos.create({
      id: "fixed",
      name: "valid",
      models: ["openai/gpt-4o"],
      allowedProviders: ["openai"],
    });
    await assert.rejects(
      combos.update("fixed", { models: ["anthropic/claude-sonnet"] }),
      ComboInvariantError
    );
    assert.deepEqual(await combos.findById("fixed"), combo);
    for (const data of [{ name: "valid" }, { id: "fixed", name: "another" }]) {
      await assert.rejects(
        combos.create(data),
        (error: PostgresError) => error.sqlState === "23505"
      );
    }
    await assert.rejects(
      mappings.create({ pattern: "*", comboId: "missing" }),
      (error: PostgresError) => error.sqlState === "23503"
    );
    assert.equal((await mappings.list()).total, 0);
    const renamed = await combos.update("fixed", { name: "renamed", models: null });
    assert.equal(renamed?.previousName, "valid");
    assert.equal(renamed?.currentName, "renamed");
    assert.equal(renamed?.modelsFieldProvided, true);
    assert.deepEqual(renamed?.combo.models, []);
    assert.equal((await combos.update("fixed", { name: null }))?.combo.name, "renamed");
  });

  test("PostgreSQL preserves row authority, context-cache precedence and page-local normalization", async () => {
    await resetStorage();
    await putRawCombo(
      "child-row",
      "child",
      JSON.stringify({ id: "stale", name: "child", models: [] })
    );
    await putRawCombo(
      "parent-row",
      "parent",
      JSON.stringify({
        id: "stale",
        name: "parent",
        models: ["child"],
        context_cache_protection: true,
      }),
      2
    );
    const parent = await combos.findById("parent-row");
    assert.equal(parent?.id, "parent-row");
    assert.equal(parent?.sortOrder, 2);
    assert.equal(parent?.context_cache_protection, true);
    assert.equal((parent?.models as Array<{ kind: string }>)[0].kind, "combo-ref");
    const page = await combos.list(1, 1);
    assert.equal((page[0].models as Array<{ kind: string }>)[0].kind, "model");
    await putRawCombo(
      "enabled-row",
      "enabled",
      JSON.stringify({ name: "enabled", models: [], context_cache_protection: false }),
      3,
      1
    );
    assert.equal((await combos.findByName("enabled"))?.context_cache_protection, true);
    await mappings.create({ pattern: "*", comboId: "parent-row" });
    const resolved = await mappings.resolveForModel("anything");
    assert.equal(resolved?.id, "stale");
    assert.deepEqual(resolved?.models, ["child"]);
    assert.equal(resolved?.version, undefined);
  });

  test("PostgreSQL ASCII case folding, exact uniqueness, and insertion ties match SQLite", async () => {
    await resetStorage();
    await combos.create({ id: "z-id", name: "Alpha", sortOrder: 0.25 });
    await combos.create({ id: "a-id", name: "alpha", sortOrder: 0.25 });
    await combos.create({ name: "Zebra", sortOrder: 0.25 });
    await combos.create({ name: "Äpfel", sortOrder: 0.25 });
    assert.deepEqual(
      (await combos.list()).map((combo) => combo.name),
      ["Alpha", "alpha", "Zebra", "Äpfel"]
    );
    assert.equal((await combos.findByNameInsensitive("ALPHA"))?.id, "z-id");
    assert.equal(await combos.findByNameInsensitive("äpfel"), null);
    assert.equal((await combos.findByName("alpha"))?.id, "a-id");
    assert.equal((await combos.list(-1, -1)).length, 4);
    const created = await mappings.create({
      pattern: "gpt-?.[x]*",
      comboId: "z-id",
      priority: 3.25,
    });
    assert.equal(created.comboName, undefined);
    assert.equal(created.priority, 3.25);
    assert.match(created.createdAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    assert.equal((await mappings.findById(created.id))?.createdAt, created.createdAt);
    assert.equal((await mappings.resolveForModel("GPT-a.[x]suffix"))?.id, "z-id");
    assert.equal(await mappings.resolveForModel("gpt-aa.[x]suffix"), null);
    const page = await mappings.list({ limit: 0, offset: 50 });
    assert.deepEqual(page, { items: [], total: 1 });
    assert.equal((await mappings.list({ limit: -1, offset: -2 })).items.length, 1);
    await assert.rejects(
      combos.create({ name: "nan-order", sortOrder: NaN }),
      (error: PostgresError) => error.sqlState === "23502"
    );
    await mappings.update(created.id, { priority: NaN });
    // better-sqlite3 binds NaN as SQL NULL; a PostgreSQL floating NaN must not outrank it.
    assert.equal((await mappings.findById(created.id))?.priority, null);
  });

  test("PostgreSQL malformed JSON aborts combo reads and reorder without partial writes", async () => {
    await resetStorage();
    const healthy = await combos.create({ name: "healthy", sortOrder: 3 });
    await putRawCombo("broken", "broken", "{", 4);
    await assert.rejects(combos.findById("broken"), SyntaxError);
    await assert.rejects(combos.list(), SyntaxError);
    await assert.rejects(combos.reorder([String(healthy.id), "broken"]), SyntaxError);
    assert.equal((await combos.findById(String(healthy.id)))?.sortOrder, 3);
    await mappings.create({ pattern: "*", comboId: "broken", priority: 20 });
    await mappings.create({ pattern: "*", comboId: String(healthy.id), priority: 10 });
    assert.equal((await mappings.resolveForModel("gpt"))?.name, "healthy");
  });

  test("PostgreSQL queued creates reserve unique default order and partial updates cannot lose fields", async () => {
    await resetStorage();
    const created = await withQueuedMutations(
      () => [combos.create({ name: "first" }), combos.create({ name: "second" })],
      2
    );
    assert.deepEqual(created.map((combo) => combo.sortOrder).sort(), [1, 2]);
    const id = String(created[0].id);
    await withQueuedMutations(
      () => [
        combos.update(id, { description: "preserved" }),
        combos.update(id, { strategy: "round-robin" }),
      ],
      2
    );
    const updated = await combos.findById(id);
    assert.equal(updated?.description, "preserved");
    assert.equal(updated?.strategy, "round-robin");
    await withQueuedMutations<unknown>(
      () => [combos.reorder([String(created[1].id), id]), combos.create({ name: "third" })],
      2
    );
    assert.deepEqual(
      (await combos.list()).map((combo) => combo.sortOrder),
      [1, 2, 3]
    );
  });

  test("PostgreSQL mapping partial updates serialize with combo mutation", async () => {
    await resetStorage();
    const combo = await combos.create({ name: "mapping-target" });
    const mapping = await mappings.create({ pattern: "old", comboId: String(combo.id) });
    await withQueuedMutations(
      () => [
        mappings.update(mapping.id, { pattern: "new" }),
        mappings.update(mapping.id, { description: "preserved", enabled: false }),
      ],
      2
    );
    const updated = await mappings.findById(mapping.id);
    assert.equal(updated?.pattern, "new");
    assert.equal(updated?.description, "preserved");
    assert.equal(updated?.enabled, false);
  });

  test("PostgreSQL combo delete commits mappings/proxy/rotation/LKGP cleanup before cache effects", async () => {
    await resetStorage();
    const combo = await combos.create({ name: "temp_%\\a" });
    const id = String(combo.id);
    await seedDeletionState(id, String(combo.name));
    const effects: string[] = [];
    const repository = createPostgresComboRepository(database, {
      async bumpProxyRegistryGeneration() {
        assert.deepEqual(await deletionSnapshot(id), {
          combos: "0",
          mappings: "0",
          assignments: "0",
          rotations: "0",
        });
        effects.push("generation");
      },
      async invalidateCachedLKGP(key) {
        assert.equal(
          (
            await database.transaction((transaction) =>
              transaction.query(
                "SELECT key FROM omniroute.key_value WHERE namespace='lkgp' AND key=$1",
                [key]
              )
            )
          ).rows.length,
          0
        );
        effects.push(key);
      },
    });
    assert.equal(await repository.deleteById(id), true);
    assert.equal(effects[0], "generation");
    assert.deepEqual(effects.slice(1).sort(), ["TEMP_%\\A:model-b", "temp_%\\a:model-a"].sort());
    const survivors = await database.transaction((transaction) =>
      transaction.query("SELECT namespace,key FROM omniroute.key_value ORDER BY namespace,key")
    );
    assert.deepEqual(survivors.rows, [
      { namespace: "lkgp", key: "temp_%\\ax:model-a" },
      { namespace: "other", key: "temp_%\\a:model-a" },
    ]);
    assert.equal(
      (
        await database.transaction((transaction) =>
          transaction.query(
            "SELECT scope_id FROM omniroute.proxy_assignments WHERE scope='provider'"
          )
        )
      ).rows[0].scope_id,
      "survivor"
    );
    assert.equal(
      (
        await database.transaction((transaction) =>
          transaction.query(
            "SELECT scope_id FROM omniroute.proxy_scope_rotation WHERE scope='provider'"
          )
        )
      ).rows[0].scope_id,
      "survivor"
    );
    assert.equal(await repository.deleteById(id), false);
    assert.equal(effects.length, 3);
  });

  test("PostgreSQL committed deletion attempts all cache effects and reports callback failure", async () => {
    await resetStorage();
    const combo = await combos.create({ name: "effects" });
    const id = String(combo.id);
    await seedDeletionState(id, "effects");
    const attempted: string[] = [];
    const repository = createPostgresComboRepository(database, {
      async bumpProxyRegistryGeneration() {
        attempted.push("generation");
        throw new Error("private callback details");
      },
      async invalidateCachedLKGP(key) {
        attempted.push(key);
        if (key === "effects:model-a") throw new Error("private callback details");
      },
    });
    await assert.rejects(repository.deleteById(id), (error: PostgresComboEffectsError) => {
      assert.equal(error.committed, true);
      assert.deepEqual(error.failedEffects, ["proxy_registry", "lkgp"]);
      assert.equal(error.cause, undefined);
      assert.doesNotMatch(error.message, /private callback/);
      return true;
    });
    assert.deepEqual(attempted.sort(), ["generation", "effects:model-a", "EFFECTS:model-b"].sort());
    assert.deepEqual(await deletionSnapshot(id), {
      combos: "0",
      mappings: "0",
      assignments: "0",
      rotations: "0",
    });
    assert.equal(await repository.deleteById(id), false);
    assert.equal(attempted.length, 3);
  });

  test("PostgreSQL cleanup rejection rolls back every collateral row and emits no cache effects", async () => {
    await resetStorage();
    const combo = await combos.create({ name: "rollback" });
    const id = String(combo.id);
    await seedDeletionState(id, "rollback");
    const effects: string[] = [];
    const repository = createPostgresComboRepository(database, {
      bumpProxyRegistryGeneration() {
        effects.push("generation");
      },
      invalidateCachedLKGP(key) {
        effects.push(key);
      },
    });
    await database.transaction(async (transaction) => {
      await transaction.query(`CREATE FUNCTION omniroute.reject_combo_cleanup_test() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test cleanup rejection'; END $$`);
      await transaction.query(`CREATE TRIGGER reject_combo_cleanup_test BEFORE DELETE
        ON omniroute.key_value FOR EACH ROW EXECUTE FUNCTION omniroute.reject_combo_cleanup_test()`);
    });
    try {
      await assert.rejects(
        repository.deleteById(id),
        (error: PostgresError) => error.sqlState === "P0001"
      );
      assert.deepEqual(await deletionSnapshot(id), {
        combos: "1",
        mappings: "1",
        assignments: "1",
        rotations: "1",
      });
      assert.deepEqual(effects, []);
      assert.equal(
        (
          await database.transaction((transaction) =>
            transaction.query("SELECT key FROM omniroute.key_value WHERE namespace='lkgp'")
          )
        ).rows.length,
        3
      );
    } finally {
      await database.transaction(async (transaction) => {
        await transaction.query("DROP TRIGGER reject_combo_cleanup_test ON omniroute.key_value");
        await transaction.query("DROP FUNCTION omniroute.reject_combo_cleanup_test()");
      });
    }
  });
}
