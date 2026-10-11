import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import type {
  ProviderConnectionRepository,
  ProviderNodeRepository,
} from "../../src/domain/persistence/providerRepositories.ts";
import { PostgresError, type PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresProviderNodeRepository } from "../../src/lib/db/postgres/providerNodesRepository.ts";
import {
  createPostgresProviderConnectionRepository,
  PostgresProviderEffectsError,
  type PostgresProviderEffects,
} from "../../src/lib/db/postgres/providerRepository.ts";
import { registerProviderRepositoryConformance } from "../helpers/persistence/providerRepositoryConformance.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

// These instrumented repositories test provider-owned rows. Adjacent durable state callbacks
// are explicit fixture stubs; application composition tests qualify their native implementation.
const fixtureEffects: PostgresProviderEffects = {
  async assertApiKeyIsNotManagementPassword() {},
  async reconcileCodexUsageHistory() {},
  async deleteConnectionState() {},
  afterConnectionWrite() {},
  afterNodeWrite() {},
};

let database: PostgresDatabase;
let connections: ProviderConnectionRepository;
let nodes: ProviderNodeRepository;

async function resetStorage(): Promise<void> {
  await database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 4)");
    await transaction.query(
      "TRUNCATE omniroute.reasoning_routing_rules, omniroute.provider_connections, omniroute.provider_nodes, omniroute.proxy_assignments RESTART IDENTITY CASCADE"
    );
  });
}

async function queued<T>(operations: () => Promise<T>[], expected: number): Promise<T[]> {
  let release!: () => void;
  let ready!: () => void;
  const barrier = new Promise<void>((resolve) => {
    release = resolve;
  });
  const acquired = new Promise<void>((resolve) => {
    ready = resolve;
  });
  const owner = database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 4)");
    ready();
    await barrier;
  });
  let pending: Promise<T>[] = [];
  try {
    await acquired;
    pending = operations();
    let waiting = 0;
    for (let attempt = 0; attempt < 50 && waiting < expected; attempt++) {
      const result = await database.transaction((transaction) =>
        transaction.query<{ count: string }>(
          "SELECT count(*) AS count FROM pg_locks WHERE locktype='advisory' AND classid=18765 AND objid=4 AND mode='ExclusiveLock' AND NOT granted"
        )
      );
      waiting = Number(result.rows[0].count);
      if (waiting < expected) await delay(10);
    }
    assert.ok(
      waiting >= expected,
      "every contender must wait on the PostgreSQL provider mutation lock"
    );
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
    "PostgreSQL providers require the isolated TLS integration runner",
    { skip: true },
    () => {}
  );
} else {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-provider-conformance-encryption-key";
  test.before(async () => {
    database = await createTestPostgresDatabase({ maxConnections: 6, lockTimeoutMs: 2_000 });
    await migratePostgresSchema(database);
    connections = createPostgresProviderConnectionRepository(database, fixtureEffects);
    nodes = createPostgresProviderNodeRepository(database, fixtureEffects);
  });
  test.after(async () => {
    await database?.close();
  });

  registerProviderRepositoryConformance("PostgreSQL", async () => ({
    connections,
    nodes,
    reset: resetStorage,
    async updateStoredConnection(id, patch) {
      const allowed = new Set(["priority", "updated_at"]);
      const keys = Object.keys(patch);
      if (keys.some((key) => !allowed.has(key))) throw new Error("Unsupported fixture field");
      await database.transaction((transaction) =>
        transaction.query(
          `UPDATE omniroute.provider_connections SET ${keys.map((key, index) => `${key}=$${index + 1}`).join(",")} WHERE id=$${keys.length + 1}`,
          [...keys.map((key) => patch[key]), id]
        )
      );
    },
  }));

  test("PostgreSQL concurrent same-credential creates dedup after acquiring the mutation lock", async () => {
    await resetStorage();
    const created = await queued(
      () => [
        connections.create({
          provider: "openai",
          authType: "apikey",
          name: "first",
          apiKey: "same",
        }),
        connections.create({
          provider: "openai",
          authType: "apikey",
          name: "second",
          apiKey: "same",
        }),
      ],
      2
    );
    assert.equal(created[0]?.id, created[1]?.id);
    assert.equal(await connections.count(), 1);
    const distinct = await queued(
      () => [
        connections.create({ provider: "openai", authType: "apikey", apiKey: "distinct-a" }),
        connections.create({ provider: "openai", authType: "apikey", apiKey: "distinct-b" }),
      ],
      2
    );
    assert.notEqual(distinct[0]?.id, distinct[1]?.id);
    assert.deepEqual(
      (await connections.listRaw()).map((row) => row.priority),
      [1, 2, 3]
    );
  });

  test("PostgreSQL queued partial connection, provider-data and node patches retain both writers", async () => {
    await resetStorage();
    const created = await connections.create({
      provider: "openai",
      authType: "apikey",
      apiKey: "preserve",
    });
    const id = String(created?.id);
    const cipher = (await connections.listRaw())[0].apiKey;
    await queued(
      () => [
        connections.update(id, { name: "retained" }),
        connections.update(id, { group: "retained" }),
      ],
      2
    );
    assert.equal((await connections.findById(id))?.name, "retained");
    assert.equal((await connections.findById(id))?.group, "retained");
    assert.equal((await connections.listRaw())[0].apiKey, cipher);
    await queued(
      () => [
        connections.mergeProviderSpecificData(id, { first: 1 }),
        connections.mergeProviderSpecificData(id, { second: 2 }),
      ],
      2
    );
    assert.deepEqual((await connections.findById(id))?.providerSpecificData, {
      first: 1,
      second: 2,
    });
    await nodes.create({
      id: "node",
      type: "custom",
      name: "original",
      customHeaders: { stable: "value" },
    });
    await queued(
      () => [
        nodes.update("node", { name: "retained" }),
        nodes.update("node", { baseUrl: "https://example.test" }),
      ],
      2
    );
    const node = await nodes.findById("node");
    assert.equal(node?.name, "retained");
    assert.equal(node?.baseUrl, "https://example.test");
    assert.deepEqual(node?.customHeaders, { stable: "value" });
  });

  test("PostgreSQL durable callback rejection rolls back provider writes and emits no application effects", async () => {
    await resetStorage();
    let afterCalls = 0;
    const repository = createPostgresProviderConnectionRepository(database, {
      ...fixtureEffects,
      async reconcileCodexUsageHistory() {
        throw new Error("identity rejected");
      },
      async deleteConnectionState() {
        throw new Error("cleanup rejected");
      },
      afterConnectionWrite() {
        afterCalls++;
      },
    });
    const created = await connections.create({
      provider: "openai",
      authType: "apikey",
      name: "retained",
      apiKey: "retained",
    });
    const id = String(created?.id);
    await assert.rejects(repository.update(id, { name: "lost" }), /identity rejected/);
    assert.equal((await connections.findById(id))?.name, "retained");
    await assert.rejects(repository.deleteById(id), /cleanup rejected/);
    assert.equal(await connections.count(), 1);
    assert.equal(afterCalls, 0);
  });

  test("PostgreSQL committed effect failure is classified without replaying the provider mutation", async () => {
    await resetStorage();
    let calls = 0;
    const repository = createPostgresProviderConnectionRepository(database, {
      ...fixtureEffects,
      async afterConnectionWrite() {
        calls++;
        throw new Error("private failure");
      },
    });
    await assert.rejects(
      repository.create({ provider: "openai", authType: "apikey", apiKey: "one" }),
      (error: PostgresProviderEffectsError) => {
        assert.equal(error.committed, true);
        assert.equal(error.operation, "create");
        assert.doesNotMatch(error.message, /private failure/);
        return true;
      }
    );
    assert.equal(await connections.count(), 1);
    assert.equal(calls, 1);
  });

  test("PostgreSQL lightweight updates preserve credentials and perform no cache callbacks", async () => {
    await resetStorage();
    let calls = 0;
    const repository = createPostgresProviderConnectionRepository(database, {
      ...fixtureEffects,
      afterConnectionWrite() {
        calls++;
      },
    });
    const created = await repository.create({
      provider: "openai",
      authType: "apikey",
      apiKey: "stable",
    });
    const id = String(created?.id);
    const raw = (await repository.listRaw())[0];
    await repository.touchLastUsed(id, 7);
    await repository.touchSyncedModelsAt(id);
    const after = (await repository.listRaw())[0];
    assert.equal(after.apiKey, raw.apiKey);
    assert.equal(after.consecutiveUseCount, 7);
    assert.match(String(after.syncedModelsAt), /Z$/);
    assert.match(String(after.lastUsedAt), /Z$/);
    assert.equal(calls, 1);
  });

  test("PostgreSQL node constraints reject duplicate IDs and preserve prior header state", async () => {
    await resetStorage();
    await nodes.create({
      id: "fixed",
      type: "custom",
      name: "name",
      customHeaders: { stable: "value" },
    });
    await assert.rejects(
      nodes.create({ id: "fixed", type: "custom", name: "name" }),
      (error: PostgresError) => error.sqlState === "23505"
    );
    await assert.rejects(
      nodes.update("fixed", { name: null }),
      (error: PostgresError) => error.sqlState === "23502"
    );
    assert.deepEqual((await nodes.findById("fixed"))?.customHeaders, { stable: "value" });
  });

  test("PostgreSQL direct writes cannot bypass retired identity and tombstone policies", async () => {
    await resetStorage();
    const created = await connections.create({
      provider: "felo",
      authType: "apikey",
      apiKey: "retained",
    });
    const id = String(created?.id);
    await database.transaction((transaction) =>
      transaction.query(
        "UPDATE omniroute.provider_connections SET is_active=1,test_status='active' WHERE id=$1",
        [id]
      )
    );
    assert.equal((await connections.findById(id))?.isActive, false);
    await assert.rejects(
      database.transaction((transaction) =>
        transaction.query(
          "UPDATE omniroute.provider_connections SET provider='openai' WHERE id=$1",
          [id]
        )
      ),
      (error: PostgresError) => error.sqlState === "23514"
    );
    await assert.rejects(
      database.transaction((transaction) =>
        transaction.query(
          "INSERT INTO omniroute.provider_connections(id,provider,created_at,updated_at) VALUES($1,'openai','created','updated')",
          [id]
        )
      ),
      (error: PostgresError) => error.sqlState === "23514"
    );
    assert.equal((await connections.findById(id))?.provider, "felo");
  });
}
