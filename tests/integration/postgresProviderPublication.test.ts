import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import type { ProviderRecord } from "../../src/domain/persistence/providerRepositories.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  readApiKeyPolicyVersion,
  type ApiKeyPolicyVersion,
} from "../../src/lib/db/postgres/apiKeyPolicyState.ts";
import {
  assertPostgresCatalogSnapshotCurrent,
  catalogGeneration,
  catalogTableRows,
  withPostgresCatalogSnapshot,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import {
  deleteConnectionState,
  reconcileCodexUsageHistory,
} from "../../src/lib/db/postgres/controlPlaneEffects.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createPostgresProviderConnectionRepository,
  type PostgresProviderEffects,
} from "../../src/lib/db/postgres/providerRepository.ts";
import { resolveUsageAccountIdentity } from "../../src/lib/usage/accountIdentity.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function selectedBinding(database: PostgresDatabase): PostgresControlPlaneBinding {
  // These tests use native provider owners and catalog reads; unrelated repositories are unused.
  return {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
}

async function version(database: PostgresDatabase): Promise<ApiKeyPolicyVersion> {
  return database.transaction(readApiKeyPolicyVersion);
}

async function waitingOn(database: PostgresDatabase, namespace: number): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt++) {
    const { rows } = await database.transaction((transaction) =>
      transaction.query<{ count: string }>(
        `SELECT count(*) AS count FROM pg_locks WHERE locktype='advisory'
         AND classid=18765 AND objid=$1 AND NOT granted`,
        [namespace]
      )
    );
    if (Number(rows[0].count) > 0) return;
    await delay(10);
  }
  assert.fail(`native provider mutation did not wait on namespace ${namespace}`);
}

test(
  "Native provider publication rejects stale two-pool snapshots without versioning runtime writes",
  { skip: !postgresTestsEnabled, timeout: 30_000 },
  async (t) => {
    const savedEncryptionKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "test-only-l27-publication-encryption-key";
    const database = await createTestPostgresDatabase({ maxConnections: 6 });
    const contender = await createTestPostgresDatabase({
      maxConnections: 3,
      lockTimeoutMs: 2_000,
    });
    let effectsApplied = 0;
    const effects: PostgresProviderEffects = {
      async assertApiKeyIsNotManagementPassword() {},
      reconcileCodexUsageHistory,
      deleteConnectionState,
      afterConnectionWrite() {
        effectsApplied++;
      },
      afterNodeWrite() {},
    };
    // The original two-argument constructor must always publish structural changes.
    const writer = createPostgresProviderConnectionRepository(database, effects);
    const otherWriter = createPostgresProviderConnectionRepository(contender, effects);
    const binding = selectedBinding(database);
    const selected = <T>(body: () => T | Promise<T>): Promise<T> =>
      withPostgresControlPlane(binding, () => withPostgresCatalogSnapshot(database, body));
    const seed = async (provider = "openai", data: ProviderRecord = {}) => {
      const connection = await writer.create({
        provider,
        authType: "apikey",
        apiKey: "test-only-l27-key",
        isActive: true,
        providerSpecificData: { excludedModels: [] },
        ...data,
      });
      assert.ok(connection);
      assert.equal(typeof connection.id, "string");
      return String(connection.id);
    };
    const reset = () =>
      database.transaction(async (transaction) => {
        await transaction.query("SELECT pg_advisory_xact_lock(18765,4)");
        await transaction.query("SELECT pg_advisory_xact_lock(18765,5)");
        await transaction.query("SELECT pg_advisory_xact_lock(18765,19)");
        await transaction.query(
          "DELETE FROM omniroute.usage_history WHERE connection_id IN (SELECT id FROM omniroute.provider_connections)"
        );
        await transaction.query("DELETE FROM omniroute.provider_connections");
      });

    try {
      await migratePostgresSchema(database);
      for (const [name, mutate] of [
        ["isActive", (id: string) => otherWriter.update(id, { isActive: false })],
        [
          "structural provider-specific model exclusions",
          (id: string) =>
            otherWriter.update(id, { providerSpecificData: { excludedModels: ["model"] } }),
        ],
        ["catalog freshness stamp", (id: string) => otherWriter.touchSyncedModelsAt(id)],
        ["delete", (id: string) => otherWriter.deleteById(id)],
      ] as const) {
        await t.test(`independent ${name} publication invalidates a prepared catalog`, async () => {
          await reset();
          const id = await seed();
          await selected(async () => {
            const captured = catalogTableRows("provider_connections");
            const generation = catalogGeneration();
            await mutate(id);
            // The other pool cannot refresh this request's immutable prepared row set.
            assert.deepEqual(catalogTableRows("provider_connections"), captured);
            assert.equal(catalogGeneration(), generation);
            assert.equal(BigInt((await version(contender)).generation), BigInt(generation) + 1n);
            await assert.rejects(
              assertPostgresCatalogSnapshotCurrent(),
              /Catalog policy changed during request preparation/
            );
          });
          await selected(async () => {
            await assertPostgresCatalogSnapshotCurrent();
          });
        });
      }

      await t.test(
        "default constructor versions creates, upserts, reorders and all delete modes",
        async () => {
          await reset();
          const publication = async (operation: () => Promise<unknown>) => {
            const before = await version(database);
            await operation();
            const after = await version(contender);
            assert.equal(after.authorityId, before.authorityId);
            assert.equal(BigInt(after.generation), BigInt(before.generation) + 1n);
          };
          let id = "";
          await publication(async () => {
            id = await seed();
          });
          await publication(() =>
            otherWriter.create({
              provider: "openai",
              authType: "apikey",
              apiKey: "test-only-l27-key",
              name: "renamed",
            })
          );
          await publication(() => otherWriter.update(id, {})); // Source predicate fails closed.
          await publication(() => otherWriter.reorder("openai"));
          await publication(() => otherWriter.deleteByIds([id, "missing-id"]));
          await seed();
          await publication(() => otherWriter.deleteByProvider("openai"));
          const before = await version(database);
          assert.equal(await otherWriter.deleteById("missing-id"), false);
          assert.equal(await otherWriter.deleteByIds([]), 0);
          assert.equal(await otherWriter.update("missing-id", { isActive: false }), null);
          await otherWriter.reorder("missing-provider");
          await otherWriter.touchSyncedModelsAt("missing-id");
          assert.deepEqual(await version(contender), before);
        }
      );

      await t.test(
        "runtime updates, metadata merges and explicit credential rotation skip retain the epoch",
        async () => {
          await reset();
          const id = await seed();
          await selected(async () => {
            const before = await version(database);
            await otherWriter.update(id, {
              testStatus: "unavailable",
              lastError: "fixed test error",
              lastErrorAt: "2026-10-11T00:00:00.000Z",
              lastErrorType: "rate_limit",
              lastErrorSource: "test",
              errorCode: "429",
              rateLimitedUntil: "2026-10-11T00:01:00.000Z",
              backoffLevel: 1,
            });
            await otherWriter.touchLastUsed(id, 2);
            await otherWriter.resetBackoff(id);
            await otherWriter.mergeProviderSpecificData(id, {
              alibabaFreeDrainedModels: ["model"],
            });
            const rotated = await otherWriter.update(
              id,
              { accessToken: "test-only-rotated-token" },
              { skipModelCatalog: true }
            );
            assert.equal(rotated?.id, id);
            assert.equal(rotated?.accessToken, "test-only-rotated-token");
            assert.deepEqual(await version(contender), before);
            await assertPostgresCatalogSnapshotCurrent();
          });
        }
      );

      await t.test("publication lock is acquired before provider row locks", async () => {
        await reset();
        const id = await seed();
        let ownerReady!: () => void;
        let probe!: () => void;
        let probeDone!: () => void;
        let release!: () => void;
        const ready = new Promise<void>((resolve) => {
          ownerReady = resolve;
        });
        const requested = new Promise<void>((resolve) => {
          probe = resolve;
        });
        const inspected = new Promise<void>((resolve) => {
          probeDone = resolve;
        });
        const barrier = new Promise<void>((resolve) => {
          release = resolve;
        });
        const owner = database.transaction(async (transaction) => {
          await transaction.query("SELECT pg_advisory_xact_lock(18765,5)");
          ownerReady();
          await requested;
          await transaction.query(
            "SELECT id FROM omniroute.provider_connections WHERE id=$1 FOR UPDATE NOWAIT",
            [id]
          );
          probeDone();
          await barrier;
        });
        let updating: Promise<ProviderRecord | null> | undefined;
        try {
          await ready;
          updating = otherWriter.update(id, { isActive: false });
          await waitingOn(database, 5);
          probe();
          await Promise.race([inspected, owner]);
          release();
          await owner;
          assert.equal((await updating)?.isActive, false);
        } finally {
          probe();
          release();
          await Promise.allSettled([owner, updating]);
        }
      });

      await t.test(
        "Codex reconciliation follows provider 4, policy 5, usage 19 without a nested lease",
        async () => {
          await reset();
          const id = await seed("codex", {
            authType: "oauth",
            apiKey: null,
            providerSpecificData: { workspaceId: "workspace-l27" },
          });
          const existing = (await writer.listRaw())[0];
          const oldAccountKey = resolveUsageAccountIdentity(existing).accountKey;
          await database.transaction((transaction) =>
            transaction.query(
              `INSERT INTO omniroute.usage_history(connection_id,account_key,account_label,
           account_label_priority,timestamp) VALUES($1,$2,'old',0,'2026-10-11T00:00:00.000Z')`,
              [id, oldAccountKey]
            )
          );
          let release!: () => void;
          let ownerReady!: () => void;
          const ready = new Promise<void>((resolve) => {
            ownerReady = resolve;
          });
          const barrier = new Promise<void>((resolve) => {
            release = resolve;
          });
          const owner = database.transaction(async (transaction) => {
            await transaction.query("SELECT pg_advisory_xact_lock(18765,19)");
            ownerReady();
            await barrier;
          });
          let updating: Promise<ProviderRecord | null> | undefined;
          try {
            await ready;
            updating = otherWriter.update(id, {
              providerSpecificData: { workspaceId: "workspace-l27", chatgptUserId: "user-l27" },
            });
            await waitingOn(database, 19);
            const locks = await database.transaction((transaction) =>
              transaction.query<{ count: string }>(
                `SELECT count(*) AS count FROM pg_locks AS usage_wait
             JOIN pg_locks AS policy_lock ON policy_lock.pid=usage_wait.pid
             WHERE usage_wait.locktype='advisory' AND usage_wait.classid=18765
             AND usage_wait.objid=19 AND NOT usage_wait.granted
             AND policy_lock.locktype='advisory' AND policy_lock.classid=18765
             AND policy_lock.objid=5 AND policy_lock.granted`
              )
            );
            assert.ok(Number(locks.rows[0].count) > 0);
            release();
            await owner;
            const updated = await updating;
            assert.ok(updated);
            const identity = resolveUsageAccountIdentity(updated);
            const { rows } = await database.transaction((transaction) =>
              transaction.query<{ account_key: string }>(
                "SELECT account_key FROM omniroute.usage_history WHERE connection_id=$1",
                [id]
              )
            );
            assert.equal(rows[0].account_key, identity.accountKey);
            assert.notEqual(rows[0].account_key, oldAccountKey);
          } finally {
            release();
            await Promise.allSettled([owner, updating]);
          }
          const single = await createTestPostgresDatabase({ maxConnections: 1 });
          try {
            const one = createPostgresProviderConnectionRepository(single, effects);
            await one.update(id, { name: "single-lease-publication" });
            const created = await one.create({
              provider: "codex",
              authType: "oauth",
              providerSpecificData: { workspaceId: "workspace-l27-single" },
            });
            assert.ok(created);
            await one.update(String(created.id), {
              providerSpecificData: {
                workspaceId: "workspace-l27-single",
                chatgptUserId: "user-l27-single",
              },
            });
          } finally {
            await single.close();
          }
        }
      );

      await t.test(
        "an epoch write failure rolls back provider state and never emits post-commit effects",
        async () => {
          await reset();
          const id = await seed();
          const before = await version(database);
          const effectsBefore = effectsApplied;
          await database.transaction(async (transaction) => {
            await transaction.query(`CREATE FUNCTION omniroute.reject_l27_epoch() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private publication detail'; END $$`);
            await transaction.query(`CREATE TRIGGER reject_l27_epoch BEFORE UPDATE ON omniroute.api_key_policy_epoch
            FOR EACH ROW EXECUTE FUNCTION omniroute.reject_l27_epoch()`);
          });
          try {
            await assert.rejects(otherWriter.update(id, { isActive: false }), (error: Error) => {
              assert.doesNotMatch(error.message, /private publication detail/);
              return true;
            });
            assert.equal((await writer.findById(id))?.isActive, true);
            assert.deepEqual(await version(contender), before);
            assert.equal(effectsApplied, effectsBefore);
          } finally {
            await database.transaction(async (transaction) => {
              await transaction.query(
                "DROP TRIGGER reject_l27_epoch ON omniroute.api_key_policy_epoch"
              );
              await transaction.query("DROP FUNCTION omniroute.reject_l27_epoch()");
            });
          }
        }
      );
    } finally {
      await contender.close();
      await database.close();
      if (savedEncryptionKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = savedEncryptionKey;
    }
  }
);
