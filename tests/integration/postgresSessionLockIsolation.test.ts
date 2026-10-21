import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import test from "node:test";
import { createPostgresComboRepository } from "../../src/lib/db/postgres/comboRepository.ts";
import { PostgresError, type PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { acquirePostgresEntityLock, acquirePostgresLock } from "../../src/lib/db/postgres/locks.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresSessionRepository } from "../../src/lib/db/postgres/sessionRepository.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function affinityKey(session: string, provider: string): string {
  return `${provider}:${createHash("sha256").update(`${provider}:${session}`).digest("hex")}`;
}

function lockTimeout(error: unknown): boolean {
  return (
    error instanceof PostgresError && error.kind === "query_failed" && error.sqlState === "55P03"
  );
}

// The existing fixture admits only the disposable TLS server. Holding an actual lock
// until the contender settles makes accidental coupling fail at the server lock timeout.
test(
  "session pins isolate contention while cleanup and combo history changes stay fenced",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 4, lockTimeoutMs: 200 });
    const sessions = createPostgresSessionRepository(database);
    const combos = createPostgresComboRepository(database, {
      bumpProxyRegistryGeneration() {},
      invalidateCachedLKGP() {},
    });
    const first = `pin-${randomUUID()}`;
    const second = `pin-${randomUUID()}`;
    const absent = `pin-${randomUUID()}`;
    const comboId = `combo-${randomUUID()}`;
    const comboName = `session-lock-${randomUUID()}`;
    const provider = "openai";
    const now = Date.parse("2026-10-21T00:00:00.000Z");
    const ttl = 60000;

    async function whileLocked(
      acquire: (tx: PostgresTransaction) => Promise<void>,
      body: () => Promise<void>
    ) {
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const holder = database.transaction(async (tx) => {
        await acquire(tx);
        entered.resolve();
        await release.promise;
      });
      void holder.catch(entered.reject);
      try {
        await entered.promise;
        await body();
      } finally {
        release.resolve();
        await holder;
      }
    }

    function whilePinLocked(session: string, body: () => Promise<void>) {
      return whileLocked(async (tx) => {
        await acquirePostgresLock(tx, "sessions", "shared");
        await acquirePostgresEntityLock(tx, "sessionAffinity", [affinityKey(session, provider)]);
      }, body);
    }

    try {
      await migratePostgresSchema(database);
      await sessions.upsertSessionAccountAffinity(first, provider, "first-connection", now, ttl);

      await t.test(
        "another pin and model-history append finish while one pin is held",
        async () => {
          await whilePinLocked(first, async () => {
            await sessions.upsertSessionAccountAffinity(
              second,
              provider,
              "second-connection",
              now,
              ttl
            );
            await sessions.upsertSessionAccountAffinity(
              first,
              "anthropic",
              "other-provider",
              now,
              ttl
            );
            await sessions.recordSessionModelUsage(second, comboName, "model-a", provider);
            assert.equal(
              (await sessions.getSessionAccountAffinity(second, provider, ttl, now))?.connectionId,
              "second-connection"
            );
            assert.equal(await sessions.getLastSessionModel(second, comboName), "model-a");
          });
        }
      );

      await t.test("the pin fence also serializes creation of an absent KV row", async () => {
        await whilePinLocked(absent, async () => {
          await assert.rejects(
            sessions.upsertSessionAccountAffinity(absent, provider, "not-written", now, ttl),
            lockTimeout
          );
        });
        assert.equal(await sessions.getSessionAccountAffinity(absent, provider, ttl, now), null);
      });

      await t.test(
        "reads, replacement, touch and compare-delete all retain the same pin fence",
        async () => {
          await whilePinLocked(first, async () => {
            for (const operation of [
              () => sessions.getSessionAccountAffinity(first, provider, ttl, now),
              () => sessions.upsertSessionAccountAffinity(first, provider, "replacement", now, ttl),
              () => sessions.touchSessionAccountAffinity(first, provider, now, ttl),
              () => sessions.deleteSessionAccountAffinity(first, provider),
              () =>
                sessions.evictSessionAccountAffinityForConnection(
                  first,
                  provider,
                  "first-connection"
                ),
            ]) {
              await assert.rejects(operation(), lockTimeout);
            }
          });
          assert.equal(
            (await sessions.getSessionAccountAffinity(first, provider, ttl, now))?.connectionId,
            "first-connection"
          );
        }
      );

      await t.test("cleanup cannot inspect pins while a pin owner is admitted", async () => {
        await whilePinLocked(first, async () => {
          await assert.rejects(
            sessions.cleanupStaleSessionAccountAffinities(undefined, now + ttl),
            lockTimeout
          );
        });
        assert.equal(
          (await sessions.getSessionAccountAffinity(first, provider, ttl, now))?.connectionId,
          "first-connection"
        );
      });

      await t.test(
        "an exclusive session owner still excludes every pin and history append",
        async () => {
          await whileLocked(
            (tx) => acquirePostgresLock(tx, "sessions"),
            async () => {
              await assert.rejects(
                sessions.upsertSessionAccountAffinity(absent, provider, "not-written", now, ttl),
                lockTimeout
              );
              await assert.rejects(
                sessions.recordSessionModelUsage(second, comboName, "not-written", provider),
                lockTimeout
              );
            }
          );
          assert.equal(await sessions.getLastSessionModel(second, comboName), "model-a");
        }
      );

      await t.test(
        "combo model edits still wait for session owners before clearing history",
        async () => {
          await combos.create({ id: comboId, name: comboName, models: [] });
          await whilePinLocked(first, async () => {
            await assert.rejects(combos.update(comboId, { models: [] }), lockTimeout);
            assert.equal(await sessions.getLastSessionModel(second, comboName), "model-a");
          });
          await combos.update(comboId, { models: [] });
          assert.equal(await sessions.getLastSessionModel(second, comboName), null);
        }
      );

      await t.test(
        "replacement eviction and exact TTL expiry retain their existing behavior",
        async () => {
          await sessions.upsertSessionAccountAffinity(first, provider, "replacement", now + 1, ttl);
          assert.equal(
            await sessions.evictSessionAccountAffinityForConnection(
              first,
              provider,
              "first-connection"
            ),
            false
          );
          const replacement = await sessions.getSessionAccountAffinity(
            first,
            provider,
            ttl,
            now + ttl
          );
          assert.equal(replacement?.connectionId, "replacement");
          assert.equal(replacement?.createdAt, new Date(now).toISOString());
          assert.equal(
            await sessions.getSessionAccountAffinity(first, provider, ttl, now + ttl + 1),
            null
          );
        }
      );
    } finally {
      try {
        await database.transaction(async (tx) => {
          await acquirePostgresLock(tx, "sessions");
          await tx.query(
            "DELETE FROM omniroute.key_value WHERE namespace='session_account_affinity' AND key=ANY($1::text[])",
            [
              [
                affinityKey(first, provider),
                affinityKey(first, "anthropic"),
                affinityKey(second, provider),
                affinityKey(absent, provider),
              ],
            ]
          );
          await tx.query("DELETE FROM omniroute.session_model_history WHERE combo_name=$1", [
            comboName,
          ]);
        });
        await combos.deleteById(comboId);
      } finally {
        await database.close();
      }
    }
  }
);
