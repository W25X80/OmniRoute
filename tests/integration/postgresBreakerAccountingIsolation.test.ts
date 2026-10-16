import assert from "node:assert/strict";
import test from "node:test";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import { PostgresDomainStateRepository } from "../../src/lib/db/postgres/domainStateRepository.ts";
import { acquirePostgresLock, type PostgresLockOwner } from "../../src/lib/db/postgres/locks.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

// Keep the unrelated fence held throughout the awaited repository operation. A timeout
// proves accidental coupling; removing every fence instead fails the second scenario.
test(
  "breaker snapshots remain available during accounting maintenance but reset stays fenced",
  {
    skip: !postgresTestsEnabled,
    timeout: 30000,
  },
  async (t) => {
    const database = await createTestPostgresDatabase();
    const repository = new PostgresDomainStateRepository(database);
    const snapshot = { state: "OPEN", failureCount: 8, lastFailureTime: Date.now(), options: null };
    async function whileLocked(owner: PostgresLockOwner, body: () => Promise<void>) {
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const holder = database.transaction(async (tx) => {
        await acquirePostgresLock(tx, owner);
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
    try {
      await migratePostgresSchema(database);
      await repository.deleteAllCircuitBreakerStates();
      await t.test("save, delete and reset do not wait for the accounting owner", async () => {
        await whileLocked("accounting", async () => {
          await repository.saveCircuitBreakerState("isolation-a", snapshot);
          assert.deepEqual(await repository.loadCircuitBreakerState("isolation-a"), snapshot);
          await repository.deleteCircuitBreakerState("isolation-a");
          assert.equal(await repository.loadCircuitBreakerState("isolation-a"), null);
          await repository.saveCircuitBreakerState("isolation-b", snapshot);
          await repository.deleteAllCircuitBreakerStates();
          assert.deepEqual(await repository.loadAllCircuitBreakerStates(), []);
        });
      });
      await t.test(
        "the breaker owner still excludes every mutation, including whole-registry reset",
        async () => {
          await repository.saveCircuitBreakerState("isolation-a", snapshot);
          await whileLocked("circuitBreakers", async () => {
            for (const mutation of [
              () => repository.saveCircuitBreakerState("isolation-b", snapshot),
              () => repository.deleteCircuitBreakerState("isolation-a"),
              () => repository.deleteAllCircuitBreakerStates(),
            ]) {
              await assert.rejects(
                mutation(),
                (error: unknown) => error instanceof PostgresError && error.sqlState === "55P03"
              );
            }
          });
          assert.deepEqual(await repository.loadCircuitBreakerState("isolation-a"), snapshot);
          assert.equal(await repository.loadCircuitBreakerState("isolation-b"), null);
          await repository.deleteAllCircuitBreakerStates();
          await repository.saveCircuitBreakerState("after-reset", snapshot);
          assert.deepEqual(
            (await repository.loadAllCircuitBreakerStates()).map((row) => row.name),
            ["after-reset"]
          );
        }
      );
    } finally {
      await repository.deleteAllCircuitBreakerStates();
      await database.close();
    }
  }
);
