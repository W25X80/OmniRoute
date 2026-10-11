import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createPostgresSessionRepository } from "../../src/lib/db/postgres/sessionRepository.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import { withPostgresControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import {
  withComboFailureFence,
  recordComboFailureAsync,
  clearComboFailureTracking,
  __resetComboFailureTrackerForTests,
} from "../../open-sse/services/combo/failureTracker.ts";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function fixture(body: (binding: PostgresControlPlaneBinding) => Promise<void>) {
  const saved = {
    encryption: process.env.STORAGE_ENCRYPTION_KEY,
    jwt: process.env.JWT_SECRET,
    api: process.env.API_KEY_SECRET,
  };
  let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
  try {
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-pg-control-plane-encryption-key";
    process.env.JWT_SECRET = "isolated-session-fence-jwt-secret";
    process.env.API_KEY_SECRET = "isolated-session-fence-api-secret";
    plane = await initializePostgresControlPlane({
      connection: await testPostgresOptions(),
      logger: { warn() {} },
      runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
    });
    __resetComboFailureTrackerForTests();
    await plane.run(body);
  } finally {
    await plane?.close();
    for (const [name, value] of Object.entries({
      STORAGE_ENCRYPTION_KEY: saved.encryption,
      JWT_SECRET: saved.jwt,
      API_KEY_SECRET: saved.api,
    })) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

/** Timing/error injection only: every admitted SQL statement uses the real PostgreSQL client. */
function interceptTransactions(
  database: PostgresDatabase,
  before: (sql: string) => Promise<void>
): PostgresDatabase {
  return new Proxy(database, {
    get(target, property, receiver) {
      if (property !== "transaction") return Reflect.get(target, property, receiver);
      return <T>(body: (transaction: PostgresTransaction) => Promise<T>) =>
        target.transaction(async (transaction) =>
          body({
            query: async <Row = Record<string, unknown>>(sql: string, values?: unknown[]) => {
              await before(sql);
              return transaction.query<Row>(sql, values);
            },
          })
        );
    },
  });
}

test(
  "delayed failure cleanup cannot delete a newer healthy pin",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (binding) => {
      assert.ok(binding.database);
      const sessions = createPostgresSessionRepository(binding.database),
        sessionId = randomUUID(),
        comboName = randomUUID();
      await sessions.recordSessionModelUsage(sessionId, comboName, "old-model", "openai");
      await withComboFailureFence(sessionId, comboName, async () => {
        await recordComboFailureAsync(sessionId, comboName);
        await recordComboFailureAsync(sessionId, comboName);
        const entered = deferred(),
          release = deferred();
        let once = true;
        const intercepted = interceptTransactions(binding.database!, async (sql) => {
          if (once && sql === "SELECT pg_advisory_xact_lock(18765,13)") {
            once = false;
            entered.resolve();
            await release.promise;
          }
        });
        const oldFailure = withPostgresControlPlane({ ...binding, database: intercepted }, () =>
          recordComboFailureAsync(sessionId, comboName)
        );
        try {
          await entered.promise;
          // Simulates another process: it writes a healthy pin without resetting this process's counter.
          await sessions.recordSessionModelUsage(sessionId, comboName, "healthy-model", "openai");
        } finally {
          release.resolve();
        }
        assert.deepEqual(await oldFailure, { count: 3, pinClearedNow: false });
        assert.equal(await sessions.getLastSessionModel(sessionId, comboName), "healthy-model");
      });
      await sessions.deleteSessionModelHistory(sessionId, comboName);
    });
  }
);

test(
  "cleanup rejection is not credited and retries against the same observed pin",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (binding) => {
      assert.ok(binding.database);
      const sessions = createPostgresSessionRepository(binding.database),
        sessionId = randomUUID(),
        comboName = randomUUID();
      await sessions.recordSessionModelUsage(sessionId, comboName, "old-model", "openai");
      await withComboFailureFence(sessionId, comboName, async () => {
        await recordComboFailureAsync(sessionId, comboName);
        await recordComboFailureAsync(sessionId, comboName);
        let once = true;
        const intercepted = interceptTransactions(binding.database!, async (sql) => {
          if (once && sql.startsWith("DELETE FROM omniroute.session_model_history")) {
            once = false;
            throw new PostgresError("query_failed", "42501");
          }
        });
        assert.deepEqual(
          await withPostgresControlPlane({ ...binding, database: intercepted }, () =>
            recordComboFailureAsync(sessionId, comboName)
          ),
          { count: 3, pinClearedNow: false }
        );
        assert.equal(await sessions.getLastSessionModel(sessionId, comboName), "old-model");
        assert.deepEqual(await recordComboFailureAsync(sessionId, comboName), {
          count: 4,
          pinClearedNow: true,
        });
        assert.equal(await sessions.getLastSessionModel(sessionId, comboName), null);
        assert.deepEqual(await recordComboFailureAsync(sessionId, comboName), {
          count: 5,
          pinClearedNow: false,
        });
      });
    });
  }
);

test(
  "a local success resetting the streak cancels an admitted old cleanup",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (binding) => {
      assert.ok(binding.database);
      const sessions = createPostgresSessionRepository(binding.database),
        sessionId = randomUUID(),
        comboName = randomUUID();
      await sessions.recordSessionModelUsage(sessionId, comboName, "old-model", "openai");
      await withComboFailureFence(sessionId, comboName, async () => {
        await recordComboFailureAsync(sessionId, comboName);
        await recordComboFailureAsync(sessionId, comboName);
        const entered = deferred(),
          release = deferred();
        let once = true;
        const intercepted = interceptTransactions(binding.database!, async (sql) => {
          if (once && sql === "SELECT pg_advisory_xact_lock(18765,13)") {
            once = false;
            entered.resolve();
            await release.promise;
          }
        });
        const cleanup = withPostgresControlPlane({ ...binding, database: intercepted }, () =>
          recordComboFailureAsync(sessionId, comboName)
        );
        try {
          await entered.promise;
          clearComboFailureTracking(sessionId, comboName);
        } finally {
          release.resolve();
        }
        assert.deepEqual(await cleanup, { count: 3, pinClearedNow: false });
        assert.equal(await sessions.getLastSessionModel(sessionId, comboName), "old-model");
      });
      await sessions.deleteSessionModelHistory(sessionId, comboName);
    });
  }
);

test(
  "concurrent threshold crossings credit an empty-pin cleanup only once",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async () => {
      const sessionId = randomUUID(),
        comboName = randomUUID();
      await withComboFailureFence(sessionId, comboName, async () => {
        await recordComboFailureAsync(sessionId, comboName);
        await recordComboFailureAsync(sessionId, comboName);
        const results = await Promise.all([
          recordComboFailureAsync(sessionId, comboName),
          recordComboFailureAsync(sessionId, comboName),
        ]);
        assert.equal(results.filter((result) => result.pinClearedNow).length, 1);
        assert.deepEqual(results.map((result) => result.count).sort(), [3, 4]);
      });
    });
  }
);

test(
  "delimiter-bearing native fence pairs cannot inherit each other's authority",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (binding) => {
      assert.ok(binding.database);
      const sessions = createPostgresSessionRepository(binding.database),
        prefix = randomUUID();
      const firstSession = `${prefix}::b`,
        firstCombo = "c",
        secondSession = prefix,
        secondCombo = "b::c";
      await sessions.recordSessionModelUsage(firstSession, firstCombo, "first-model", "openai");
      await sessions.recordSessionModelUsage(secondSession, secondCombo, "second-model", "openai");
      await withComboFailureFence(firstSession, firstCombo, async () => {
        await recordComboFailureAsync(firstSession, firstCombo);
        await recordComboFailureAsync(firstSession, firstCombo);
        await withComboFailureFence(secondSession, secondCombo, async () => {
          assert.deepEqual(await recordComboFailureAsync(secondSession, secondCombo), {
            count: 1,
            pinClearedNow: false,
          });
          assert.deepEqual(await recordComboFailureAsync(firstSession, firstCombo), {
            count: 3,
            pinClearedNow: true,
          });
          assert.equal(await sessions.getLastSessionModel(firstSession, firstCombo), null);
          assert.equal(
            await sessions.getLastSessionModel(secondSession, secondCombo),
            "second-model"
          );
        });
      });
      await sessions.deleteSessionModelHistory(secondSession, secondCombo);
    });
  }
);
