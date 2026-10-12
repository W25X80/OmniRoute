import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { withPostgresControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  readProviderQuotaRows,
  writeProviderQuotaUsage,
} from "../../src/lib/db/postgres/providerQuotaRepository.ts";
import {
  getProviderQuotaAsync,
  recordProviderQuotaUsageAsync,
} from "../../src/lib/quota/providerQuotaState.ts";
import { canAffordRequestAsync } from "../../src/lib/quota/quotaScheduler.ts";
import {
  createTestPostgresDatabase,
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

function interceptQueries(
  database: PostgresDatabase,
  before: (sql: string) => Promise<void>
): PostgresDatabase {
  return new Proxy(database, {
    get(target, property, receiver) {
      if (property !== "transaction") return Reflect.get(target, property, receiver);
      return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
        target.transaction((tx) =>
          body({
            query: async <Row = Record<string, unknown>>(sql: string, values?: unknown[]) => {
              await before(sql);
              return tx.query<Row>(sql, values);
            },
          })
        );
    },
  });
}

test(
  "provider quota windows follow acknowledged per-entity lock order",
  {
    skip: !postgresTestsEnabled,
    timeout: 60_000,
  },
  async (t) => {
    const database = await createTestPostgresDatabase({
      maxConnections: 8,
      lockTimeoutMs: 10_000,
      statementTimeoutMs: 12_000,
      queryTimeoutMs: 13_000,
      transactionTimeoutMs: 20_000,
    });
    const savedKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "provider-quota-window-order-test";
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    let now = 59_999;
    try {
      await migratePostgresSchema(database);
      await database.transaction((tx) => tx.query("TRUNCATE omniroute.provider_quota_state"));
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      t.mock.method(Date, "now", () => now);

      await t.test(
        "old arrival cannot roll back a newer committed window or reopen its gate",
        async () => {
          const entered = deferred(),
            release = deferred();
          let first = true;
          const delayed = interceptQueries(database, async () => {
            if (!first) return;
            first = false;
            entered.resolve();
            await release.promise;
          });
          now = 59_999;
          const oldArrival = writeProviderQuotaUsage(delayed, "ordered", "model", 10, {
            tokenLimit: 100,
          });
          try {
            await entered.promise;
            now = 60_001;
            await writeProviderQuotaUsage(database, "ordered", "model", 90, { tokenLimit: 100 });
          } finally {
            release.resolve();
          }
          await oldArrival;
          const [row] = await readProviderQuotaRows(database, "ordered", "model");
          assert.equal(row.window_start, 60_000);
          assert.equal(row.window_reset, 120_000);
          assert.equal(row.tokens_used, 100);
          await plane!.run(async () => {
            const snapshot = await getProviderQuotaAsync("ordered", "model");
            assert.equal(snapshot?.known, true);
            assert.equal(snapshot?.tokensUsed, 100);
            assert.equal(
              (
                await canAffordRequestAsync("ordered", "model", {
                  messages: [{ role: "user", content: "hello" }],
                  max_tokens: 20,
                })
              ).affordable,
              false
            );
          });
          now = 60_011;
          await writeProviderQuotaUsage(database, "ordered", "model", 5);
          assert.equal(
            (await readProviderQuotaRows(database, "ordered", "model"))[0].tokens_used,
            105
          );
        }
      );

      await t.test("absent-row lock blocks only the same exact entity", async () => {
        const held = deferred(),
          release = deferred();
        const blocker = database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18767,hashtext($1))", [
            JSON.stringify(["absent", "model"]),
          ]);
          held.resolve();
          await release.promise;
        });
        await held.promise;
        let settled = false;
        const blocked = writeProviderQuotaUsage(database, "absent", "model", 7).finally(() => {
          settled = true;
        });
        try {
          let waiting = false;
          for (let attempt = 0; attempt < 100; attempt++) {
            waiting = (
              await database.transaction((tx) =>
                tx.query<{ waiting: boolean }>(
                  "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=18767::oid AND NOT granted) AS waiting"
                )
              )
            ).rows[0].waiting;
            if (waiting) break;
            await delay(10);
          }
          assert.equal(
            waiting,
            true,
            "writer must actually wait for the database lock before a row exists"
          );
          await writeProviderQuotaUsage(database, "absent", "different-model", 3);
          await writeProviderQuotaUsage(database, "different-connection", "model", 4);
          assert.equal(settled, false);
        } finally {
          release.resolve();
          await blocker;
          await blocked;
        }
        assert.equal((await readProviderQuotaRows(database, "absent", "model"))[0].tokens_used, 7);
      });

      await t.test(
        "same-window sums, custom durations and token limits retain their contract",
        async () => {
          now = 60_001;
          await Promise.all(
            Array.from({ length: 8 }, () =>
              writeProviderQuotaUsage(database, "controls", "model", 2, { tokenLimit: 100 })
            )
          );
          assert.equal(
            (await readProviderQuotaRows(database, "controls", "model"))[0].tokens_used,
            16
          );
          now = 70_001;
          await writeProviderQuotaUsage(database, "controls", "model", 4, {
            windowMs: 120_000,
            tokenLimit: 0,
          });
          let row = (await readProviderQuotaRows(database, "controls", "model"))[0];
          assert.equal(row.window_start, 0, "a larger duration legitimately has an earlier start");
          assert.equal(row.window_reset, 120_000);
          assert.equal(row.tokens_used, 20);
          assert.equal(row.token_limit, 100);
          now = 120_001;
          await writeProviderQuotaUsage(database, "controls", "model", 5, {
            windowMs: 120_000,
            tokenLimit: 200,
          });
          row = (await readProviderQuotaRows(database, "controls", "model"))[0];
          assert.equal(row.window_start, 120_000);
          assert.equal(row.window_reset, 240_000);
          assert.equal(row.tokens_used, 5);
          assert.equal(row.token_limit, 200);
          // Tuple framing distinguishes concatenation collisions.
          await writeProviderQuotaUsage(database, "a:b", "c", 1);
          await writeProviderQuotaUsage(database, "a", "b:c", 2);
          assert.equal((await readProviderQuotaRows(database, "a:b", "c"))[0].tokens_used, 1);
          assert.equal((await readProviderQuotaRows(database, "a", "b:c"))[0].tokens_used, 2);
        }
      );

      await t.test("best-effort unknown COMMIT is never replayed", async () => {
        let attempts = 0;
        const uncertain = new Proxy(database, {
          get(target, property, receiver) {
            if (property !== "transaction") return Reflect.get(target, property, receiver);
            return async <T>(body: (tx: PostgresTransaction) => Promise<T>) => {
              attempts++;
              await target.transaction(body);
              throw new PostgresError("commit_unknown");
            };
          },
        });
        await plane!.run((binding) =>
          withPostgresControlPlane({ ...binding, database: uncertain }, () =>
            recordProviderQuotaUsageAsync("uncertain", "model", 4)
          )
        );
        assert.equal(attempts, 1);
        assert.equal(
          (await readProviderQuotaRows(database, "uncertain", "model"))[0].tokens_used,
          4
        );
      });
    } finally {
      t.mock.restoreAll();
      await plane?.close();
      await database.close();
      if (savedKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = savedKey;
    }
  }
);
