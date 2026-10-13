import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

function deferred() {
  let done!: () => void;
  const promise = new Promise<void>((resolve) => {
    done = resolve;
  });
  return { promise, resolve: () => done() };
}

test(
  "native completed-cost accounting awaits its existing scope without background fan-out",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const keys = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "OMNIROUTE_SPEND_MAX_BUFFER_SIZE",
      "OMNIROUTE_SPEND_FLUSH_INTERVAL_MS",
    ] as const;
    const originalEnv = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
    const originalRuntime = globalThis.__omniroutePersistenceRuntime;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-native-cost-completion-key";
    process.env.OMNIROUTE_SPEND_MAX_BUFFER_SIZE = "1000";
    process.env.OMNIROUTE_SPEND_FLUSH_INTERVAL_MS = "3600000";
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { withPostgresCatalogSnapshot } =
      await import("../../src/lib/db/postgres/catalogSnapshot.ts");
    const database = await createTestPostgresDatabase();
    let plane: PostgresControlPlane | undefined;
    const prefix = `native-cost-${randomUUID()}`;
    try {
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      const state: NonNullable<typeof globalThis.__omniroutePersistenceRuntime> = {
        backend: "postgres",
        phase: "ready",
        plane,
        requests: new Set(),
        attempts: new Set(),
        attemptAdmissions: new Set(),
        attemptCancellation: new AbortController(),
        background: new Set(),
        tails: new Set(),
        owners: new AsyncLocalStorage<{ active: boolean }>(),
      };
      globalThis.__omniroutePersistenceRuntime = state;
      await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, async () => {
          const cost = await import("../../src/domain/costRules.ts");
          const { spendBatchWriter } = await import("../../src/lib/spend/batchWriter.ts");
          const { PostgresDomainStateRepository } =
            await import("../../src/lib/db/postgres/domainStateRepository.ts");
          const { readAcceptedSpendSnapshot } =
            await import("../../src/lib/db/postgres/spendReceipts.ts");
          const { recordTokenUsageAsync } =
            await import("../../open-sse/services/tokenLimitCounter.ts");
          const { PostgresTokenLimitRepository } =
            await import("../../src/lib/db/postgres/tokenLimitRepository.ts");
          const { listLedgerEntriesAsync } = await import("../../src/lib/db/costLedger.ts");
          const owner = new PostgresDomainStateRepository(binding.database!);
          const countLedger = async (id: string) =>
            (await listLedgerEntriesAsync(id, { limit: 500 })).length;
          const seedKey = (id: string) =>
            database.transaction((tx) =>
              tx.query(
                "INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES($1,'completion test',$2,$3)",
                [id, randomUUID(), new Date().toISOString()]
              )
            );
          try {
            await t.test("the async return follows the real ledger write attempt", async () => {
              const id = `${prefix}-await`;
              await seedKey(id);
              const held = deferred(),
                release = deferred();
              const holder = database.transaction(async (tx) => {
                await tx.query("LOCK TABLE omniroute.request_cost_ledger IN ACCESS EXCLUSIVE MODE");
                held.resolve();
                await release.promise;
              });
              await held.promise;
              let finished = false;
              const writing = cost
                .recordCostAsync(id, 1, {
                  provider: "clock-provider",
                  model: "clock-model",
                  tokens: { input_tokens: 1 },
                  requestId: `${id}-request`,
                })
                .then(() => {
                  finished = true;
                });
              try {
                await setImmediate();
                assert.equal(finished, false, "cost accounting must await the ledger attempt");
                assert.equal(spendBatchWriter.getPendingCostTotal(id), 1);
              } finally {
                release.resolve();
                await holder;
                await writing;
              }
              assert.equal(await countLedger(id), 1);
            });

            await t.test(
              "a saturated background owner does not lose native awaited ledger rows",
              async () => {
                const id = `${prefix}-capacity`;
                await seedKey(id);
                // Admission-boundary falsifier, using real database writes in the existing scope.
                const slots = Array.from({ length: 64 }, () => deferred());
                for (const slot of slots) state.background.add(slot.promise);
                try {
                  for (let i = 0; i < 128; i++)
                    await cost.recordCostAsync(id, 0.01, {
                      provider: "clock-provider",
                      model: "clock-model",
                      tokens: {},
                      requestId: `${id}-${i}`,
                    });
                  assert.equal(await countLedger(id), 128);
                  assert.equal(state.background.size, 64);
                  assert.equal(spendBatchWriter.getBufferedEntries(id).length, 128);
                } finally {
                  for (const slot of slots) {
                    slot.resolve();
                    state.background.delete(slot.promise);
                  }
                }
              }
            );

            await t.test(
              "ledger persistence failure stays best-effort after spend admission",
              async () => {
                const id = `${prefix}-best-effort`;
                await seedKey(id);
                await database.transaction((tx) =>
                  tx.query(
                    "ALTER TABLE omniroute.request_cost_ledger ADD CONSTRAINT completion_test_reject CHECK(provider <> 'completion-test-failure')"
                  )
                );
                try {
                  await assert.doesNotReject(
                    cost.recordCostAsync(id, 2, {
                      provider: "completion-test-failure",
                      model: "clock-model",
                      tokens: {},
                    })
                  );
                  assert.equal(spendBatchWriter.getPendingCostTotal(id), 2);
                  assert.equal(await countLedger(id), 0);
                } finally {
                  await database.transaction((tx) =>
                    tx.query(
                      "ALTER TABLE omniroute.request_cost_ledger DROP CONSTRAINT completion_test_reject"
                    )
                  );
                }
              }
            );

            await t.test(
              "a deferred cost keeps its terminal reset window and immutable receipt across retries",
              async () => {
                const id = `${prefix}-reset`;
                await seedKey(id);
                const processingAt = Date.now();
                const today = cost.getBudgetWindow("daily", "00:00", processingAt);
                const completedAt = today.periodStartAt - 1;
                const previous = cost.getBudgetWindow("daily", "00:00", completedAt);
                await cost.recordCostAsync(id, 3, {
                  provider: "clock-provider",
                  model: "clock-model",
                  tokens: {},
                  completedAt,
                  timestamp: new Date(completedAt).toISOString(),
                  requestId: `${id}-request`,
                });
                const admitted = spendBatchWriter.getBufferedEntries(id)[0];
                assert.equal(admitted.timestamp, completedAt);
                assert.ok(admitted.receiptId);
                const windows = [
                  { since: previous.periodStartAt, until: today.periodStartAt },
                  { since: today.periodStartAt, until: today.nextResetAt },
                ];
                const before = await owner.withDomainLock((tx) =>
                  readAcceptedSpendSnapshot(
                    tx,
                    id,
                    windows,
                    spendBatchWriter.getBufferedEntries(id)
                  )
                );
                assert.deepEqual(before.totals, [
                  { total: 3, count: 1 },
                  { total: 0, count: 0 },
                ]);
                const slots = Array.from({ length: 64 }, () => deferred());
                for (const slot of slots) state.background.add(slot.promise);
                try {
                  assert.equal((await spendBatchWriter.flush()).requeued, true);
                  const retry = spendBatchWriter.getBufferedEntries(id)[0];
                  assert.equal(retry.receiptId, admitted.receiptId);
                  assert.equal(retry.timestamp, completedAt);
                } finally {
                  for (const slot of slots) {
                    slot.resolve();
                    state.background.delete(slot.promise);
                  }
                }
                await spendBatchWriter.flush();
                const durable = await owner.withDomainLock((tx) =>
                  readAcceptedSpendSnapshot(tx, id, windows, [])
                );
                assert.deepEqual(durable.totals, before.totals);
                const receipt = (
                  await database.transaction((tx) =>
                    tx.query<{ receipt_id: string; timestamp: string }>(
                      "SELECT receipt_id,timestamp FROM omniroute.domain_cost_receipts WHERE receipt_id=$1",
                      [admitted.receiptId]
                    )
                  )
                ).rows[0];
                assert.equal(receipt.receipt_id, admitted.receiptId);
                assert.equal(Number(receipt.timestamp), completedAt);
                assert.equal(
                  (await listLedgerEntriesAsync(id))[0].timestamp,
                  new Date(completedAt).toISOString()
                );
              }
            );

            await t.test(
              "native no-receipt token usage keeps the captured completion bucket",
              async () => {
                const id = `${prefix}-token-reset`;
                const tokenOwner = new PostgresTokenLimitRepository(binding.database!);
                const limit = await tokenOwner.upsert({
                  apiKeyId: id,
                  scopeType: "global",
                  tokenLimit: 100,
                  resetInterval: "daily",
                  resetTime: "00:00",
                });
                const current = Date.now(),
                  completedAt = cost.getBudgetWindow("daily", "00:00", current).periodStartAt - 1;
                await recordTokenUsageAsync(
                  id,
                  "clock-provider",
                  "clock-model",
                  9,
                  undefined,
                  completedAt
                );
                assert.equal(await tokenOwner.currentUsage(limit, completedAt), 9);
                assert.equal(await tokenOwner.currentUsage(limit, current), 0);
                await tokenOwner.delete(limit.id);
              }
            );

            await t.test(
              "invalid internal clocks reject before spend, token or ledger admission",
              async () => {
                const id = `${prefix}-invalid`;
                await seedKey(id);
                const tokenOwner = new PostgresTokenLimitRepository(binding.database!);
                const limit = await tokenOwner.upsert({
                  apiKeyId: id,
                  scopeType: "global",
                  tokenLimit: 100,
                  resetInterval: "daily",
                });
                for (const completedAt of [
                  NaN,
                  -1,
                  0.5,
                  Infinity,
                  Number.MAX_SAFE_INTEGER,
                  8_640_000_000_000_001,
                ]) {
                  await assert.rejects(cost.recordCostAsync(id, 1, { completedAt }), RangeError);
                  await assert.rejects(
                    recordTokenUsageAsync(
                      id,
                      "clock-provider",
                      "clock-model",
                      1,
                      undefined,
                      completedAt
                    ),
                    RangeError
                  );
                }
                assert.equal(spendBatchWriter.getBufferedEntries(id).length, 0);
                assert.equal(await countLedger(id), 0);
                assert.equal(
                  (
                    await database.transaction((tx) =>
                      tx.query("SELECT 1 FROM omniroute.api_key_token_counters WHERE limit_id=$1", [
                        limit.id,
                      ])
                    )
                  ).rows.length,
                  0
                );
                await cost.recordCostAsync(id, 1, {
                  completedAt: 0,
                  timestamp: new Date(0).toISOString(),
                });
                assert.equal(spendBatchWriter.getBufferedEntries(id)[0].timestamp, 0);
                await recordTokenUsageAsync(id, "clock-provider", "clock-model", 1, undefined, 0);
                assert.equal(await tokenOwner.currentUsage(limit, 0), 1);
                await tokenOwner.delete(limit.id);
              }
            );
          } finally {
            // Buffered work is deliberately best-effort, so discard the test-only remainder.
            spendBatchWriter.resetForTests();
            await database.transaction(async (tx) => {
              await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
              await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
              for (const table of [
                "domain_cost_history",
                "domain_cost_receipts",
                "request_cost_ledger",
              ])
                await tx.query(`DELETE FROM omniroute.${table} WHERE api_key_id LIKE $1`, [
                  `${prefix}%`,
                ]);
              await tx.query("DELETE FROM omniroute.api_keys WHERE id LIKE $1", [`${prefix}%`]);
            });
          }
        })
      );
    } finally {
      await plane?.close();
      await database.close();
      globalThis.__omniroutePersistenceRuntime = originalRuntime;
      for (const key of keys) {
        const prior = originalEnv[key];
        if (prior === undefined) delete process.env[key];
        else process.env[key] = prior;
      }
    }
  }
);
