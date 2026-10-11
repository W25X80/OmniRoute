import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { PostgresDomainStateRepository } from "../../src/lib/db/postgres/domainStateRepository.ts";
import { SpendBatchWriter, type BufferedCostEntry } from "../../src/lib/spend/batchWriter.ts";
import {
  PostgresSpendReceiptError,
  readAcceptedSpendSnapshot,
  type SpendWindow,
} from "../../src/lib/db/postgres/spendReceipts.ts";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";

const quiet = { log() {}, error() {} };
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native spend receipts separate accepted events from their durable/pending representations",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 6 });
    const owner = new PostgresDomainStateRepository(database);
    const prefix = `spend-receipt-${randomUUID()}`;
    const windows: SpendWindow[] = [{ since: 0, until: Number.MAX_SAFE_INTEGER }];
    const snapshot = (id: string, pending: BufferedCostEntry[], ranges = windows) =>
      owner.withDomainLock((tx) => readAcceptedSpendSnapshot(tx, id, ranges, pending));
    try {
      await migratePostgresSchema(database);
      await t.test(
        "a committed row and its unacknowledged in-flight mirror count once",
        async () => {
          const id = `${prefix}-overlap`;
          const committed = deferred(),
            acknowledge = deferred();
          const writer = new SpendBatchWriter({
            createReceiptId: randomUUID,
            persistEntries: async (entries) => {
              await owner.batchSaveCostEntries(entries);
              committed.resolve();
              await acknowledge.promise;
            },
            logger: quiet,
          });
          writer.increment(id, 3.25, 123);
          const pending = writer.getBufferedEntries(id);
          assert.deepEqual((await snapshot(id, pending)).totals, [{ total: 3.25, count: 1 }]);
          const flush = writer.flush();
          try {
            await committed.promise;
            assert.equal(writer.getPendingCostTotal(id), 3.25);
            const overlap = await snapshot(id, writer.getBufferedEntries(id));
            assert.deepEqual(overlap.totals, [{ total: 3.25, count: 1 }]);
            assert.deepEqual(overlap.confirmedReceiptIds, [pending[0].receiptId]);
          } finally {
            acknowledge.resolve();
            await flush;
            writer.resetForTests();
          }
          assert.deepEqual((await snapshot(id, [])).totals, [{ total: 3.25, count: 1 }]);
        }
      );

      await t.test("capture precedes the await for the writer lock", async () => {
        const id = `${prefix}-capture`;
        const held = deferred(),
          release = deferred();
        const holder = owner.withDomainLock(async () => {
          held.resolve();
          await release.promise;
        });
        await held.promise;
        const event = { apiKeyId: id, cost: 2, timestamp: 321, receiptId: randomUUID() };
        const saving = owner.batchSaveCostEntries([event]);
        event.cost = 900;
        event.timestamp = 999;
        release.resolve();
        await holder;
        await saving;
        assert.deepEqual(await owner.loadCostEntries(id, 0), [{ cost: 2, timestamp: 321 }]);
      });

      await t.test(
        "reused identity rejects the entire batch before new events are written",
        async () => {
          const id = `${prefix}-duplicate`;
          const first = { apiKeyId: id, cost: 1, timestamp: 123, receiptId: randomUUID() };
          await owner.batchSaveCostEntries([first]);
          await assert.rejects(
            owner.batchSaveCostEntries([first, { ...first, receiptId: randomUUID() }]),
            PostgresSpendReceiptError
          );
          await assert.rejects(
            owner.batchSaveCostEntries([{ ...first, cost: 100 }]),
            PostgresSpendReceiptError
          );
          assert.deepEqual((await snapshot(id, [first])).totals, [{ total: 1, count: 1 }]);
          await assert.rejects(snapshot(id, [{ ...first, cost: 100 }]), PostgresSpendReceiptError);
          await assert.rejects(
            snapshot(id, [{ ...first, timestamp: 456 }]),
            PostgresSpendReceiptError
          );
          const anotherKey = `${prefix}-foreign`;
          await assert.rejects(
            snapshot(anotherKey, [{ ...first, apiKeyId: anotherKey }]),
            PostgresSpendReceiptError
          );
          assert.equal((await owner.loadCostEntries(id, 0)).length, 1);
        }
      );

      await t.test(
        "one statement returns independent windows without double counting",
        async () => {
          const id = `${prefix}-windows`;
          const a = { apiKeyId: id, cost: 2, timestamp: 100, receiptId: randomUUID() };
          const b = { apiKeyId: id, cost: 3, timestamp: 200, receiptId: randomUUID() };
          const c = { apiKeyId: id, cost: 4, timestamp: 300, receiptId: randomUUID() };
          await owner.batchSaveCostEntries([a, b]);
          const result = await snapshot(
            id,
            [a, b, c],
            [
              { since: 100, until: 200 },
              { since: 200, until: 400 },
              { since: 0, until: 400 },
            ]
          );
          assert.deepEqual(result.totals, [
            { total: 2, count: 1 },
            { total: 7, count: 2 },
            { total: 9, count: 3 },
          ]);
          assert.deepEqual(
            new Set(result.confirmedReceiptIds),
            new Set([a.receiptId, b.receiptId])
          );
        }
      );

      await t.test(
        "equal event tuples with different identities are separate accepted charges",
        async () => {
          const id = `${prefix}-equal-tuples`;
          const a = { apiKeyId: id, cost: 2, timestamp: 123, receiptId: randomUUID() };
          const b = { ...a, receiptId: randomUUID() };
          await owner.batchSaveCostEntries([a, b]);
          assert.deepEqual((await snapshot(id, [a, b])).totals, [{ total: 4, count: 2 }]);
          assert.equal((await owner.loadCostEntries(id, 0)).length, 2);
          const duplicate = { ...a, receiptId: randomUUID() };
          await assert.rejects(
            owner.batchSaveCostEntries([duplicate, duplicate]),
            PostgresSpendReceiptError
          );
          assert.equal((await owner.loadCostEntries(id, 0)).length, 2);
        }
      );

      await t.test(
        "history retention preserves non-replay identity and does not resurrect a stale mirror",
        async () => {
          const id = `${prefix}-retention`;
          const event = { apiKeyId: id, cost: 5, timestamp: 123, receiptId: randomUUID() };
          await owner.batchSaveCostEntries([event]);
          await owner.deleteCostEntries(id);
          const read = await snapshot(id, [event]);
          assert.deepEqual(read.totals, [{ total: 0, count: 0 }]);
          assert.deepEqual(read.confirmedReceiptIds, [event.receiptId]);
          await assert.rejects(owner.batchSaveCostEntries([event]), PostgresSpendReceiptError);
        }
      );

      await t.test(
        "a definite rejected write keeps one pending charge and no receipt",
        async () => {
          const id = `${prefix}-rejected`;
          const event = { apiKeyId: id, cost: 6, timestamp: 123, receiptId: randomUUID() };
          await assert.rejects(
            owner.withDomainLock(async (tx) => {
              await tx.query(
                "INSERT INTO omniroute.domain_cost_history(api_key_id,cost,timestamp) VALUES($1,$2,$3)",
                [id, 6, 123]
              );
              throw new Error("reject before commit");
            })
          );
          assert.deepEqual((await snapshot(id, [event])).totals, [{ total: 6, count: 1 }]);
          await owner.batchSaveCostEntries([event]);
          assert.deepEqual((await snapshot(id, [event])).totals, [{ total: 6, count: 1 }]);
        }
      );

      await t.test(
        "loss of a real COMMIT reply is reconciled by a durable receipt without replay",
        async () => {
          const options = await testPostgresOptions();
          const proxy = await createPostgresReplyLossProxy({
            upstreamPort: options.port!,
            certificate: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_CA!, "utf8"),
            key: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_KEY!, "utf8"),
            dropCommit: 1,
          });
          const uncertain = await createPostgresDatabase(
            {
              ...options,
              port: proxy.port,
              queryTimeoutMs: 1000,
              statementTimeoutMs: 800,
              lockTimeoutMs: 200,
            },
            { warn() {} }
          );
          let attempts = 0;
          const writer = new SpendBatchWriter({
            createReceiptId: randomUUID,
            persistEntries: async (entries) => {
              attempts++;
              await new PostgresDomainStateRepository(uncertain).batchSaveCostEntries(entries);
            },
            logger: quiet,
          });
          const id = `${prefix}-lost-reply`;
          writer.increment(id, 7, 123);
          try {
            assert.equal((await writer.flush()).requeued, false);
            assert.equal(proxy.dropped(), true);
            assert.equal(writer.getUncertainEntries().length, 1);
            const read = await snapshot(id, writer.getBufferedEntries(id));
            assert.deepEqual(read.totals, [{ total: 7, count: 1 }]);
            assert.equal(writer.resolveCommittedReceipts(read.confirmedReceiptIds), 1);
            assert.equal(writer.getPendingCostTotal(id), 0);
            await writer.flush();
            assert.equal(attempts, 1);
            assert.deepEqual((await snapshot(id, [])).totals, [{ total: 7, count: 1 }]);
          } finally {
            writer.resetForTests();
            await uncertain.close();
            await proxy.close();
          }
        }
      );
    } finally {
      await owner.withDomainLock(async (tx) => {
        await tx.query("DELETE FROM omniroute.domain_cost_history WHERE api_key_id LIKE $1", [
          `${prefix}%`,
        ]);
        // Test-only cleanup is safe after all local writers above have drained.
        await tx.query("DELETE FROM omniroute.domain_cost_receipts WHERE api_key_id LIKE $1", [
          `${prefix}%`,
        ]);
      });
      await database.close();
    }
  }
);

test(
  "restored native breakers do not share mutable failure-kind counters",
  { skip: !postgresTestsEnabled },
  async () => {
    const original = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-spend-receipt-test-encryption-key";
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { CircuitBreaker, initializeCircuitBreakerPersistence } =
      await import("../../src/shared/utils/circuitBreaker.ts");
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    const name = `receipt-breaker-${randomUUID()}`;
    try {
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });

      await plane.run(async (binding) => {
        const owner = new PostgresDomainStateRepository(binding.database!);
        await owner.saveCircuitBreakerState(name, {
          state: "CLOSED",
          failureCount: 2,
          lastFailureTime: null,
          options: { kindFailureCounts: { transient: 2 } },
        });
        await initializeCircuitBreakerPersistence(binding.database!);
        const first = new CircuitBreaker(name);
        first.kindFailureCounts.transient = 99;
        const second = new CircuitBreaker(name);
        assert.equal(second.kindFailureCounts.transient, 2);
        assert.notEqual(first.kindFailureCounts, second.kindFailureCounts);
        assert.deepEqual((await owner.loadCircuitBreakerState(name))!.options.kindFailureCounts, {
          transient: 2,
        });
        await owner.deleteCircuitBreakerState(name);
      });
    } finally {
      await plane?.close();
      if (original === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = original;
    }
  }
);
