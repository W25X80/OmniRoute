import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { AsyncResource } from "node:async_hooks";
import test from "node:test";
import type { BatchRecord } from "../../src/lib/db/batches.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

/** Observe real client calls; no SQL response, lock or transaction is substituted. */
function observeQueries(
  database: PostgresDatabase,
  before: (text: string, values?: unknown[]) => void | Promise<void>,
  after: (text: string, values?: unknown[]) => void | Promise<void> = () => {}
): PostgresDatabase {
  return new Proxy(database, {
    get(target, property, receiver) {
      if (property === "transaction")
        return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
          target.transaction((tx) =>
            body({
              async query<Row>(text: string, values?: unknown[]) {
                await before(text, values);
                const result = await tx.query<Row>(text, values);
                await after(text, values);
                return result;
              },
            })
          );
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

test(
  "terminal batch expiry and new input references share one native authority",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
    // Keep process bootstrap isolated; native domain calls enter the explicit binding below.
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    const database = await createTestPostgresDatabase({
      maxConnections: 6,
      lockTimeoutMs: 10000,
      statementTimeoutMs: 12000,
      queryTimeoutMs: 13000,
      transactionTimeoutMs: 20000,
    });
    const identity = {};
    const prefix = `batch-expiry-${randomUUID()}`;
    const owner = `${prefix}-owner`;
    const now = Math.floor(Date.now() / 1000);
    const payload = Buffer.from(
      JSON.stringify({
        custom_id: "one",
        method: "POST",
        url: "/v1/embeddings",
        body: { model: "openai/text-embedding-3-small", input: "hello" },
      }) + "\n"
    );
    const batchIds: string[] = [];
    const fileIds: string[] = [];
    try {
      const { migratePostgresSchema } = await import("../../src/lib/db/postgres/migrations.ts");
      await migratePostgresSchema(database);
      const { withPostgresControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
      const batches = await import("../../src/lib/db/batches.ts");
      const files = await import("../../src/lib/db/files.ts");
      const nativeFiles = await import("../../src/lib/db/postgres/serviceFiles.ts");
      const { DEFAULT_BATCH_EXPIRATION_SECONDS } =
        await import("../../src/shared/constants/batch.ts");
      const run = <T>(body: () => T, selected = database): T =>
        withPostgresControlPlane(
          {
            database: selected,
            databaseIdentity: identity,
            assertReady() {},
          } as unknown as PostgresControlPlaneBinding,
          body
        );
      async function setup(label: string) {
        const input = await run(() =>
          files.createFileAsync({
            bytes: payload.length,
            content: payload,
            filename: `${prefix}-${label}.jsonl`,
            purpose: "fine-tune",
            apiKeyId: owner,
          })
        );
        fileIds.push(input.id);
        const terminal = await run(() =>
          batches.createBatchAsync({
            endpoint: "/v1/embeddings",
            completionWindow: "24h",
            inputFileId: input.id,
            apiKeyId: owner,
            status: "cancelled",
            cancelledAt: now - DEFAULT_BATCH_EXPIRATION_SECONDS - 1,
          })
        );
        batchIds.push(terminal.id);
        assert.equal(input.expiresAt, null);
        const sibling = async (selected = database) => {
          const batch = await run(
            () =>
              batches.createBatchAsync({
                endpoint: "/v1/embeddings",
                completionWindow: "24h",
                inputFileId: input.id,
                apiKeyId: owner,
              }),
            selected
          );
          batchIds.push(batch.id);
          return batch;
        };
        return { input, terminal, sibling };
      }

      await t.test(
        "actual scheduler retains a sibling committed before its delete lock",
        async () => {
          const f = await setup("create-first");
          assert.equal((await run(() => files.getFileAsync(f.input.id)))?.apiKeyId, owner);
          let sibling: BatchRecord | undefined;
          let scheduled = false;
          // The injected writer models a separate HTTP request, not a nested SQL callback.
          const siblingRequest = new AsyncResource("batch-expiry-sibling-request");
          const observed = observeQueries(database, async (text, values) => {
            if (
              !scheduled &&
              text.includes("pg_advisory_xact_lock(hashtextextended") &&
              values?.[0] === "omniroute.services:files-batches"
            ) {
              scheduled = true;
              sibling = await siblingRequest.runInAsyncScope(() => f.sibling());
            }
          });
          const { processPendingBatches, stopBatchProcessor, waitForAllBatches } =
            await import("../../open-sse/services/batchProcessor.ts");
          try {
            await run(() => processPendingBatches(), observed);
            assert.equal(scheduled, true);
            assert.ok(sibling);
            assert.equal(
              (await run(() => batches.getBatchAsync(sibling!.id)))?.status,
              "validating"
            );
            assert.deepEqual(await run(() => files.getFileContentAsync(f.input.id)), payload);
          } finally {
            siblingRequest.emitDestroy();
            stopBatchProcessor();
            await waitForAllBatches();
          }
        }
      );

      await t.test("cleanup holds the authority after the last-reference observation", async () => {
        const f = await setup("cleanup-first");
        // This is the successful HTTP metadata/owner check before the concurrent cleanup.
        assert.equal((await run(() => files.getFileAsync(f.input.id)))?.apiKeyId, owner);
        const observedReference = barrier();
        const releaseCleanup = barrier();
        const createEntered = barrier();
        let observed = false;
        const cleanupDb = observeQueries(
          database,
          () => {},
          async (text, values) => {
            if (
              !observed &&
              text.startsWith("SELECT 1 FROM omniroute.batches WHERE") &&
              values?.[0] === f.input.id
            ) {
              observed = true;
              observedReference.resolve();
              await releaseCleanup.promise;
            }
          }
        );
        const createDb = observeQueries(database, (text, values) => {
          if (
            text.includes("pg_advisory_xact_lock(hashtextextended") &&
            values?.[0] === "omniroute.services:files-batches"
          )
            createEntered.resolve();
        });
        const cleanup = run(
          () => batches.deleteExpiredBatchFilesAsync(f.terminal.id, now),
          cleanupDb
        );
        let create: Promise<BatchRecord> | undefined;
        try {
          await observedReference.promise;
          let settled = false;
          create = f.sibling(createDb);
          void create.then(
            () => (settled = true),
            () => (settled = true)
          );
          const rejected = assert.rejects(create, /Input file not found/);
          await createEntered.promise;
          const fence = await database.transaction((tx) =>
            tx.query<{ acquired: boolean }>(
              "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS acquired",
              ["omniroute.services:files-batches"]
            )
          );
          assert.equal(fence.rows[0].acquired, false, "the cleanup still owns the real SQL fence");
          assert.equal(settled, false, "create cannot pass the authority held by cleanup");
          releaseCleanup.resolve();
          await cleanup;
          await rejected;
          assert.equal(await run(() => files.getFileContentAsync(f.input.id)), null);
          const references = await database.transaction((tx) =>
            tx.query<{ id: string }>("SELECT id FROM omniroute.batches WHERE input_file_id=$1", [
              f.input.id,
            ])
          );
          assert.deepEqual(references.rows, [{ id: f.terminal.id }]);
        } finally {
          releaseCleanup.resolve();
          await Promise.allSettled([cleanup, ...(create ? [create] : [])]);
        }
      });

      await t.test(
        "current expiry, active status and explicit file policies stay independent",
        async () => {
          const f = await setup("policy-controls");
          await run(() =>
            batches.updateBatchAsync(f.terminal.id, { cancelledAt: now, status: "cancelled" })
          );
          await run(() => batches.deleteExpiredBatchFilesAsync(f.terminal.id, now));
          assert.deepEqual(await run(() => files.getFileContentAsync(f.input.id)), payload);
          await run(() =>
            batches.updateBatchAsync(f.terminal.id, {
              cancelledAt: now - DEFAULT_BATCH_EXPIRATION_SECONDS - 1,
              status: "in_progress",
            })
          );
          await run(() => batches.deleteExpiredBatchFilesAsync(f.terminal.id, now));
          assert.deepEqual(await run(() => files.getFileContentAsync(f.input.id)), payload);
          assert.equal(
            await database.transaction((tx) =>
              nativeFiles.deleteFileOwnedBy(tx, f.input.id, "other")
            ),
            false
          );
          await f.sibling();
          assert.equal(await run(() => files.deleteFileAsync(f.input.id)), true);
          assert.equal(await run(() => files.getFileContentAsync(f.input.id)), null);
          await assert.rejects(f.sibling(), /Input file not found/);

          const ownTtl = await run(() =>
            files.createFileAsync({
              bytes: payload.length,
              content: payload,
              filename: `${prefix}-own-ttl.jsonl`,
              purpose: "fine-tune",
              apiKeyId: owner,
              expiresAt: now - 1,
            })
          );
          fileIds.push(ownTtl.id);
          const accepted = await run(() =>
            batches.createBatchAsync({
              endpoint: "/v1/embeddings",
              completionWindow: "24h",
              inputFileId: ownTtl.id,
              apiKeyId: owner,
            })
          );
          batchIds.push(accepted.id);
          assert.equal(
            await database.transaction((tx) => nativeFiles.pruneExpiredFiles(tx, now)),
            1
          );
          assert.equal(await run(() => files.getFileContentAsync(ownTtl.id)), null);
          assert.equal((await run(() => batches.getBatchAsync(accepted.id)))?.status, "validating");
        }
      );
    } finally {
      await database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.batches WHERE id=ANY($1::text[])", [batchIds]);
        await tx.query("DELETE FROM omniroute.files WHERE id=ANY($1::text[])", [fileIds]);
      });
      await database.close();
      if (previousBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = previousBackend;
    }
  }
);
