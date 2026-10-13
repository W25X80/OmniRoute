import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import test from "node:test";
import type { BatchTransitionResult } from "../../src/lib/db/batches.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
  PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";
import { isPostgresErrorKind } from "../../src/lib/db/postgres/database.ts";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import * as batches from "../../src/lib/db/postgres/serviceBatches.ts";
import * as files from "../../src/lib/db/postgres/serviceFiles.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

// These tests use the real native SQL owners and two PostgreSQL connections.
// Barriers choose the lock winner; they never substitute a query result or use sleeps.
test(
  "batch cancellation and result publication have one acknowledged winner",
  {
    skip: !postgresTestsEnabled,
    timeout: 30000,
  },
  async (t) => {
    const database = await createTestPostgresDatabase({
      maxConnections: 6,
      lockTimeoutMs: 10000,
      statementTimeoutMs: 12000,
      queryTimeoutMs: 13000,
      transactionTimeoutMs: 20000,
    });
    const prefix = `batch-transition-${randomUUID()}`;
    const completion = {
      requestCountsTotal: 2,
      requestCountsCompleted: 1,
      requestCountsFailed: 1,
      completedAt: Math.floor(Date.now() / 1000),
    };
    try {
      await migratePostgresSchema(database);
      async function setup(label: string) {
        const name = `${prefix}-${label}`;
        const input = await files.createFile(database, {
          bytes: 1,
          filename: `${name}-input`,
          purpose: "batch",
          content: Buffer.from("x"),
        });
        const batch = await database.transaction((tx) =>
          batches.createBatch(tx, {
            endpoint: "/v1/embeddings",
            inputFileId: input.id,
            completionWindow: "24h",
            status: "finalizing",
          })
        );
        const output = {
          filename: `${name}-output`,
          content: Buffer.from("success"),
          expiresAt: null,
        };
        const error = {
          filename: `${name}-error`,
          content: Buffer.from("failure"),
          expiresAt: null,
        };
        const finish = (selected: PostgresDatabase) =>
          batches.completeBatchWithFiles(selected, batch.id, completion, output, error);
        const cancel = (selected: PostgresDatabase) =>
          selected.transaction((tx) =>
            batches.transitionBatch(tx, batch.id, ["validating", "in_progress", "finalizing"], {
              status: "cancelling",
              cancellingAt: Math.floor(Date.now() / 1000),
            })
          );
        const resultRows = () =>
          database.transaction(
            async (tx) =>
              (
                await tx.query<{
                  id: string;
                  batch_id: string | null;
                }>(
                  `SELECT f.id,b.id AS batch_id FROM omniroute.files f
          LEFT JOIN omniroute.batches b ON f.id=b.output_file_id OR f.id=b.error_file_id
          WHERE f.filename=ANY($1::text[]) AND f.deleted_at IS NULL`,
                  [[output.filename, error.filename]]
                )
              ).rows
          );
        return { batch, output, error, finish, cancel, resultRows };
      }
      function instrument(
        selected: PostgresDatabase,
        afterQuery?: (text: string, values?: unknown[]) => Promise<void>,
        beforeCommit?: (value: unknown) => void
      ): PostgresDatabase {
        return new Proxy(selected, {
          get(target, property, receiver) {
            if (property !== "transaction") return Reflect.get(target, property, receiver);
            return <T>(
              body: (tx: PostgresTransaction) => Promise<T>,
              options?: PostgresTransactionOptions
            ) =>
              target.transaction(async (tx) => {
                const value = await body({
                  async query<Row>(text: string, values?: unknown[]) {
                    const result = await tx.query<Row>(text, values);
                    await afterQuery?.(text, values);
                    return result;
                  },
                });
                beforeCommit?.(value);
                return value;
              }, options);
          },
        });
      }
      async function orderedRace(
        first: (selected: PostgresDatabase) => Promise<BatchTransitionResult>,
        second: (selected: PostgresDatabase) => Promise<BatchTransitionResult>
      ) {
        const held = barrier(),
          release = barrier();
        let once = true;
        const winner = first(
          instrument(database, async (text, values) => {
            if (
              once &&
              text.includes("pg_advisory_xact_lock") &&
              values?.[0] === "omniroute.services:files-batches"
            ) {
              once = false;
              held.resolve();
              await release.promise;
            }
          })
        );
        let contender: Promise<BatchTransitionResult> | undefined;
        try {
          await held.promise;
          contender = second(database);
          release.resolve();
          return await Promise.all([winner, contender]);
        } finally {
          release.resolve();
          await Promise.allSettled([winner, ...(contender ? [contender] : [])]);
        }
      }
      await t.test(
        "publication commits first; cancellation observes completed and changes nothing",
        async () => {
          const f = await setup("complete-first");
          const [completed, cancelled] = await orderedRace(f.finish, f.cancel);
          assert.equal(completed.changed, true);
          assert.equal(completed.batch?.status, "completed");
          assert.equal(cancelled.changed, false);
          assert.equal(cancelled.batch?.status, "completed");
          const rows = await f.resultRows();
          assert.equal(rows.length, 2);
          assert(rows.every((row) => row.batch_id === f.batch.id));
          assert.deepEqual(
            await files.getFileContent(database, completed.batch!.outputFileId!),
            f.output.content
          );
          assert.deepEqual(
            await files.getFileContent(database, completed.batch!.errorFileId!),
            f.error.content
          );
        }
      );
      await t.test(
        "cancellation commits first; the finalizer settles cancelled and creates no files",
        async () => {
          const f = await setup("cancel-first");
          const [cancelled, completed] = await orderedRace(f.cancel, f.finish);
          assert.equal(cancelled.changed, true);
          assert.equal(cancelled.batch?.status, "cancelling");
          assert.equal(completed.batch?.status, "cancelled");
          assert.equal(completed.batch?.outputFileId ?? null, null);
          assert.equal(completed.batch?.errorFileId ?? null, null);
          assert.deepEqual(await f.resultRows(), []);
        }
      );
      await t.test(
        "cancellation progresses while an unpublished output transfer is paused",
        async () => {
          const f = await setup("late-cancel");
          const transferring = barrier(),
            release = barrier();
          let paused = false;
          const finalizer = f.finish(
            instrument(database, async (sql) => {
              if (!paused && sql.includes("INSERT INTO omniroute.files_content_chunks")) {
                paused = true;
                transferring.resolve();
                await release.promise;
              }
            })
          );
          try {
            await transferring.promise;
            const cancelled = await f.cancel(database);
            assert.equal(cancelled.batch?.status, "cancelling");
            release.resolve();
            assert.equal((await finalizer).batch?.status, "cancelled");
            assert.deepEqual(await f.resultRows(), []);
            assert.ok(await database.transaction((tx) => files.getFile(tx, f.batch.inputFileId)));
          } finally {
            release.resolve();
            await Promise.allSettled([finalizer]);
          }
        }
      );
      await t.test(
        "rollback after both file writes leaves neither files nor a completed batch",
        async () => {
          const f = await setup("rollback");
          await assert.rejects(
            f.finish(
              instrument(database, undefined, (value) => {
                assert.equal((value as BatchTransitionResult).batch?.status, "completed");
                throw new Error("reject before COMMIT");
              })
            ),
            /reject before COMMIT/
          );
          assert.deepEqual(await f.resultRows(), []);
          assert.equal(
            (await database.transaction((tx) => batches.getBatch(tx, f.batch.id)))?.status,
            "finalizing"
          );
        }
      );
      await t.test(
        "lost real COMMIT acknowledgement retains linked files without replay or deletion",
        async () => {
          const f = await setup("lost-reply");
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
              lockTimeoutMs: 200,
              statementTimeoutMs: 800,
              queryTimeoutMs: 1000,
            },
            { warn() {} }
          );
          try {
            await assert.rejects(f.finish(uncertain), (error: unknown) =>
              isPostgresErrorKind(error, "commit_unknown")
            );
            assert.equal(proxy.dropped(), true);
            const current = await database.transaction((tx) => batches.getBatch(tx, f.batch.id));
            assert.equal(current?.status, "completed");
            const rows = await f.resultRows();
            assert.equal(rows.length, 2);
            assert(rows.every((row) => row.batch_id === f.batch.id));
            // The worker's failure path cannot replace the durable winner after lost acknowledgement.
            const failed = await database.transaction((tx) =>
              batches.transitionBatch(tx, f.batch.id, ["validating", "in_progress", "finalizing"], {
                status: "failed",
                failedAt: Math.floor(Date.now() / 1000),
              })
            );
            assert.equal(failed.changed, false);
            assert.equal(failed.batch?.status, "completed");
            assert.equal((await f.resultRows()).length, 2);
          } finally {
            await uncertain.close();
            await proxy.close();
          }
        }
      );
    } finally {
      await database.close();
    }
  }
);
