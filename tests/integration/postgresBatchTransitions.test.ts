import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import test from "node:test";
import type { BatchTransitionResult } from "../../src/lib/db/batches.ts";
import type { PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { isPostgresErrorKind } from "../../src/lib/db/postgres/database.ts";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import * as batches from "../../src/lib/db/postgres/serviceBatches.ts";
import * as files from "../../src/lib/db/postgres/serviceFiles.ts";
import { lockServices } from "../../src/lib/db/postgres/serviceQuery.ts";
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
        const batch = await database.transaction(async (tx) => {
          const input = await files.createFile(tx, {
            bytes: 1,
            filename: `${name}-input`,
            purpose: "batch",
            content: Buffer.from("x"),
          });
          return batches.createBatch(tx, {
            endpoint: "/v1/embeddings",
            inputFileId: input.id,
            completionWindow: "24h",
            status: "finalizing",
          });
        });
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
        const finish = (tx: PostgresTransaction) =>
          batches.completeBatchWithFiles(tx, batch.id, completion, output, error);
        const cancel = (tx: PostgresTransaction) =>
          batches.transitionBatch(tx, batch.id, ["validating", "in_progress", "finalizing"], {
            status: "cancelling",
            cancellingAt: Math.floor(Date.now() / 1000),
          });
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
      async function orderedRace(
        first: (tx: PostgresTransaction) => Promise<BatchTransitionResult>,
        second: (tx: PostgresTransaction) => Promise<BatchTransitionResult>
      ) {
        const held = barrier(),
          release = barrier(),
          entered = barrier();
        const winner = database.transaction(async (tx) => {
          await lockServices(tx);
          held.resolve();
          await release.promise;
          return first(tx);
        });
        let contender: Promise<BatchTransitionResult> | undefined;
        try {
          await held.promise;
          contender = database.transaction((tx) => {
            entered.resolve();
            return second(tx);
          });
          await entered.promise;
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
            await database.transaction((tx) =>
              files.getFileContent(tx, completed.batch!.outputFileId!)
            ),
            f.output.content
          );
          assert.deepEqual(
            await database.transaction((tx) =>
              files.getFileContent(tx, completed.batch!.errorFileId!)
            ),
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
        "rollback after both file writes leaves neither files nor a completed batch",
        async () => {
          const f = await setup("rollback");
          await assert.rejects(
            database.transaction(async (tx) => {
              const result = await f.finish(tx);
              assert.equal(result.batch?.status, "completed");
              throw new Error("reject before COMMIT");
            }),
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
            await assert.rejects(uncertain.transaction(f.finish), (error: unknown) =>
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
