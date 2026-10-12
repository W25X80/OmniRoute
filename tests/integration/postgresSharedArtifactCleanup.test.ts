import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import { PostgresCallLogRepository } from "../../src/lib/db/postgres/callLogRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { prepareArtifactPublication } from "../../src/lib/db/postgres/artifactPublicationRepository.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  artifactAuthorityFixture,
  createS3ArtifactStore,
} from "../helpers/persistence/artifactAuthorityFixture.ts";

const target = {
  table: "call_logs",
  column: "timestamp",
  cutoff: "iso",
  resultKey: "logs",
} as const;
const cutoff = "0002-01-01T00:00:00.000Z";
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native cleanup preserves shared artifacts until their last referenced log is retired",
  {
    skip: !postgresTestsEnabled,
    timeout: 60000,
  },
  async (t) => {
    const previous = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "5d".repeat(32);
    const database = await createTestPostgresDatabase();
    const writerDatabase = await createTestPostgresDatabase();
    const cleanupDatabase = await createTestPostgresDatabase();
    const prefix = `shared-artifact-${randomUUID()}`;
    const references = new Set<string>();
    artifactAuthorityFixture.reset();
    const store = createS3ArtifactStore();
    const owner = createPostgresCleanupRepository(database);
    const seed = (id: string, timestamp: string, reference: string) =>
      database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
        await tx.query(
          "INSERT INTO omniroute.call_logs(id,timestamp,artifact_relpath,detail_state) VALUES($1,$2,$3,'ready')",
          [id, timestamp, reference]
        );
      });
    const queue = (reference: string) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<{ count: string }>(
              "SELECT COUNT(*)::text AS count FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
              [reference]
            )
          ).rows[0].count
      );
    const object = async () => {
      const stored = await store.put(Buffer.from('{"shared":"payload"}'));
      references.add(stored.reference);
      return stored.reference;
    };
    const applyDeletes = async (paths: string[]) => {
      for (const path of paths) {
        assert.equal(await store.delete(path), true);
        await owner.acknowledgeArtifactDeletion(path);
      }
    };
    try {
      await migratePostgresSchema(database);
      for (const reset of [false, true]) {
        await t.test(
          `${reset ? "partial reset" : "retention"} keeps a newer row's shared object`,
          async () => {
            const reference = await object();
            await seed(`${prefix}-${reset}-old`, "0001-01-01T00:00:00.000Z", reference);
            await seed(`${prefix}-${reset}-new`, "0003-01-01T00:00:00.000Z", reference);
            const removed = reset
              ? await owner.reset([target], cutoff)
              : await owner.deleteCallLogs(cutoff);
            assert.equal(
              reset
                ? (removed as { counts: Record<string, number> }).counts.logs
                : (removed as { deleted: number }).deleted,
              1
            );
            assert.deepEqual(removed.artifacts, []);
            assert.equal(await queue(reference), "0");
            await applyDeletes(removed.artifacts);
            assert.deepEqual(await store.get(reference), Buffer.from('{"shared":"payload"}'));
            const last = reset ? await owner.reset([target]) : await owner.deleteCallLogs();
            assert.equal(last.artifacts.filter((path) => path === reference).length, 1);
            assert.equal(await queue(reference), "1");
            await applyDeletes(last.artifacts);
            assert.equal(await store.get(reference), null);
          }
        );
      }

      for (const reset of [false, true]) {
        await t.test(
          `${reset ? "full reset" : "explicit purge"} queues a shared object exactly once`,
          async () => {
            const reference = await object();
            await seed(`${prefix}-${reset}-one`, "0003-01-01T00:00:00.000Z", reference);
            await seed(`${prefix}-${reset}-two`, "0003-01-01T00:00:00.000Z", reference);
            const result = reset ? await owner.reset([target]) : await owner.deleteCallLogs();
            assert.equal(result.artifacts.filter((path) => path === reference).length, 1);
            assert.equal(await queue(reference), "1");
            const before = artifactAuthorityFixture.deletes;
            await applyDeletes(result.artifacts);
            assert.equal(artifactAuthorityFixture.deletes - before, result.artifacts.length);
            assert.equal(await store.get(reference), null);
          }
        );
      }

      for (const reset of [false, true]) {
        await t.test(
          `${reset ? "reset" : "retention"} rolls back deletion when queue persistence fails`,
          async () => {
            const reference = await object();
            const id = `${prefix}-${reset}-queue-failure`;
            await seed(id, "0001-01-01T00:00:00.000Z", reference);
            await database.transaction((tx) =>
              tx.query(
                "ALTER TABLE omniroute.artifact_deletion_queue ADD CONSTRAINT shared_artifact_queue_test CHECK(relpath NOT LIKE 's3/%') NOT VALID"
              )
            );
            try {
              await assert.rejects(
                reset ? owner.reset([target], cutoff) : owner.deleteCallLogs(cutoff),
                { kind: "query_failed", sqlState: "23514" }
              );
              const row = await database.transaction((tx) =>
                tx.query<{ artifact_relpath: string }>(
                  "SELECT artifact_relpath FROM omniroute.call_logs WHERE id=$1",
                  [id]
                )
              );
              assert.equal(row.rows[0].artifact_relpath, reference);
              assert.equal(await queue(reference), "0");
              assert.deepEqual(await store.get(reference), Buffer.from('{"shared":"payload"}'));
            } finally {
              await database.transaction((tx) =>
                tx.query(
                  "ALTER TABLE omniroute.artifact_deletion_queue DROP CONSTRAINT IF EXISTS shared_artifact_queue_test"
                )
              );
              await database.transaction((tx) =>
                tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [id])
              );
            }
          }
        );
      }

      await t.test(
        "classification waits for a real native insert and sees its committed shared reference",
        async () => {
          const publicationOwner = randomUUID();
          const stored = await store.put(Buffer.from('{"shared":"payload"}'), {
            onPrepared: (artifact) =>
              prepareArtifactPublication(database, artifact, publicationOwner),
          });
          const reference = stored.reference;
          references.add(reference);
          // The fresh intent is durable before this historical/imported row shares its key.
          await seed(`${prefix}-concurrent-old`, "0001-01-01T00:00:00.000Z", reference);
          const entered = barrier(),
            release = barrier();
          let writerPid = 0,
            cleanupPid = 0;
          const writer = new PostgresCallLogRepository({
            transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
              return writerDatabase.transaction(async (tx) => {
                writerPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
                  .rows[0].pid;
                return body({
                  async query<Row>(sql: string, values?: unknown[]) {
                    const result = await tx.query<Row>(sql, values);
                    if (sql.startsWith("INSERT INTO omniroute.call_logs")) {
                      entered.resolve();
                      await release.promise;
                    }
                    return result;
                  },
                });
              });
            },
          } as PostgresDatabase);
          const cleaner = createPostgresCleanupRepository({
            transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
              return cleanupDatabase.transaction(async (tx) => {
                cleanupPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
                  .rows[0].pid;
                return body(tx);
              });
            },
          } as PostgresDatabase);
          const writing = writer.insert({
            id: `${prefix}-concurrent-new`,
            timestamp: "0003-01-01T00:00:00.000Z",
            artifactRelPath: reference,
            artifactSizeBytes: stored.sizeBytes,
            artifactSha256: stored.checksum,
            artifactPublicationOwner: publicationOwner,
            detailState: "ready",
            videoContentRemoved: 0,
          });
          void writing.catch(() => {});
          await Promise.race([
            entered.promise,
            writing.then(() => assert.fail("Native insert did not reach its transaction barrier")),
          ]);
          const deleting = cleaner.deleteCallLogs(cutoff);
          void deleting.catch(() => {});
          try {
            let witnessed = false;
            const deadline = Date.now() + 3000;
            while (Date.now() < deadline) {
              if (cleanupPid) {
                const blocked = await database.transaction((tx) =>
                  tx.query<{ owners: number[] }>("SELECT pg_blocking_pids($1) AS owners", [
                    cleanupPid,
                  ])
                );
                if (blocked.rows[0].owners.includes(writerPid)) {
                  witnessed = true;
                  break;
                }
              }
              await delay(10);
            }
            assert.equal(witnessed, true, "real PostgreSQL must witness the shared history fence");
          } finally {
            release.resolve();
            await Promise.allSettled([writing, deleting]);
          }
          const result = await deleting;
          assert.equal(result.deleted, 1);
          assert.deepEqual(result.artifacts, []);
          assert.equal(
            await queue(reference),
            "1",
            "attached publication ownership remains durable"
          );
          assert.equal((await owner.pendingArtifactDeletions()).includes(reference), false);
          const publication = await database.transaction((tx) =>
            tx.query(
              "SELECT publication_state,upload_state,publication_owner FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
              [reference]
            )
          );
          assert.deepEqual(publication.rows, [
            {
              publication_state: "attached",
              upload_state: "acknowledged",
              publication_owner: publicationOwner,
            },
          ]);
          assert.deepEqual(await store.get(reference), Buffer.from('{"shared":"payload"}'));
        }
      );
    } finally {
      await database.transaction(async (tx) => {
        await tx.query(
          "ALTER TABLE omniroute.artifact_deletion_queue DROP CONSTRAINT IF EXISTS shared_artifact_queue_test"
        );
        await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [`${prefix}%`]);
        await tx.query(
          "DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=ANY($1::text[])",
          [[...references]]
        );
      });
      await store.close();
      artifactAuthorityFixture.reset();
      await Promise.all([database.close(), writerDatabase.close(), cleanupDatabase.close()]);
      if (previous === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = previous;
    }
  }
);
