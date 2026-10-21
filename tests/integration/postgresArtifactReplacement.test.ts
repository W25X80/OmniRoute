import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  ArtifactStorageError,
  type ArtifactPutOptions,
  type NativeArtifactStore,
} from "../../src/domain/persistence/artifactStore.ts";
import { encrypt } from "../../src/lib/db/encryption.ts";
import { PostgresCallLogRepository } from "../../src/lib/db/postgres/callLogRepository.ts";
import {
  attachArtifactRecovery,
  claimArtifactRecoveryDispatch,
  readArtifactRecovery,
  replaceUncertainArtifactRecovery,
  type ArtifactRecoveryRow,
} from "../../src/lib/db/postgres/artifactRecoveryRepository.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
  type PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function barrier<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "artifact replacement preserves single-attempt ownership, bounded liabilities and privacy on real SQL",
  { skip: !postgresTestsEnabled, timeout: 90000 },
  async (t) => {
    const database = await createTestPostgresDatabase();
    const prefix = "artifact-replacement-" + randomUUID();
    const previousKey = process.env.STORAGE_ENCRYPTION_KEY;
    const previousDataDir = process.env.DATA_DIR;
    const directory = mkdtempSync(join(tmpdir(), "omni-artifact-replacement-"));
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    const scope = "c".repeat(64);
    let sequence = 0;
    const row = async (reference: string) => {
      const value = await readArtifactRecovery(database, reference);
      assert.ok(value);
      return value;
    };
    const lineage = async (original: ArtifactRecoveryRow) =>
      (
        await database.transaction((tx) =>
          tx.query<ArtifactRecoveryRow & { staged_bytes: number }>(
            `SELECT * FROM omniroute.artifact_deletion_queue
           WHERE call_log_order=$1 AND call_log_id=$2 ORDER BY created_at,relpath`,
            [original.call_log_order, original.call_log_id]
          )
        )
      ).rows;
    const due = async (reference: string) =>
      database.transaction((tx) =>
        tx.query(
          "UPDATE omniroute.artifact_deletion_queue SET next_attempt_at='0001-01-01'::timestamptz WHERE relpath=$1",
          [reference]
        )
      );
    const seed = async (keyId?: string) => {
      const id = prefix + "-" + ++sequence;
      const bytes = Buffer.from(JSON.stringify({ summary: { id }, body: "private staging" }));
      const checksum = createHash("sha256").update(bytes).digest("hex");
      const reference = `s3/${randomUUID()}_${checksum}.json`;
      const inserted = await new PostgresCallLogRepository(database).insert(
        {
          id,
          apiKeyId: keyId,
          timestamp: new Date().toISOString(),
          detailState: "missing",
          detailReason: "publication_pending",
          videoContentRemoved: 0,
        },
        {
          artifact: {
            reference,
            checksum,
            sizeBytes: bytes.byteLength,
            storageScope: scope,
            singleAttempt: true,
          },
          owner: randomUUID(),
          ciphertext: encrypt(bytes.toString("base64"))!,
        }
      );
      assert.equal(inserted.intentAccepted, true);
      const prepared = await row(reference);
      assert.equal(
        await claimArtifactRecoveryDispatch(
          database,
          reference,
          prepared.publication_owner,
          randomUUID()
        ),
        true
      );
      await due(reference);
      return { bytes, original: await row(reference) };
    };
    const testStore = (bytes: Uint8Array) => {
      const objects = new Map<string, Uint8Array>();
      const puts: string[] = [];
      let unknown = false;
      let deleteFailure = false;
      let getHook: ((reference: string) => Promise<Uint8Array | null>) | undefined;
      const store: NativeArtifactStore = {
        storageScope: scope,
        isPutActive: () => false,
        async put(body: Uint8Array, options: ArtifactPutOptions = {}) {
          assert.ok(options.prepared);
          await options.onPrepared?.(options.prepared);
          assert.deepEqual(body, bytes);
          puts.push(options.prepared.reference);
          if (unknown)
            throw new ArtifactStorageError("put_unknown", options.prepared.reference, true);
          objects.set(options.prepared.reference, body);
          return options.prepared;
        },
        async get(reference) {
          return getHook ? getHook(reference) : (objects.get(reference) ?? null);
        },
        async delete(reference) {
          if (deleteFailure) throw new ArtifactStorageError("unavailable");
          return objects.delete(reference);
        },
        async close() {},
      };
      return {
        store,
        puts,
        objects,
        unknown(value: boolean) {
          unknown = value;
        },
        failDelete(value: boolean) {
          deleteFailure = value;
        },
        get(hook?: typeof getHook) {
          getHook = hook;
        },
      };
    };
    try {
      const { recoverNativeArtifact } =
        await import("../../src/lib/usage/nativeArtifactRecovery.ts");
      await migratePostgresSchema(database);
      await t.test(
        "concurrent transfers preserve one staging owner and exactly two identities",
        async () => {
          const { original } = await seed();
          const before = await lineage(original);
          const outcomes = await Promise.all([
            replaceUncertainArtifactRecovery(database, original, () => false),
            replaceUncertainArtifactRecovery(database, original, () => false),
          ]);
          assert.deepEqual(outcomes.sort(), [false, true]);
          const after = await lineage(original);
          assert.equal(after.length, 2);
          assert.equal(
            after.reduce((sum, value) => sum + value.staged_bytes, 0),
            before[0].staged_bytes
          );
          const retired = after.find((value) => value.relpath === original.relpath)!;
          const current = after.find((value) => value.relpath !== original.relpath)!;
          assert.equal(retired.publication_state, "delete_pending");
          assert.equal(retired.upload_state, "uncertain");
          assert.equal(retired.staged_payload, null);
          assert.equal(retired.dispatch_owner, original.dispatch_owner);
          assert.equal(current.publication_state, "publishing");
          assert.equal(current.upload_state, "not_sent");
          assert.equal(current.staged_payload, original.staged_payload);
          assert.equal(current.dispatch_owner, null);
          assert.notEqual(current.publication_owner, original.publication_owner);
          assert.ok(current.relpath.endsWith(`_${original.expected_sha256}.json`));
          assert.equal(
            await attachArtifactRecovery(database, original.relpath, original.publication_owner),
            false
          );
        }
      );

      await t.test(
        "a late original acknowledgement cannot overwrite its attached successor",
        async () => {
          const { original, bytes } = await seed();
          const transport = testStore(bytes);
          const entered = barrier();
          const release = barrier<Uint8Array | null>();
          transport.get(async (reference) => {
            if (reference === original.relpath) {
              entered.resolve();
              return release.promise;
            }
            return transport.objects.get(reference) ?? null;
          });
          const late = recoverNativeArtifact(original.relpath, database, transport.store);
          void late.catch(() => {});
          try {
            await entered.promise;
            assert.equal(
              await replaceUncertainArtifactRecovery(database, original, () => false),
              true
            );
            const successor = (await lineage(original)).find(
              (value) => value.relpath !== original.relpath
            )!;
            await due(successor.relpath);
            assert.equal(
              await recoverNativeArtifact(successor.relpath, database, transport.store),
              true
            );
            release.resolve(bytes);
            assert.equal(await late, false);
            const saved = await database.transaction((tx) =>
              tx.query<{ artifact_relpath: string }>(
                "SELECT artifact_relpath FROM omniroute.call_logs WHERE id=$1",
                [original.call_log_id]
              )
            );
            assert.equal(saved.rows[0].artifact_relpath, successor.relpath);
            assert.equal((await row(original.relpath)).upload_state, "acknowledged");
            assert.deepEqual(transport.puts, [successor.relpath]);
          } finally {
            release.resolve(bytes);
            await late;
          }
        }
      );

      await t.test(
        "late original positive GET cannot attach and failed DELETE still consumes credit",
        async () => {
          const { original, bytes } = await seed();
          const transport = testStore(bytes);
          const entered = barrier();
          const release = barrier<Uint8Array | null>();
          transport.get(async (reference) => {
            if (reference === original.relpath) {
              entered.resolve();
              return release.promise;
            }
            return null;
          });
          const late = recoverNativeArtifact(original.relpath, database, transport.store);
          void late.catch(() => {});
          try {
            await entered.promise;
            assert.equal(
              await replaceUncertainArtifactRecovery(database, original, () => false),
              true
            );
            const successor = (await lineage(original)).find(
              (value) => value.relpath !== original.relpath
            )!;
            await due(successor.relpath);
            transport.unknown(true);
            t.mock.method(console, "warn", () => {});
            assert.equal(
              await recoverNativeArtifact(successor.relpath, database, transport.store),
              false
            );
            release.resolve(bytes);
            assert.equal(await late, false);
            assert.equal((await row(original.relpath)).upload_state, "acknowledged");
            const current = await row(successor.relpath);
            await due(current.relpath);
            assert.equal(
              await replaceUncertainArtifactRecovery(database, current, () => false),
              false
            );
            assert.equal(
              (await lineage(original)).length,
              2,
              "GET acknowledgement without DELETE retains credit"
            );
            const summary = await database.transaction((tx) =>
              tx.query<{ artifact_relpath: string | null }>(
                "SELECT artifact_relpath FROM omniroute.call_logs WHERE id=$1",
                [original.call_log_id]
              )
            );
            assert.equal(summary.rows[0].artifact_relpath, null);
            // Until DELETE is acknowledged, including a transport failure, the
            // acknowledged GET row must continue consuming identity credit.
            transport.objects.set(original.relpath, bytes);
            transport.failDelete(true);
            await assert.rejects(
              transport.store.delete(original.relpath),
              (error) => error instanceof ArtifactStorageError && error.kind === "unavailable"
            );
            const retained = await row(original.relpath);
            assert.equal(retained.publication_state, "delete_pending");
            assert.equal(retained.upload_state, "acknowledged");
            assert.equal(retained.staged_payload, null);
            assert.equal(transport.objects.has(original.relpath), true);
            await due(current.relpath);
            assert.equal(
              await replaceUncertainArtifactRecovery(database, current, () => false),
              false,
              "failed DELETE leaves its acknowledged SQL liability consuming credit"
            );
            assert.equal((await lineage(original)).length, 2);
            assert.equal((await row(current.relpath)).staged_payload, current.staged_payload);
            transport.failDelete(false);
            assert.equal(await transport.store.delete(original.relpath), true);
            assert.equal(
              await createPostgresCleanupRepository(database).acknowledgeArtifactDeletion(
                original.relpath
              ),
              true
            );
            await due(current.relpath);
            assert.equal(
              await replaceUncertainArtifactRecovery(database, current, () => false),
              true
            );
            assert.equal((await lineage(original)).length, 2);
            assert.equal(
              transport.puts.length,
              1,
              "allocating a successor never dispatches inline"
            );
          } finally {
            release.resolve(bytes);
            await late;
          }
        }
      );

      await t.test(
        "lost transfer COMMIT ACK dispatches nothing and restart-shaped lookup finds its successor",
        async () => {
          const { original, bytes } = await seed();
          const transport = testStore(bytes);
          const wrapped = {
            async transaction<T>(
              body: (tx: PostgresTransaction) => Promise<T>,
              options: PostgresTransactionOptions = {}
            ) {
              const result = await database.transaction(body, options);
              if (options.operation === "artifact.replace")
                throw new PostgresError("commit_unknown");
              return result;
            },
          } as unknown as PostgresDatabase;
          t.mock.method(console, "warn", () => {});
          assert.equal(
            await recoverNativeArtifact(original.relpath, wrapped, transport.store),
            false
          );
          assert.equal(transport.puts.length, 0);
          const rows = await lineage(original);
          assert.equal(rows.length, 2);
          const successor = rows.find((value) => value.relpath !== original.relpath)!;
          await due(successor.relpath);
          assert.equal(
            await recoverNativeArtifact(successor.relpath, database, transport.store),
            true
          );
          assert.deepEqual(transport.puts, [successor.relpath]);
        }
      );

      await t.test("insertion collision rolls back retirement and staging transfer", async () => {
        const { original } = await seed();
        const wrapped = {
          transaction<T>(
            body: (tx: PostgresTransaction) => Promise<T>,
            options?: PostgresTransactionOptions
          ) {
            return database.transaction(
              (tx) =>
                body({
                  noteAdvisoryLock: tx.noteAdvisoryLock?.bind(tx),
                  async query<Row>(sql: string, values?: unknown[]) {
                    if (sql.startsWith("WITH source AS MATERIALIZED"))
                      await tx.query(
                        "INSERT INTO omniroute.artifact_deletion_queue(relpath) VALUES($1)",
                        [values![1]]
                      );
                    return tx.query<Row>(sql, values);
                  },
                }),
              options
            );
          },
        } as unknown as PostgresDatabase;
        await assert.rejects(replaceUncertainArtifactRecovery(wrapped, original, () => false));
        const after = await lineage(original);
        assert.equal(after.length, 1);
        assert.equal(after[0].publication_state, "publishing");
        assert.equal(after[0].staged_payload, original.staged_payload);
      });

      await t.test(
        "privacy revocation and call reincarnation cannot receive transferred bytes",
        async () => {
          for (const revoke of ["no_log", "key_delete", "call_recreate"] as const) {
            const key = prefix + "-key-" + revoke;
            await database.transaction((tx) =>
              tx.query(
                "INSERT INTO omniroute.api_keys(id,name,key,no_log,created_at) VALUES($1,'replacement test',$2,0,$3)",
                [key, randomUUID(), new Date().toISOString()]
              )
            );
            const { original } = await seed(key);
            await database.transaction(async (tx) => {
              if (revoke === "no_log")
                await tx.query("UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1", [key]);
              else if (revoke === "key_delete")
                await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [key]);
              else {
                await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [
                  original.call_log_id,
                ]);
                await tx.query(
                  "INSERT INTO omniroute.call_logs(id,timestamp,detail_state,detail_reason,video_content_removed) VALUES($1,$2,'missing','publication_pending',0)",
                  [original.call_log_id, new Date().toISOString()]
                );
              }
            });
            assert.equal(
              await replaceUncertainArtifactRecovery(database, original, () => false),
              false
            );
            const retained = await lineage(original);
            assert.equal(retained.length, 1);
            assert.equal(retained[0].publication_state, "delete_pending");
            assert.equal(retained[0].staged_payload, null);
          }
        }
      );

      await t.test(
        "active raw PUT and mismatched captured identities refuse transfer",
        async () => {
          const { original } = await seed();
          assert.equal(
            await replaceUncertainArtifactRecovery(database, original, () => true),
            false
          );
          for (const changed of [
            { publication_owner: randomUUID() },
            { dispatch_owner: randomUUID() },
            { call_log_id: original.call_log_id + "-stale" },
            { call_log_order: String(BigInt(original.call_log_order) + 1n) },
            { store_scope: "d".repeat(64) },
            { expected_size_bytes: String(BigInt(original.expected_size_bytes) + 1n) },
            { expected_sha256: "e".repeat(64) },
          ]) {
            assert.equal(
              await replaceUncertainArtifactRecovery(
                database,
                { ...original, ...changed },
                () => false
              ),
              false
            );
            assert.equal((await lineage(original)).length, 1);
            assert.equal((await row(original.relpath)).staged_payload, original.staged_payload);
          }
        }
      );
    } finally {
      try {
        await database.transaction(async (tx) => {
          await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [prefix + "%"]);
          await tx.query(
            "DELETE FROM omniroute.artifact_deletion_queue WHERE call_log_id LIKE $1",
            [prefix + "%"]
          );
          await tx.query("DELETE FROM omniroute.api_keys WHERE id LIKE $1", [prefix + "%"]);
        });
      } finally {
        await database.close();
        if (previousKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
        else process.env.STORAGE_ENCRYPTION_KEY = previousKey;
        if (previousDataDir === undefined) delete process.env.DATA_DIR;
        else process.env.DATA_DIR = previousDataDir;
        rmSync(directory, { recursive: true, force: true });
      }
    }
  }
);
