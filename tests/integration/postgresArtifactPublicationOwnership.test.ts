import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  fixture,
  barrier,
  configuration,
} from "../helpers/persistence/artifactPublicationFixture.ts";
import {
  PostgresDatabase,
  PostgresError,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";

test(
  "native artifact publication owns crash gaps and actual SQL fences",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const source = new URL("../../src/lib/artifacts/s3ArtifactStore.ts", import.meta.url).href;
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const mock = new URL("../helpers/persistence/artifactPublicationFixture.ts", import.meta.url)
      .href;
    const realBoot = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        const parent = context.parentURL?.split("?")[0];
        if (
          parent === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
          ].includes(specifier)
        )
          return { url: realBoot, shortCircuit: true };
        if (parent === runtimeUrl && specifier === "../artifacts/s3ArtifactStore")
          return { url: mock, shortCircuit: true };
        if (parent === source && specifier === "@aws-sdk/client-s3")
          return { url: mock, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const directory = mkdtempSync(join(tmpdir(), "omniroute-artifact-publication-"));
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "NODE_ENV",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    fixture.reset();
    const store = await import(source);
    fixture.factory = store.createS3ArtifactStore;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let logs: typeof import("../../src/lib/usage/callLogs.ts") | undefined;
    const database = await createTestPostgresDatabase(),
      prefix = "artifact-owner-" + randomUUID();
    let rejectId: string | undefined, loseCommitId: string | undefined;
    let loseDispatchCommit = false,
      loseAttachCommit = false;
    const original = PostgresDatabase.prototype.transaction;
    t.mock.method(PostgresDatabase.prototype, "transaction", async function <
      T,
    >(this: PostgresDatabase, body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
      let insertedId: string | undefined;
      let dispatchClaim = false,
        attachment = false;
      const value = await original.call(this, async (tx: PostgresTransaction) =>
        body({
          async query<Row>(text: string, values?: unknown[]) {
            if (text.includes("INSERT INTO omniroute.call_logs(")) {
              insertedId = String(values?.[0]);
              if (insertedId === rejectId) await tx.query("SELECT 1/0");
            }
            dispatchClaim ||= text.includes("SET upload_state='uncertain',dispatch_owner=");
            attachment ||= text.includes(
              "SET detail_state='ready',detail_reason=NULL,artifact_relpath="
            );
            return tx.query<Row>(text, values);
          },
        })
      );
      if ((dispatchClaim && loseDispatchCommit) || (attachment && loseAttachCommit)) {
        loseDispatchCommit = false;
        loseAttachCommit = false;
        throw new PostgresError("commit_unknown");
      }
      if (insertedId && insertedId === loseCommitId) {
        loseCommitId = undefined;
        throw new PostgresError("commit_unknown");
      }
      return value as T;
    });
    const entry = (id: string, key: string) => ({
      id,
      apiKeyId: key,
      provider: "artifact-test",
      model: "artifact-test",
      status: 200,
      requestBody: { messages: [{ role: "user", content: "retained identity" }] },
      responseBody: { choices: [{ message: { content: "response" } }] },
    });
    const seed = async (key: string) =>
      database.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.api_keys(id,name,key,no_log,created_at) VALUES($1,'artifact owner test',$2,0,$3)",
          [key, randomUUID(), new Date().toISOString()]
        )
      );
    const rows = async (id: string) =>
      (
        await database.transaction((tx) =>
          tx.query<{
            artifact_relpath: string | null;
            detail_state: string;
            request_summary: string | null;
          }>(
            "SELECT artifact_relpath,detail_state,request_summary FROM omniroute.call_logs WHERE id=$1",
            [id]
          )
        )
      ).rows;
    const intents = async () =>
      (
        await database.transaction((tx) =>
          tx.query<{
            relpath: string;
            publication_state: string;
            upload_state: string;
            publication_owner: string | null;
            store_scope: string | null;
          }>(
            "SELECT relpath,publication_state,upload_state,publication_owner,store_scope FROM omniroute.artifact_deletion_queue WHERE store_scope=$1",
            [storeScope]
          )
        )
      ).rows;
    let storeScope: string | undefined;
    const changedRefs = new Set<string>();
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      storeScope = await runtime!.runPersistenceRequest(
        () => runtime!.getPersistenceArtifactStore().storageScope
      );
      logs = await import("../../src/lib/usage/callLogs.ts");
      const publication = await import("../../src/lib/usage/nativeArtifactPublication.ts");
      const recovery = await import("../../src/lib/usage/nativeArtifactRecovery.ts");
      const recover = (reference: string) =>
        runtime!.runPersistenceRequest(() => recovery.recoverNativeArtifact(reference));
      const remove = (paths: readonly string[]) =>
        runtime!.runPersistenceRequest(() => publication.deleteNativeCallArtifacts(paths));
      const save = (entry: Parameters<NonNullable<typeof logs>["saveCallLog"]>[0]) =>
        runtime!.runPersistenceRequest(() => logs!.saveCallLog(entry));
      const collect = async () => {
        const all = await intents();
        for (const row of all) changedRefs.add(row.relpath);
        return all;
      };
      await t.test(
        "actual journal precedes SDK PUT and full publication remains scoped attached",
        async () => {
          const id = prefix + "-ack",
            key = prefix + "-ack-key";
          await seed(key);
          fixture.writeGate = barrier();
          fixture.putEntered = barrier();
          const pending = save(entry(id, key));
          void pending.catch(() => {});
          await fixture.putEntered.promise;
          const before = await collect();
          assert.equal(before.length, 1);
          assert.equal(before[0].publication_state, "publishing");
          assert.equal(before[0].upload_state, "uncertain");
          assert.match(before[0].publication_owner!, /^[0-9a-f-]{36}$/);
          assert.equal((await rows(id)).length, 1);
          assert.equal((await rows(id))[0].detail_state, "missing");
          fixture.writeGate.resolve();
          await pending;
          fixture.writeGate = undefined;
          const after = await collect();
          assert.equal(after[0].publication_state, "attached");
          assert.equal(after[0].upload_state, "acknowledged");
          assert.equal((await rows(id))[0].artifact_relpath, after[0].relpath);
          const indices = (
            await database.transaction((tx) =>
              tx.query<{ indexname: string }>(
                "SELECT indexname FROM pg_indexes WHERE schemaname='omniroute' AND indexname IN ('call_logs_artifact_relpath_idx','call_logs_v1_legacy_artifact_relpath_idx')"
              )
            )
          ).rows;
          assert.equal(indices.length, 2);
          const selected = await import("../../src/lib/db/controlPlaneContext.ts"),
            cleanup = await import("../../src/lib/db/postgres/cleanupRepository.ts");
          await runtime!.runPersistenceRequest(async () =>
            assert.equal(
              (
                await cleanup
                  .createPostgresCleanupRepository(selected.getSelectedPostgresDatabase()!)
                  .pendingArtifactDeletions()
              ).includes(after[0].relpath),
              false
            )
          );
        }
      );
      await t.test(
        "lost PUT ACK preserves metadata and positive GET attaches exactly one logical artifact",
        async () => {
          const id = prefix + "-lost",
            key = prefix + "-lost-key";
          await seed(key);
          fixture.putMode = "lost";
          await save(entry(id, key));
          fixture.putMode = "ack";
          assert.equal((await rows(id)).length, 1);
          assert.equal((await rows(id))[0].detail_state, "missing");
          const row = (await collect()).find(
            (item) => item.publication_state === "publishing" && item.upload_state === "uncertain"
          )!;
          assert.ok(row);
          const puts = fixture.requests.filter((request) => request.method === "PUT").length;
          assert.equal(await recover(row.relpath), true);
          assert.equal(await recover(row.relpath), true);
          assert.equal((await rows(id))[0].artifact_relpath, row.relpath);
          assert.equal(fixture.requests.filter((request) => request.method === "PUT").length, puts);
          assert.deepEqual(await remove([row.relpath]), {
            deletedArtifacts: 0,
            pendingArtifactDeletions: 0,
            errors: 0,
          });
        }
      );
      await t.test(
        "GET404 cannot retire a sole remote PUT which completes after client settlement",
        async () => {
          const id = prefix + "-late",
            key = prefix + "-late-key";
          await seed(key);
          fixture.putMode = "lost_before_effect";
          fixture.writeGate = barrier();
          await save(entry(id, key));
          const row = (await collect()).find(
            (item) => item.publication_state === "publishing" && item.upload_state === "uncertain"
          )!;
          assert.ok(row);
          const puts = fixture.requests.filter((request) => request.method === "PUT").length;
          assert.equal(await recover(row.relpath), false);
          assert.equal((await rows(id))[0].artifact_relpath, null);
          assert.deepEqual(await remove([row.relpath]), {
            deletedArtifacts: 0,
            pendingArtifactDeletions: 1,
            errors: 0,
          });
          fixture.writeGate.resolve();
          await fixture.remoteEffect;
          fixture.writeGate = undefined;
          fixture.putMode = "ack";
          assert.equal(await recover(row.relpath), true);
          assert.equal((await rows(id))[0].artifact_relpath, row.relpath);
          assert.equal(fixture.requests.filter((request) => request.method === "PUT").length, puts);
        }
      );
      await t.test("metadata transaction rejection dispatches no PUT", async () => {
        const id = prefix + "-reject",
          key = prefix + "-reject-key";
        await seed(key);
        rejectId = id;
        const puts = fixture.requests.filter((request) => request.method === "PUT").length;
        await save(entry(id, key));
        rejectId = undefined;
        assert.equal((await rows(id)).length, 0);
        assert.equal(fixture.requests.filter((request) => request.method === "PUT").length, puts);
      });
      await t.test(
        "lost actual COMMIT acknowledgment retains both current and legacy ownership fences",
        async () => {
          const id = prefix + "-commit",
            key = prefix + "-commit-key";
          await seed(key);
          loseCommitId = id;
          await save(entry(id, key));
          const pending = (await collect()).find(
            (item) => item.publication_state === "publishing" && item.upload_state === "not_sent"
          )!;
          assert.ok(pending);
          assert.equal((await rows(id))[0].artifact_relpath, null);
          assert.equal(await recover(pending.relpath), true);
          const committed = (await rows(id))[0];
          assert.ok(committed?.artifact_relpath);
          changedRefs.add(committed.artifact_relpath!);
          assert.equal((await remove([committed.artifact_relpath!])).deletedArtifacts, 0);
          await database.transaction(async (tx) => {
            await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
            await tx.query(
              `INSERT INTO omniroute.call_logs_v1_legacy(id,timestamp,artifact_relpath,insertion_order)
               SELECT $2,timestamp,artifact_relpath,insertion_order FROM omniroute.call_logs WHERE id=$1`,
              [id, id + "-legacy"]
            );
            await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [id]);
            await tx.query(
              "UPDATE omniroute.artifact_deletion_queue SET publication_state='delete_pending' WHERE relpath=$1",
              [committed.artifact_relpath]
            );
          });
          assert.equal((await remove([committed.artifact_relpath!])).deletedArtifacts, 0);
          assert.equal(
            (await collect()).find((item) => item.relpath === committed.artifact_relpath)!
              .publication_state,
            "attached"
          );
        }
      );
      await t.test(
        "unknown dispatch COMMIT never authorizes PUT, even after restart-shaped recovery",
        async () => {
          const id = prefix + "-dispatch-unknown",
            key = id + "-key";
          await seed(key);
          const puts = fixture.requests.filter((request) => request.method === "PUT").length;
          loseDispatchCommit = true;
          await save(entry(id, key));
          const pending = (await collect()).find(
            (row) => row.publication_state === "publishing" && row.upload_state === "uncertain"
          )!;
          assert.ok(pending);
          assert.equal((await rows(id))[0].detail_state, "missing");
          assert.equal(await recover(pending.relpath), false);
          assert.equal(fixture.requests.filter((request) => request.method === "PUT").length, puts);
          assert.ok((await collect()).some((row) => row.relpath === pending.relpath));
          // This row intentionally remains unresolved: 404 is not non-dispatch proof.
        }
      );
      await t.test(
        "unknown attachment COMMIT is read back without another PUT or duplicate log",
        async () => {
          const id = prefix + "-attach-unknown",
            key = id + "-key";
          await seed(key);
          const puts = fixture.requests.filter((request) => request.method === "PUT").length;
          loseAttachCommit = true;
          await save(entry(id, key));
          const saved = (await rows(id))[0];
          assert.equal(saved.detail_state, "ready");
          assert.equal(await recover(saved.artifact_relpath!), true);
          assert.equal((await rows(id)).length, 1);
          assert.equal(
            fixture.requests.filter((request) => request.method === "PUT").length,
            puts + 1
          );
          const staged = await database.transaction((tx) =>
            tx.query<{ staged_payload: string | null; staged_bytes: number }>(
              "SELECT staged_payload,staged_bytes FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
              [saved.artifact_relpath]
            )
          );
          assert.deepEqual(staged.rows[0], { staged_payload: null, staged_bytes: 0 });
        }
      );
      await t.test(
        "128 MiB staging exhaustion retains metadata with explicit reason and no PUT",
        async () => {
          const id = prefix + "-pressure",
            key = id + "-key",
            pressure = prefix + "-capacity-";
          await seed(key);
          const puts = fixture.requests.filter((request) => request.method === "PUT").length;
          try {
            await database.transaction((tx) =>
              tx.query(
                `INSERT INTO omniroute.artifact_deletion_queue
              (relpath,publication_state,call_log_id,call_log_order,staged_payload,staged_bytes)
             SELECT $1||n,'publishing',$1||n,0,'enc:v1:'||repeat('a',16777209),16777216 FROM generate_series(1,8) n`,
                [pressure]
              )
            );
            await save(entry(id, key));
            const saved = await database.transaction((tx) =>
              tx.query<{ detail_state: string; detail_reason: string }>(
                "SELECT detail_state,detail_reason FROM omniroute.call_logs WHERE id=$1",
                [id]
              )
            );
            assert.deepEqual(saved.rows[0], {
              detail_state: "missing",
              detail_reason: "staging_capacity",
            });
            assert.equal(
              fixture.requests.filter((request) => request.method === "PUT").length,
              puts
            );
          } finally {
            await database.transaction((tx) =>
              tx.query("DELETE FROM omniroute.artifact_deletion_queue WHERE relpath LIKE $1", [
                pressure + "%",
              ])
            );
          }
        }
      );
      await t.test(
        "deleting metadata revokes pending bytes and leaves uncertain late-PUT cleanup authority",
        async () => {
          const id = prefix + "-delete-pending",
            key = id + "-key";
          await seed(key);
          fixture.putMode = "lost_before_effect";
          fixture.writeGate = barrier();
          await save(entry(id, key));
          const pending = await database.transaction((tx) =>
            tx.query<{ relpath: string; staged_payload: string | null }>(
              "SELECT relpath,staged_payload FROM omniroute.artifact_deletion_queue WHERE call_log_id=$1",
              [id]
            )
          );
          assert.ok(pending.rows[0].staged_payload?.startsWith("enc:v1:"));
          const ref = pending.rows[0].relpath;
          changedRefs.add(ref);
          await database.transaction((tx) =>
            tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [id])
          );
          const tombstone = await database.transaction((tx) =>
            tx.query<{ staged_payload: string | null; staged_bytes: number }>(
              "SELECT staged_payload,staged_bytes FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
              [ref]
            )
          );
          assert.deepEqual(tombstone.rows[0], { staged_payload: null, staged_bytes: 0 });
          assert.equal(await recover(ref), false);
          assert.equal((await remove([ref])).pendingArtifactDeletions, 1);
          fixture.writeGate.resolve();
          await fixture.remoteEffect;
          fixture.writeGate = undefined;
          fixture.putMode = "ack";
          assert.equal((await remove([ref])).deletedArtifacts, 1);
          assert.equal((await rows(id)).length, 0);
        }
      );
      for (const revoke of [false, true])
        await t.test(
          "privacy mutation during PUT omits payload and immediately drains terminal claimed deletion " +
            revoke,
          async () => {
            const id = prefix + "-private-" + revoke,
              key = id + "-key";
            await seed(key);
            fixture.writeGate = barrier();
            fixture.putEntered = barrier();
            const before = fixture.requests.filter((request) => request.method === "DELETE").length,
              pending = save(entry(id, key));
            void pending.catch(() => {});
            await fixture.putEntered.promise;
            const ref = (
              await database.transaction((tx) =>
                tx.query<{ relpath: string }>(
                  "SELECT relpath FROM omniroute.artifact_deletion_queue WHERE call_log_id=$1",
                  [id]
                )
              )
            ).rows[0].relpath;
            changedRefs.add(ref);
            await database.transaction(async (tx) => {
              await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
              await tx.query(
                revoke
                  ? "DELETE FROM omniroute.api_keys WHERE id=$1"
                  : "UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1",
                [key]
              );
            });
            fixture.writeGate.resolve();
            await pending;
            fixture.writeGate = undefined;
            const saved = (await rows(id))[0];
            assert.equal(saved.artifact_relpath, null);
            assert.equal(saved.detail_state, "none");
            assert.equal(saved.request_summary, null);
            assert.equal(
              (await collect()).some((item) => item.relpath === ref),
              false
            );
            assert.equal(
              fixture.requests.filter((request) => request.method === "DELETE").length,
              before + 1
            );
          }
        );
      for (const outcome of ["acknowledged", "unknown", "revoked"] as const) {
        await t.test(
          `deadline retains claim lifetime before late ${outcome} COMMIT acknowledgement`,
          async () => {
            const id = prefix + "-claim-deadline-" + outcome,
              key = id + "-key";
            await seed(key);
            loseCommitId = id;
            await save(entry(id, key));
            const pending = (
              await database.transaction((tx) =>
                tx.query<{ relpath: string }>(
                  "SELECT relpath FROM omniroute.artifact_deletion_queue WHERE call_log_id=$1",
                  [id]
                )
              )
            ).rows[0];
            changedRefs.add(pending.relpath);
            const claimEntered = barrier(),
              releaseClaim = barrier(),
              deadlineSeen = barrier();
            const boundedStore = store.createS3ArtifactStore(configuration, { deadlineMs: 30 });
            await boundedStore.warmup!();
            let claimHeld = false,
              finished = false;
            const heldDatabase = {
              async transaction<T>(
                body: (tx: PostgresTransaction) => Promise<T>,
                options: { operation?: string } = {}
              ) {
                const result = await database.transaction(body, options);
                if (options.operation === "artifact.claim" && !claimHeld) {
                  claimHeld = true;
                  claimEntered.resolve();
                  await releaseClaim.promise;
                  if (outcome === "unknown") throw new PostgresError("commit_unknown");
                }
                return result;
              },
            } as unknown as PostgresDatabase;
            const observedStore = {
              ...boundedStore,
              async put(bytes: Uint8Array, options: Parameters<typeof boundedStore.put>[1]) {
                try {
                  return await boundedStore.put(bytes, options);
                } catch (error) {
                  deadlineSeen.resolve();
                  throw error;
                }
              },
            };
            const puts = fixture.requests.filter((request) => request.method === "PUT").length;
            const recovering = runtime!.runPersistenceRequest(() =>
              recovery.recoverNativeArtifact(pending.relpath, heldDatabase, observedStore)
            );
            void recovering.finally(() => {
              finished = true;
            });
            try {
              await claimEntered.promise;
              await deadlineSeen.promise;
              // Join a real SQL read after the store deadline; this cannot release
              // the held claim acknowledgement or authorize a second send.
              assert.equal((await rows(id))[0].detail_state, "missing");
              assert.equal(finished, false, "recovery retains ownership until claim settlement");
              if (outcome === "revoked")
                await database.transaction((tx) =>
                  tx.query("UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1", [key])
                );
              releaseClaim.resolve();
              assert.equal(await recovering, false);
              const current = (
                await database.transaction((tx) =>
                  tx.query<{
                    upload_state: string;
                    publication_state: string;
                    staged_payload: string | null;
                  }>(
                    "SELECT upload_state,publication_state,staged_payload FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
                    [pending.relpath]
                  )
                )
              ).rows[0];
              assert.equal(
                fixture.requests.filter((request) => request.method === "PUT").length,
                puts
              );
              assert.equal(
                current.upload_state,
                outcome === "acknowledged" ? "not_sent" : "uncertain"
              );
              if (outcome === "revoked") {
                assert.equal(current.publication_state, "delete_pending");
                assert.equal(current.staged_payload, null);
                assert.equal(await recover(pending.relpath), false);
              } else if (outcome === "unknown") {
                assert.equal(await recover(pending.relpath), false);
                assert.equal(
                  fixture.requests.filter((request) => request.method === "PUT").length,
                  puts
                );
              } else {
                await boundedStore.close();
                assert.equal(await recover(pending.relpath), true);
                assert.equal(
                  fixture.requests.filter((request) => request.method === "PUT").length,
                  puts + 1
                );
                assert.equal((await rows(id))[0].detail_state, "ready");
              }
            } finally {
              releaseClaim.resolve();
              await recovering;
              await boundedStore.close();
            }
          }
        );
      }
      await t.test(
        "committed not-sent intent survives runtime restart and concurrent recovery performs one PUT",
        async () => {
          const id = prefix + "-restart",
            key = id + "-key";
          await seed(key);
          loseCommitId = id;
          await save(entry(id, key));
          const pending = (
            await database.transaction((tx) =>
              tx.query<{ relpath: string; staged_payload: string }>(
                "SELECT relpath,staged_payload FROM omniroute.artifact_deletion_queue WHERE call_log_id=$1",
                [id]
              )
            )
          ).rows[0];
          assert.ok(pending.staged_payload.startsWith("enc:v1:"));
          assert.equal(pending.staged_payload.includes("retained identity"), false);
          changedRefs.add(pending.relpath);
          const puts = fixture.requests.filter((request) => request.method === "PUT").length;
          await runtime!.closePersistenceRuntime();
          delete globalThis.__omniroutePersistenceRuntime;
          await runtime!.ensurePersistenceReady();
          // SDK/client preparation can fail without consuming the durable one-PUT claim.
          await runtime!.runPersistenceRequest(async () => {
            const selected = await import("../../src/lib/db/controlPlaneContext.ts");
            const native = runtime!.getPersistenceArtifactStore();
            assert.equal(
              await recovery.recoverNativeArtifact(
                pending.relpath,
                selected.getSelectedPostgresDatabase()!,
                {
                  ...native,
                  async warmup() {
                    throw new Error("synthetic local preparation failure");
                  },
                }
              ),
              false
            );
          });
          assert.equal(fixture.requests.filter((request) => request.method === "PUT").length, puts);
          const results = await Promise.all([recover(pending.relpath), recover(pending.relpath)]);
          assert.ok(results.includes(true));
          assert.equal(
            fixture.requests.filter((request) => request.method === "PUT").length,
            puts + 1
          );
          assert.equal((await rows(id)).length, 1);
          assert.equal((await rows(id))[0].detail_state, "ready");
        }
      );
    } finally {
      fixture.writeGate?.resolve();
      await fixture.remoteEffect?.catch(() => {});
      rejectId = undefined;
      loseCommitId = undefined;
      t.mock.restoreAll();
      await runtime?.runPersistenceShutdownWork(async () => {
        await logs?.closeCallLogSaves();
      });
      await database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
        await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [prefix + "%"]);
        await tx.query("DELETE FROM omniroute.call_logs_v1_legacy WHERE id LIKE $1", [
          prefix + "%",
        ]);
        await tx.query(
          "DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=ANY($1::text[])",
          [[...changedRefs]]
        );
        await tx.query("DELETE FROM omniroute.api_keys WHERE id LIKE $1", [prefix + "%"]);
      });
      await runtime?.closePersistenceRuntime();
      await database.close();
      hooks.deregister();
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
