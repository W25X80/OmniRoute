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
import { fixture, barrier } from "../helpers/persistence/artifactPublicationFixture.ts";
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
    const original = PostgresDatabase.prototype.transaction;
    t.mock.method(PostgresDatabase.prototype, "transaction", async function <
      T,
    >(this: PostgresDatabase, body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
      let insertedId: string | undefined;
      const value = await original.call(this, async (tx: PostgresTransaction) =>
        body({
          async query<Row>(text: string, values?: unknown[]) {
            if (text.includes("INSERT INTO omniroute.call_logs(")) {
              insertedId = String(values?.[0]);
              if (insertedId === rejectId) await tx.query("SELECT 1/0");
            }
            return tx.query<Row>(text, values);
          },
        })
      );
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
          assert.equal((await rows(id)).length, 0);
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
        "lost PUT ACK then positive GET and acknowledged DELETE retire only unowned identity",
        async () => {
          const id = prefix + "-lost",
            key = prefix + "-lost-key";
          await seed(key);
          fixture.putMode = "lost";
          await save(entry(id, key));
          fixture.putMode = "ack";
          assert.equal((await rows(id)).length, 0);
          const row = (await collect()).find(
            (item) =>
              item.publication_state === "delete_pending" && item.upload_state === "uncertain"
          )!;
          assert.ok(row);
          assert.deepEqual(await remove([row.relpath]), {
            deletedArtifacts: 1,
            pendingArtifactDeletions: 0,
            errors: 0,
          });
          assert.equal(
            (await collect()).some((item) => item.relpath === row.relpath),
            false
          );
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
            (item) =>
              item.publication_state === "delete_pending" && item.upload_state === "uncertain"
          )!;
          assert.ok(row);
          assert.equal(
            await runtime!.runPersistenceRequest(() =>
              runtime!.getPersistenceArtifactStore().isPutActive!(row.relpath)
            ),
            false
          );
          assert.deepEqual(await remove([row.relpath]), {
            deletedArtifacts: 0,
            pendingArtifactDeletions: 1,
            errors: 0,
          });
          assert.equal(
            (await collect()).some((item) => item.relpath === row.relpath),
            true
          );
          fixture.writeGate.resolve();
          await fixture.remoteEffect;
          fixture.writeGate = undefined;
          fixture.putMode = "ack";
          assert.deepEqual(await remove([row.relpath]), {
            deletedArtifacts: 1,
            pendingArtifactDeletions: 0,
            errors: 0,
          });
        }
      );
      await t.test(
        "acknowledged PUT plus actual SQL rejection retains cleanup ownership",
        async () => {
          const id = prefix + "-reject",
            key = prefix + "-reject-key";
          await seed(key);
          rejectId = id;
          await save(entry(id, key));
          rejectId = undefined;
          assert.equal((await rows(id)).length, 0);
          const row = (await collect()).find(
            (item) =>
              item.publication_state === "delete_pending" && item.upload_state === "acknowledged"
          )!;
          assert.ok(row);
          assert.equal((await remove([row.relpath])).deletedArtifacts, 1);
        }
      );
      await t.test(
        "lost actual COMMIT acknowledgment retains both current and legacy ownership fences",
        async () => {
          const id = prefix + "-commit",
            key = prefix + "-commit-key";
          await seed(key);
          loseCommitId = id;
          await save(entry(id, key));
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
            const ref = (await collect()).find(
              (item) => item.publication_state === "publishing"
            )!.relpath;
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
