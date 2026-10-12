import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { artifactAuthorityFixture } from "../helpers/persistence/artifactAuthorityFixture.ts";
import { nativeRuntimeFixture } from "../helpers/persistence/runtimeNativeIntegrationFixture.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

test(
  "the real native application store is bound to its held opaque database authority",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const bootstrapFixture = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const artifactFixture = new URL(
      "../helpers/persistence/artifactAuthorityFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (context.parentURL === runtimeUrl) {
          if (specifier === "../artifacts/s3ArtifactStore")
            return { url: artifactFixture, shortCircuit: true };
          if (
            [
              "./postgres/environment",
              "./postgres/controlPlane",
              "../artifacts/s3ArtifactConfiguration",
            ].includes(specifier)
          )
            return { url: bootstrapFixture, shortCircuit: true };
        }
        return nextResolve(specifier, context);
      },
    });
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "NODE_ENV",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "4d".repeat(32);
    process.env.NODE_ENV = "test";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const database = await createTestPostgresDatabase();
    const prefix = `artifact-authority-${randomUUID()}`;
    const references = new Set<string>();
    let foreign: PostgresControlPlane | undefined;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let writer: typeof import("../../src/lib/usage/callLogArtifactWriter.ts") | undefined;
    artifactAuthorityFixture.reset();
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      assert.ok(state.applicationIdentity);
      const owner = await import("../../src/lib/usage/callLogArtifacts.ts");
      writer = await import("../../src/lib/usage/callLogArtifactWriter.ts");
      const { UnsupportedPersistenceDomainError } =
        await import("../../src/lib/db/controlPlaneContext.ts");
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      foreign = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        runtime: {
          applyRuntimeSettings() {},
          scheduleCatalogRevalidationAfterInit() {},
        },
        logger: { warn() {} },
      });
      const snapshot = () =>
        database.transaction(async (tx) => ({
          logs: (
            await tx.query<{ data: string }>(
              "SELECT row_to_json(log)::text AS data FROM omniroute.call_logs AS log ORDER BY id"
            )
          ).rows,
          queue: (
            await tx.query<{ data: string }>(
              "SELECT row_to_json(item)::text AS data FROM omniroute.artifact_deletion_queue AS item ORDER BY relpath"
            )
          ).rows,
        }));
      const store = runtime!.getPersistenceArtifactStore();
      const { createNativeArtifactPublication } =
        await import("../../src/lib/usage/nativeArtifactPublication.ts");
      const writeUnattachedArtifact = async (bytes: Uint8Array) => {
        const publication = createNativeArtifactPublication();
        try {
          const stored = await owner.writeCallArtifactToNativeStore(bytes, publication.options);
          publication.acknowledge({
            reference: stored.relPath,
            sizeBytes: stored.sizeBytes,
            checksum: stored.sha256,
          });
          // This binary authority fixture has no log row; retain acknowledged deletion ownership.
          await publication.fail(new Error("Unattached authority fixture output"));
          return stored;
        } catch (error) {
          await publication.fail(error);
          throw error;
        } finally {
          publication.release();
        }
      };
      const object = await writeUnattachedArtifact(Buffer.from('{"keep":"application"}'));
      references.add(object.relPath);

      await t.test(
        "a foreign plane with free app capacity cannot GET, PUT, DELETE or mutate objects/SQL",
        async () => {
          const before = artifactAuthorityFixture.effects();
          const objects = [...artifactAuthorityFixture.objects];
          const rows = await snapshot();
          assert.equal(state.background.size, 0);
          // Distinct opaque authorities intentionally use the SAME disposable physical database.
          // No CREATE DATABASE privilege is required, and no SQL write is requested in this case.
          await foreign!.run(async (binding) => {
            assert.notEqual(binding.databaseIdentity, state.applicationIdentity);
            assert.throws(runtime!.getPersistenceArtifactStore, UnsupportedPersistenceDomainError);
            await assert.rejects(
              owner.readCallArtifactAsync(object.relPath),
              UnsupportedPersistenceDomainError
            );
            await assert.rejects(
              owner.deleteCallArtifactAsync(object.relPath),
              UnsupportedPersistenceDomainError
            );
            await assert.rejects(
              owner.writeCallArtifactToNativeStore(Buffer.from("must not upload"), {
                async onPrepared() {
                  assert.fail("Foreign authority must preempt preparation");
                },
              }),
              UnsupportedPersistenceDomainError
            );
            await assert.rejects(
              writer!.writeCallArtifactAsync(
                {} as Parameters<
                  typeof import("../../src/lib/usage/callLogArtifactWriter.ts").writeCallArtifactAsync
                >[0]
              ),
              UnsupportedPersistenceDomainError
            );
          });
          assert.deepEqual(artifactAuthorityFixture.effects(), before);
          assert.deepEqual([...artifactAuthorityFixture.objects], objects);
          assert.deepEqual(await snapshot(), rows);
          assert.equal(state.background.size, 0);
        }
      );

      await t.test(
        "manual foreign retention can commit its SQL deletion and durable queue without app S3 access",
        async () => {
          const id = `${prefix}-retention`;
          const before = artifactAuthorityFixture.effects();
          const objects = [...artifactAuthorityFixture.objects];
          const existing = (await snapshot()).logs;
          await database.transaction(async (tx) => {
            await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
            await tx.query(
              "INSERT INTO omniroute.call_logs(id,timestamp,detail_state,artifact_relpath) VALUES($1,'0001-01-01T00:00:00.000Z','ready',$2)",
              [id, object.relPath]
            );
          });
          const { deleteCallLogsBeforeAsync } =
            await import("../../src/lib/usage/callLogRotation.ts");
          await foreign!.run(async () => {
            await assert.rejects(
              deleteCallLogsBeforeAsync("0002-01-01T00:00:00.000Z", 1),
              UnsupportedPersistenceDomainError
            );
          });
          const after = await snapshot();
          assert.deepEqual(
            after.logs,
            existing,
            "only the explicitly selected seed row is deleted"
          );
          assert.ok(
            after.queue.some((row) => JSON.parse(row.data).relpath === object.relPath),
            "the denied S3 deletion must retain a durable recovery reference"
          );
          assert.deepEqual(artifactAuthorityFixture.effects(), before);
          assert.deepEqual([...artifactAuthorityFixture.objects], objects);
          // The foreign SQL delete is intentional. This case does NOT claim physical DB immutability.
        }
      );

      await t.test(
        "fresh request/background/shutdown scopes preserve the actual application store",
        async () => {
          await runtime!.runPersistenceRequest(async () => {
            assert.equal(runtime!.getPersistenceArtifactStore(), store);
            assert.deepEqual(await owner.readCallArtifactAsync(object.relPath), {
              artifact: { keep: "application" },
              state: "ready",
            });
          });
          await runtime!.runPersistenceBackgroundWork(async () => {
            assert.equal(runtime!.getPersistenceArtifactStore(), store);
            const temporary = await writeUnattachedArtifact(Buffer.from("{}"));
            references.add(temporary.relPath);
            assert.equal(await owner.deleteCallArtifactAsync(temporary.relPath), true);
          });
          await runtime!.runPersistenceShutdownWork(async () => {
            assert.equal(runtime!.getPersistenceArtifactStore(), store);
            assert.equal(await owner.deleteCallArtifactAsync(object.relPath), true);
          });
        }
      );

      await t.test("an ended real SQL scope cannot substitute the held app authority", async () => {
        let resource: AsyncResource | undefined;
        await runtime!.runPersistenceRequest(async () => {
          resource = new AsyncResource("native-artifact-ended-scope");
          assert.equal(runtime!.getPersistenceArtifactStore(), store);
        });
        const before = artifactAuthorityFixture.effects();
        try {
          assert.throws(
            () => resource!.runInAsyncScope(runtime!.getPersistenceArtifactStore),
            /scope has ended/
          );
          assert.deepEqual(artifactAuthorityFixture.effects(), before);
        } finally {
          resource?.emitDestroy();
        }
      });
    } finally {
      await writer?.closeCallLogArtifactWriter().catch(() => {});
      await foreign?.close();
      await runtime?.closePersistenceRuntime();
      await database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [`${prefix}%`]);
        await tx.query(
          "DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=ANY($1::text[])",
          [[...references]]
        );
      });
      await database.close();
      assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
      delete globalThis.__omniroutePersistenceRuntime;
      nativeRuntimeFixture.plane = undefined;
      hooks.deregister();
      artifactAuthorityFixture.reset();
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
