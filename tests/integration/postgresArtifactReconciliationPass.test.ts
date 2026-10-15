import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";
import { artifactAuthorityFixture } from "../helpers/persistence/artifactAuthorityFixture.ts";

test(
  "one native pass isolates corrupt objects, serves both queues and preserves uncertain/foreign authority",
  {
    skip: !postgresTestsEnabled,
    timeout: 60000,
  },
  async () => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, next) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return {
            url: new URL(
              specifier === "../artifacts/s3ArtifactStore"
                ? "../helpers/persistence/artifactAuthorityFixture.ts"
                : "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
              import.meta.url
            ).href,
            shortCircuit: true,
          };
        return next(specifier, context);
      },
    });
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    artifactAuthorityFixture.reset();
    const runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") = await import(
      runtimeUrl
    );
    try {
      await runtime.ensurePersistenceReady();
      await runtime.runPersistenceRequest(async () => {
        const { getSelectedPostgresDatabase } =
          await import("../../src/lib/db/controlPlaneContext.ts");
        const { PostgresCallLogRepository } =
          await import("../../src/lib/db/postgres/callLogRepository.ts");
        const { prepareNativeArtifactRecovery, runNativeArtifactReconciliationPass } =
          await import("../../src/lib/usage/nativeArtifactRecovery.ts");
        const { getArtifactReconciliationStatus } =
          await import("../../src/lib/usage/artifactReconciliationStatus.ts");
        const database = getSelectedPostgresDatabase()!,
          store = runtime.getPersistenceArtifactStore();
        const intent = prepareNativeArtifactRecovery(Buffer.from("publish-neighbour"));
        const poisoned = prepareNativeArtifactRecovery(Buffer.from("poisoned-staging"));
        await new PostgresCallLogRepository(database).insert(
          {
            id: randomUUID(),
            timestamp: new Date().toISOString(),
            detailState: "missing",
            detailReason: "publication_pending",
            videoContentRemoved: 0,
          },
          poisoned
        );
        await database.transaction((tx) =>
          tx.query(
            "UPDATE omniroute.artifact_deletion_queue SET staged_payload='enc:v1:malformed',staged_bytes=octet_length('enc:v1:malformed'),next_attempt_at='0001-01-01'::timestamptz WHERE relpath=$1",
            [poisoned.artifact.reference]
          )
        );
        assert.equal(
          (
            await new PostgresCallLogRepository(database).insert(
              {
                id: randomUUID(),
                timestamp: new Date().toISOString(),
                detailState: "missing",
                detailReason: "publication_pending",
                videoContentRemoved: 0,
              },
              intent
            )
          ).intentAccepted,
          true
        );
        async function queued(state: string, scope: string | null = store.storageScope!) {
          const prepared = store.prepare!(Buffer.from("delete-neighbour"));
          await database.transaction((tx) =>
            tx.query(
              `INSERT INTO omniroute.artifact_deletion_queue(relpath,publication_state,upload_state,store_scope,expected_size_bytes,expected_sha256,next_attempt_at)
           VALUES($1,'delete_pending',$2,$3,$4,$5,'0001-01-01'::timestamptz)`,
              [prepared.reference, state, scope, prepared.sizeBytes, prepared.checksum]
            )
          );
          return prepared.reference;
        }
        const corrupt = await queued("uncertain"),
          good = await queued("acknowledged"),
          missing = await queued("uncertain");
        const foreign = await queued("acknowledged", "b".repeat(64));
        const legacy = await queued("legacy_unknown", null);
        artifactAuthorityFixture.objects.set("gateway/call-logs/" + corrupt, {
          bytes: Buffer.from("corrupt"),
          checksum: Buffer.from("corrupt").toString("base64"),
        });
        await runNativeArtifactReconciliationPass(true);
        const remaining = (
          await database.transaction((tx) =>
            tx.query<{ relpath: string; publication_state: string }>(
              "SELECT relpath,publication_state FROM omniroute.artifact_deletion_queue"
            )
          )
        ).rows;
        assert.equal(
          remaining.some((row) => row.relpath === good),
          false
        );
        assert.equal(
          remaining.find((row) => row.relpath === intent.artifact.reference)?.publication_state,
          "attached"
        );
        for (const ref of [corrupt, missing, foreign, legacy])
          assert.ok(remaining.some((row) => row.relpath === ref));
        assert.equal(artifactAuthorityFixture.deletes, 1);
        assert.equal(getArtifactReconciliationStatus().state, "degraded");
        assert.equal(getArtifactReconciliationStatus().foreignScope, 1);
        assert.equal(getArtifactReconciliationStatus().legacy, 1);
        // A common IAM/endpoint failure stops later network work rather than treating it as absence.
        artifactAuthorityFixture.getFailure = Object.assign(
          new Error("private diagnostic must not escape"),
          { name: "AccessDenied", $metadata: { httpStatusCode: 403, requestId: "safe-request-id" } }
        );
        await queued("uncertain");
        const gets = artifactAuthorityFixture.gets;
        await assert.rejects(runNativeArtifactReconciliationPass(true));
        assert.equal(artifactAuthorityFixture.gets, gets + 1);
        const status = getArtifactReconciliationStatus();
        assert.equal(status.state, "degraded");
        assert.equal(status.lastFailure?.status, 403);
        assert.equal(JSON.stringify(status).includes("private diagnostic"), false);
      });
    } finally {
      artifactAuthorityFixture.getFailure = undefined;
      await runtime.closePersistenceRuntime();
      hooks.deregister();
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
