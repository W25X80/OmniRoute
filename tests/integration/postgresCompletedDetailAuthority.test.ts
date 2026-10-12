import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { streamingNativeFixture } from "../helpers/persistence/streamingNativeIntegrationFixture.ts";
import { completedDetailNativeFixture } from "../helpers/persistence/completedDetailNativeIntegrationFixture.ts";
import { closeNativeStreamingFixture } from "../helpers/persistence/nativeStreamingFixtureShutdown.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import type { PersistenceTailPermit } from "../../src/lib/db/persistenceRuntime.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import type { PendingRequestDetail } from "../../src/lib/usage/usageHistory.ts";
import { CALL_LOG_BODY_OMITTED_FOR_SIZE_LIMIT } from "../../src/shared/constants/callLogSizeLimitMarkers.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function deadline<T>(work: Promise<T>, milliseconds = 15000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Completed-detail PostgreSQL oracle timed out")),
          milliseconds
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
async function eventually(predicate: () => Promise<boolean> | boolean) {
  const until = Date.now() + 6000;
  while (!(await predicate())) {
    if (Date.now() >= until) throw new Error("Completed-detail SQL wait was not witnessed");
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

// Real PostgreSQL engine/authority and real artifact serialization/reader. The in-memory carrier
// explicitly excludes AWS/S3 and this test makes no installed Next HTTP acceptance claim.
test(
  "completed-detail enrichment survives old-tail release, respects source payload rules, and joins shutdown",
  {
    skip: !postgresTestsEnabled,
    timeout: 45000,
  },
  async (t) => {
    const tag = randomUUID();
    const model = `completed-detail-${tag}`;
    const directory = mkdtempSync(join(tmpdir(), "omni-completed-detail-native-"));
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "OMNIROUTE_BUILDING",
      "NEXT_PHASE",
      "APP_LOG_TO_FILE",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.APP_LOG_TO_FILE = "false";
    delete process.env.OMNIROUTE_BUILDING;
    delete process.env.NEXT_PHASE;
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const contextUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/controlPlaneContext.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/completedDetailNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const probeUrl = new URL("../helpers/persistence/streamingSqliteGuardProbe.ts", import.meta.url)
      .href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        const resolved = nextResolve(specifier, context);
        if (resolved.url === contextUrl && context.parentURL !== probeUrl)
          return { url: probeUrl, shortCircuit: true };
        return resolved;
      },
    });
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let verification: PostgresDatabase | undefined;
    let locker: PostgresDatabase | undefined;
    let permit: PersistenceTailPermit | undefined;
    let held: Promise<void> | undefined;
    let closing: Promise<void> | undefined;
    const locked = barrier(),
      release = barrier();
    const ids = ["delayed", "evicted", "omitted", "absent", "no-log"].map(
      (name) => `${name}-${tag}`
    );
    const warnings: unknown[][] = [];
    t.mock.method(console, "warn", (...args: unknown[]) => {
      warnings.push(args);
    });
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      verification = await createTestPostgresDatabase();
      locker = await createTestPostgresDatabase();
      const context: typeof import("../../src/lib/db/controlPlaneContext.ts") = await import(
        contextUrl
      );
      const completed = await import("../../src/lib/usage/completedRequestDetails.ts");
      const { writeCallArtifactAsync, closeCallLogArtifactWriter } =
        await import("../../src/lib/usage/callLogArtifactWriter.ts");
      const { readCallArtifactAsync } = await import("../../src/lib/usage/callLogArtifacts.ts");
      const { getSelectedCallLogRepository } =
        await import("../../src/lib/db/postgres/callLogRepository.ts");
      const { saveCallLog } = await import("../../src/lib/usage/callLogs.ts");
      const { createNativeArtifactPublication } =
        await import("../../src/lib/usage/nativeArtifactPublication.ts");
      const previews: PendingRequestDetail[] = ids.map((id) => ({
        id,
        provider: "openai",
        model,
        connectionId: id,
        startedAt: Date.now(),
        providerResponse: null,
        clientResponse: null,
        tokens: {
          in: 1,
          out: 1,
          cacheRead: null,
          cacheCreation: null,
          reasoning: null,
          compressed: null,
        },
      }));
      const references: string[] = [];
      await runtime.runPersistenceRequest(async () => {
        const repository = getSelectedCallLogRepository()!;
        for (const [index, id] of ids.entries()) {
          if (index === 4) {
            const before = streamingNativeFixture.artifactPuts;
            // Source explicit NoLog suppresses payload creation. This DTO does not carry a key:
            // the test does not claim retroactive current-principal authorization of older artifacts.
            await saveCallLog({
              id,
              noLog: true,
              model,
              provider: "openai",
              connectionId: id,
              status: 200,
              requestBody: { sensitive: true },
              responseBody: { sensitive: true },
              pipelinePayloads: {
                providerResponse: { sensitive: true },
                clientResponse: { sensitive: true },
              },
            });
            assert.equal(streamingNativeFixture.artifactPuts, before);
            continue;
          }
          let artifact: Awaited<ReturnType<typeof writeCallArtifactAsync>> = null;
          const publication = index < 3 ? createNativeArtifactPublication() : undefined;
          try {
            if (index < 3) {
              artifact = await writeCallArtifactAsync(
                {
                  schemaVersion: 5,
                  summary: {
                    id,
                    timestamp: new Date().toISOString(),
                    method: "POST",
                    path: "/v1/chat/completions",
                    status: 200,
                    model,
                    requestedModel: null,
                    provider: "openai",
                    account: "fixture",
                    connectionId: id,
                    duration: 1,
                    tokens: {
                      in: 1,
                      out: 1,
                      cacheRead: null,
                      cacheWrite: null,
                      reasoning: null,
                      compressed: null,
                    },
                    requestType: null,
                    sourceFormat: null,
                    targetFormat: null,
                    apiKeyId: null,
                    apiKeyName: null,
                    comboName: null,
                    comboStepId: null,
                    comboExecutionKey: null,
                  },
                  requestBody: null,
                  responseBody:
                    index === 2 ? CALL_LOG_BODY_OMITTED_FOR_SIZE_LIMIT : { side: "fallback" },
                  error: null,
                  ...(index === 0
                    ? {
                        pipeline: {
                          providerResponse: { side: "provider" },
                          clientResponse: { side: "client" },
                        },
                      }
                    : {}),
                },
                publication!.options
              );
              assert.ok(artifact);
              publication!.acknowledge({
                reference: artifact.relPath,
                sizeBytes: artifact.sizeBytes,
                checksum: artifact.sha256,
              });
              references.push(artifact!.relPath);
            }
            await repository.insert({
              id,
              timestamp: new Date().toISOString(),
              model,
              provider: "openai",
              method: "POST",
              path: "/v1/chat/completions",
              status: 200,
              connectionId: id,
              videoContentRemoved: 0,
              detailState: artifact ? "ready" : "none",
              artifactRelPath: artifact?.relPath ?? null,
              artifactSizeBytes: artifact?.sizeBytes ?? null,
              artifactSha256: artifact?.sha256 ?? null,
              artifactPublicationOwner: publication?.owner,
            });
          } catch (error) {
            await publication?.fail(error);
            throw error;
          } finally {
            publication?.release();
          }
        }
      });
      await runtime.drainPersistenceAcceptedWork();
      // Foreign explicit planes never borrow the application's artifact store, even when it is ready.
      const foreign = {
        databaseIdentity: Object.freeze({}),
        assertReady() {},
      } as unknown as PostgresControlPlaneBinding;
      const readsBefore = completedDetailNativeFixture.reads;
      await assert.rejects(
        context.withPostgresControlPlane(foreign, () => readCallArtifactAsync(references[0])),
        /not been ported/
      );
      assert.equal(completedDetailNativeFixture.reads, readsBefore);
      const suppressed = await verification.transaction((tx) =>
        tx.query<{ artifact_relpath: string | null; detail_state: string }>(
          "SELECT artifact_relpath,detail_state FROM omniroute.call_logs WHERE id=$1",
          [ids[4]]
        )
      );
      assert.equal(suppressed.rows[0]?.artifact_relpath, null);
      assert.equal(suppressed.rows[0]?.detail_state, "none");

      held = locker.transaction(async (tx) => {
        await tx.query("LOCK TABLE omniroute.call_logs IN ACCESS EXCLUSIVE MODE");
        locked.resolve();
        await release.promise;
      });
      void held.catch(() => {});
      await deadline(locked.promise);
      permit = await runtime.runPersistenceRequest(() => runtime!.acquirePersistenceTail());
      let oldBinding: PostgresControlPlaneBinding | undefined;
      await permit.run(async () => {
        oldBinding = context.getSelectedControlPlane();
        for (const preview of previews) {
          completed.storeCompletedDetail(preview);
          assert.equal(completed.maybeEnrichCompletedDetail(preview, preview.id), undefined);
        }
      });
      await eventually(async () => {
        const waiting = await verification!.transaction((tx) =>
          tx.query<{ count: string }>(
            "SELECT count(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT artifact_relpath FROM omniroute.call_logs WHERE connection_id=%'"
          )
        );
        return Number(waiting.rows[0]?.count) >= previews.length;
      });
      assert.ok(runtime.getPersistenceRuntimeStatus().backgroundOperations >= previews.length);
      await permit.release();
      assert.throws(() => oldBinding!.assertReady(), /scope has ended/);
      const newestTokens = { ...previews[0].tokens!, in: 321, out: 654 };
      completed.storeCompletedDetail({ ...previews[0], tokens: newestTokens });
      assert.equal(
        completed.storeCompletedDetail({
          ...previews[1],
          clientRequest: "x".repeat(completed.MAX_COMPLETED_DETAILS_BYTES + 1),
        }),
        false
      );
      assert.equal(completed.getCompletedDetails().has(previews[1].id), false);
      closing = closeNativeStreamingFixture(runtime);
      void closing.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      assert.equal(streamingNativeFixture.artifactsClosed, 0);
      completed.maybeEnrichCompletedDetail(
        { ...previews[0], id: `refused-${tag}` },
        previews[0].id
      );
      await eventually(() =>
        warnings.some((args) => String(args[1]).includes("admission is unavailable"))
      );
      assert.equal(streamingNativeFixture.artifactsClosed, 0);
      release.resolve();
      await deadline(held);
      await deadline(closing);
      const stored = completed.getCompletedDetails().get(previews[0].id)!;
      assert.deepEqual(stored.providerResponse, { side: "provider" });
      assert.deepEqual(stored.clientResponse, { side: "client" });
      assert.deepEqual(stored.tokens, newestTokens);
      assert.equal(completed.getCompletedDetails().has(previews[1].id), false);
      for (const preview of previews.slice(2)) {
        assert.equal(preview.providerResponse, null);
        assert.equal(preview.clientResponse, null);
      }
      assert.equal(completedDetailNativeFixture.reads - readsBefore, 3);
      assert.equal(
        streamingNativeFixture.sqliteAttempts,
        0,
        streamingNativeFixture.sqliteAttemptStacks.join("\n")
      );
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      assert.equal(streamingNativeFixture.artifactsClosed, 1);
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
      assert.ok(completed.getCompletedDetailsByteSize() <= completed.MAX_COMPLETED_DETAILS_BYTES);
      assert.equal(
        warnings.filter((args) => String(args[0]).startsWith("[usageHistory]")).length,
        1
      );
      completed.clearCompletedDetails();
      await closeCallLogArtifactWriter(0);
    } finally {
      release.resolve();
      await held?.catch(() => {});
      await permit?.release().catch(() => {});
      await closing?.catch(() => {});
      await closeNativeStreamingFixture(runtime).catch(() => {});
      try {
        if (verification)
          await verification.transaction((tx) =>
            tx.query("DELETE FROM omniroute.call_logs WHERE id=ANY($1::text[])", [ids])
          );
      } finally {
        await verification?.close();
        await locker?.close();
        hooks.deregister();
        delete globalThis.__omniroutePersistenceRuntime;
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
        rmSync(directory, { recursive: true, force: true });
      }
    }
  }
);
