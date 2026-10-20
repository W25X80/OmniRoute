import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { registerHooks } from "node:module";
import { fileURLToPath } from "node:url";
import { setImmediate as yieldToLoop, setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { encrypt } from "../../src/lib/db/encryption.ts";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { PostgresCallLogRepository } from "../../src/lib/db/postgres/callLogRepository.ts";
import { acquirePostgresLock } from "../../src/lib/db/postgres/locks.ts";
import {
  claimArtifactRecoveryDispatch,
  attachArtifactRecovery,
} from "../../src/lib/db/postgres/artifactRecoveryRepository.ts";
import {
  readStagedArtifact,
  projectStagedArtifact,
} from "../../src/lib/db/postgres/stagedArtifactRepository.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { makeManagementSessionRequest } from "../helpers/managementSession.ts";
import { observedWorker } from "../helpers/persistence/artifactObservedWorker.ts";

function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

test(
  "management staged details preserve native privacy, incarnation and publication authority",
  { skip: !postgresTestsEnabled, timeout: 90000 },
  async (t) => {
    const previous = new Map(
      [
        "OMNIROUTE_DATABASE_BACKEND",
        "STORAGE_ENCRYPTION_KEY",
        "APP_LOG_TO_FILE",
        "INITIAL_PASSWORD",
        "JWT_SECRET",
        "API_KEY_SECRET",
      ].map((name) => [name, process.env[name]])
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "73".repeat(32);
    process.env.APP_LOG_TO_FILE = "false";
    process.env.INITIAL_PASSWORD = "staged-management-fixture";
    const oldRuntime = Object.getOwnPropertyDescriptor(globalThis, "__omniroutePersistenceRuntime");
    const hooks = registerHooks({
      resolve(specifier, context, next) {
        if (
          context.parentURL?.endsWith("/usage/callLogArtifactWriter.ts") &&
          specifier === "node:worker_threads"
        )
          return {
            url: new URL("../helpers/persistence/artifactObservedWorker.ts", import.meta.url).href,
            shortCircuit: true,
          };
        return next(specifier, context);
      },
    });
    const database = await createTestPostgresDatabase();
    const monitor = await createTestPostgresDatabase();
    const plane = await initializePostgresControlPlane({
      connection: await testPostgresOptions(),
      runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      logger: { warn() {} },
    });
    const prefix = "staged-detail-" + randomUUID();
    let actor: typeof import("../../src/lib/usage/callLogArtifactWriter.ts") | undefined;
    const objects = new Map<string, Uint8Array>();
    let gets = 0;
    try {
      await plane.run(async (binding) => {
        const scope = "a".repeat(64);
        Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", {
          configurable: true,
          writable: true,
          value: {
            backend: "postgres",
            applicationIdentity: binding.databaseIdentity,
            phase: "ready",
            artifacts: {
              storageScope: scope,
              async get(reference: string) {
                gets++;
                return objects.get(reference) ?? null;
              },
              async put() {
                assert.fail("A detail read must not PUT");
              },
              async delete() {
                assert.fail("A detail read must not DELETE");
              },
            },
          },
        });
        const selected = binding.database!;
        const repository = new PostgresCallLogRepository(selected);
        actor = await import("../../src/lib/usage/callLogArtifactWriter.ts");
        const callLogs = await import("../../src/lib/usage/callLogs.ts");
        const service = await import("../../src/lib/usage/nativeStagedCallDetail.ts");
        const routes = [
          await import("../../src/app/api/logs/[id]/route.ts"),
          await import("../../src/app/api/usage/call-logs/[id]/route.ts"),
        ];
        await binding.settings.update({ requireLogin: true });
        let serial = 0;
        async function seed(keyless = false, id = `${prefix}-${serial++}`) {
          const keyId = keyless ? null : id + "-key";
          if (keyId)
            await database.transaction(async (tx) => {
              await acquirePostgresLock(tx, "keyPolicy");
              await tx.query(
                "INSERT INTO omniroute.api_keys(id,name,key,no_log,created_at) VALUES($1,'staged detail key',$2,0,$3)",
                [keyId, randomUUID(), new Date().toISOString()]
              );
            });
          const artifact = {
            schemaVersion: 5,
            summary: { id },
            requestBody: { private: id },
            responseBody: { retained: true },
            error: null,
            pipeline: { clientRequest: { model: "fixture" } },
          };
          const bytes = Buffer.from(JSON.stringify(artifact));
          const checksum = createHash("sha256").update(bytes).digest("hex");
          const intent = {
            artifact: {
              reference: `s3/${randomUUID()}_${checksum}.json`,
              checksum,
              sizeBytes: bytes.length,
              storageScope: scope,
              singleAttempt: true as const,
            },
            owner: randomUUID(),
            ciphertext: encrypt(bytes.toString("base64")),
          };
          assert.equal(
            (
              await repository.insert(
                {
                  id,
                  timestamp: new Date().toISOString(),
                  apiKeyId: keyId,
                  detailState: "missing",
                  detailReason: "publication_pending",
                },
                intent
              )
            ).intentAccepted,
            true
          );
          assert.equal(
            await claimArtifactRecoveryDispatch(
              selected,
              intent.artifact.reference,
              intent.owner,
              randomUUID()
            ),
            true
          );
          const row = await repository.byId(id);
          return { id, keyId, artifact, bytes, intent, callOrder: Number(row!.insertion_order) };
        }
        async function queue(reference: string) {
          return database.transaction(
            async (tx) =>
              (
                await tx.query("SELECT * FROM omniroute.artifact_deletion_queue WHERE relpath=$1", [
                  reference,
                ])
              ).rows[0]
          );
        }
        async function gated(id: string, mutate: () => Promise<void>, signal?: AbortSignal) {
          await observedWorker.last?.terminate();
          await yieldToLoop();
          actor!.__setCallLogWorkerOverrideForTests({
            workerFile: fileURLToPath(
              new URL("../helpers/persistence/stagedArtifactGateWorker.ts", import.meta.url)
            ),
            execArgv: ["--import", "tsx/esm"],
          });
          const entered = barrier();
          observedWorker.entered = entered.release;
          const reading = callLogs.getManagementCallLogById(id, signal);
          await Promise.race([
            entered.promise,
            reading.then(() => {
              assert.fail("Staged decode must reach the worker barrier");
            }),
          ]);
          await mutate();
          observedWorker.last!.postMessage({ release: true });
          try {
            return await reading;
          } finally {
            observedWorker.entered = undefined;
          }
        }

        await t.test(
          "both actual handlers enforce management auth and recover staging without mutation or S3 I/O",
          async () => {
            const item = await seed();
            const before = await queue(item.intent.artifact.reference);
            for (const route of routes) {
              const denied = await route.GET(new Request("http://localhost/api/logs/" + item.id), {
                params: Promise.resolve({ id: item.id }),
              });
              assert.equal(denied.status, 401);
              const req = await makeManagementSessionRequest(
                "http://localhost/api/logs/" + item.id
              );
              const response = await route.GET(req, { params: Promise.resolve({ id: item.id }) });
              assert.equal(response.status, 200);
              const detail = await response.json();
              assert.deepEqual(detail.requestBody, item.artifact.requestBody);
              assert.deepEqual(detail.pipelinePayloads, item.artifact.pipeline);
              assert.equal(detail.detailSource, "staging");
              assert.equal(detail.artifactPublicationState, "uncertain");
              assert.equal(detail.detailState, "missing");
            }
            assert.deepEqual(await queue(item.intent.artifact.reference), before);
            assert.equal(gets, 0);
            assert.equal((await callLogs.getCallLogById(item.id))!.requestBody, null);
            assert.equal(
              (await callLogs.getManagementCallLogById((await seed(true)).id))!.detailSource,
              "staging"
            );
          }
        );

        await t.test("an ordinary attached object still uses canonical S3 detail", async () => {
          const item = await seed();
          objects.set(item.intent.artifact.reference, item.bytes);
          assert.equal(
            await attachArtifactRecovery(
              selected,
              item.intent.artifact.reference,
              item.intent.owner
            ),
            true
          );
          const detail = await callLogs.getManagementCallLogById(item.id);
          assert.equal(detail!.detailState, "ready");
          assert.deepEqual(detail!.requestBody, item.artifact.requestBody);
          assert.equal(gets, 1);
        });

        await t.test(
          "each original owner operand is necessary at initial and final reads",
          async () => {
            const item = await seed();
            const owner = { callId: item.id, callOrder: item.callOrder, storageScope: scope };
            const snapshot = await readStagedArtifact(selected, owner);
            assert.ok(snapshot);
            for (const wrong of [
              { ...owner, callOrder: owner.callOrder + 1 },
              { ...owner, storageScope: "b".repeat(64) },
              { ...owner, callId: "missing" },
            ])
              assert.equal(await readStagedArtifact(selected, wrong), null);
            for (const wrong of [
              { ...snapshot, owner: randomUUID() },
              { ...snapshot, sha256: "f".repeat(64) },
              { ...snapshot, sizeBytes: snapshot.sizeBytes + 1 },
              { ...snapshot, apiKeyId: null },
              { ...snapshot, reference: "missing" },
            ])
              assert.equal(
                await projectStagedArtifact(selected, wrong, () =>
                  assert.fail("Invalid owner projected")
                ),
                null
              );
          }
        );

        await t.test(
          "no_log, key deletion, call deletion and attachment during real decoding discard stale content",
          async () => {
            for (const action of ["no_log", "key_delete", "call_delete", "attach"] as const) {
              const item = await seed();
              const detail = await gated(item.id, async () => {
                if (action === "attach") {
                  assert.equal(
                    await attachArtifactRecovery(
                      selected,
                      item.intent.artifact.reference,
                      item.intent.owner
                    ),
                    true
                  );
                } else
                  await database.transaction(async (tx) => {
                    await acquirePostgresLock(tx, "keyPolicy");
                    await acquirePostgresLock(tx, "accounting");
                    if (action === "no_log")
                      await tx.query("UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1", [
                        item.keyId,
                      ]);
                    else if (action === "key_delete")
                      await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [item.keyId]);
                    else await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [item.id]);
                  });
              });
              assert.equal(detail?.requestBody ?? null, null);
              if (action === "attach")
                assert.equal(
                  (await repository.byId(item.id))!.artifact_relpath,
                  item.intent.artifact.reference
                );
            }
          }
        );

        await t.test(
          "id reuse and authority replacement cannot admit the old decoded payload",
          async () => {
            const item = await seed();
            const result = await gated(item.id, async () => {
              await database.transaction(async (tx) => {
                await acquirePostgresLock(tx, "accounting");
                await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [item.id]);
              });
              await seed(true, item.id);
            });
            assert.equal(result?.requestBody ?? null, null);
            const other = await seed();
            const state = globalThis.__omniroutePersistenceRuntime!;
            const identity = state.applicationIdentity;
            try {
              assert.equal(
                (
                  await gated(other.id, async () => {
                    state.applicationIdentity = {};
                  })
                )?.requestBody ?? null,
                null
              );
            } finally {
              state.applicationIdentity = identity;
            }
          }
        );

        await t.test(
          "cancelled reads retain both real admissions until blocked SQL settles",
          async () => {
            await observedWorker.last?.terminate();
            await yieldToLoop();
            actor!.__setCallLogWorkerOverrideForTests(null);
            const item = await seed();
            const held = barrier(),
              release = barrier();
            const holder = database.transaction(async (tx) => {
              await acquirePostgresLock(tx, "keyPolicy");
              held.release();
              await release.promise;
            });
            await held.promise;
            const controller = new AbortController();
            const authority = service.captureStagedDetailAuthority()!;
            const first = service.readNativeStagedCallDetail(
              authority,
              item.id,
              item.callOrder,
              controller.signal
            );
            const second = service.readNativeStagedCallDetail(authority, item.id, item.callOrder);
            try {
              controller.abort();
              assert.equal(await first, null);
              const third = service.readNativeStagedCallDetail(authority, item.id, item.callOrder);
              assert.equal(
                await Promise.race([
                  third,
                  delay(100).then(() => assert.fail("Third read was admitted")),
                ]),
                null
              );
            } finally {
              release.release();
              await holder;
            }
            assert.equal((await second)?.detailSource, "staging");
          }
        );

        await t.test(
          "a real key-policy writer blocks final projection and fresh policy wins",
          async () => {
            const item = await seed();
            const snapshot = await readStagedArtifact(selected, {
              callId: item.id,
              callOrder: item.callOrder,
              storageScope: scope,
            });
            assert.ok(snapshot);
            const held = barrier(),
              release = barrier();
            let holderPid = 0;
            const writer = database.transaction(async (tx) => {
              holderPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
                .rows[0].pid;
              await acquirePostgresLock(tx, "keyPolicy");
              held.release();
              await release.promise;
              await tx.query("UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1", [item.keyId]);
            });
            await held.promise;
            const projection = projectStagedArtifact(selected, snapshot, () =>
              assert.fail("Revoked payload projected")
            );
            try {
              let waiting = false;
              for (let i = 0; i < 30 && !waiting; i++) {
                waiting = await monitor.transaction(
                  async (tx) =>
                    (
                      await tx.query<{ waiting: boolean }>(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid))) AS waiting",
                        [holderPid]
                      )
                    ).rows[0].waiting
                );
                if (!waiting) await delay(10);
              }
              assert.equal(waiting, true, "Projection must actually wait on the owner lock");
            } finally {
              release.release();
              await writer;
            }
            assert.equal(await projection, null);
          }
        );

        await t.test(
          "SQL queue composition distinguishes publication from retirement",
          async () => {
            const counts =
              await createPostgresCleanupRepository(selected).artifactReconciliationCounts(scope);
            assert.ok(counts.publishing > 0 && counts.deletePending > 0 && counts.uncertain > 0);
            assert.equal(counts.pending, counts.publishing + counts.deletePending);
            assert.ok(counts.oldestUncertainCreatedAt);
          }
        );
      });
    } finally {
      observedWorker.entered = undefined;
      await actor?.closeCallLogArtifactWriter();
      await plane.close();
      await database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [prefix + "%"]);
        await tx.query("DELETE FROM omniroute.artifact_deletion_queue WHERE call_log_id LIKE $1", [
          prefix + "%",
        ]);
        await tx.query("DELETE FROM omniroute.api_keys WHERE id LIKE $1", [prefix + "%"]);
      });
      await database.close();
      await monitor.close();
      hooks.deregister();
      if (oldRuntime)
        Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", oldRuntime);
      else delete globalThis.__omniroutePersistenceRuntime;
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
