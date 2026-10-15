import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { mkdtempSync, realpathSync, rmSync, existsSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  barrier,
  callLogNativeFixture,
  nativeRuntimeFixture,
} from "../helpers/persistence/callLogNativeRuntimeFixture.ts";

test(
  "ignored application call logs own their native I/O until acknowledged or refused",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/callLogNativeRuntimeFixture.ts",
      import.meta.url
    ).href;
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
        return nextResolve(specifier, context);
      },
    });
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "NODE_ENV",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "omniroute-native-owned-call-log-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const prefix = `owned-call-log-${randomUUID()}`;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    const database = await createTestPostgresDatabase();
    let foreign:
      | Awaited<
          ReturnType<
            typeof import("../../src/lib/db/postgres/controlPlane.ts").initializePostgresControlPlane
          >
        >
      | undefined;
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      assert.ok(state.applicationIdentity);
      const callLogs = await import("../../src/lib/usage/callLogs.ts");
      const { runWithCallLogApiKeyContext } =
        await import("../../src/lib/usage/callLogApiKeyContext.ts");
      const actions = await import("../../src/lib/usage/resilienceActionsContext.ts");
      const seedKey = async (id: string) =>
        database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
          await tx.query(
            "INSERT INTO omniroute.api_keys(id,name,key,no_log,created_at) VALUES($1,'owned log key',$2,0,$3)",
            [id, randomUUID(), new Date().toISOString()]
          );
        });
      const rows = async (id: string) =>
        (
          await database.transaction((tx) =>
            tx.query<{
              id: string;
              api_key_id: string;
              api_key_name: string;
              resilience_actions: string;
              detail_state: string;
              artifact_relpath: string;
            }>(
              "SELECT id,api_key_id,api_key_name,resilience_actions,detail_state,artifact_relpath FROM omniroute.call_logs WHERE id=$1",
              [id]
            )
          )
        ).rows;

      await t.test(
        "an ignored save commits after its parent scope ends, with captured attribution/actions",
        async () => {
          const id = `${prefix}-ignored`,
            key = `${prefix}-key`;
          await seedKey(key);
          const gate = callLogNativeFixture.arm(),
            settled = barrier();
          let observer: AsyncResource | undefined;
          const entry = { id, status: 200, requestBody: { marker: "captured artifact" } };
          try {
            await runtime!.runPersistenceRequest(async () =>
              runWithCallLogApiKeyContext({ apiKeyId: key, apiKeyName: "captured name" }, () =>
                actions.runWithResilienceActionsContext(() => {
                  observer = new AsyncResource("call-log-resilience-observer");
                  actions.noteResilienceAction({ rotations: 1 });
                  void callLogs.saveCallLog(entry).then(settled.resolve);
                  assert.equal(
                    state.background.size,
                    1,
                    "admission is registered before save returns"
                  );
                  actions.noteResilienceAction({ continued: 1 });
                  entry.id = `${id}-mutated`;
                })
              )
            );
            await Promise.race([
              gate.entered,
              settled.promise.then(() =>
                assert.fail("ignored save ended before its artifact boundary")
              ),
            ]);
            assert.equal(state.requests.size, 0);
            assert.equal(state.background.size, 1);
            const idle = await database.transaction((tx) =>
              tx.query<{ count: string }>(
                "SELECT COUNT(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'"
              )
            );
            assert.equal(idle.rows[0].count, "0", "the S3 wait must not reserve a SQL transaction");
            const [pendingMetadata] = await rows(id);
            assert.ok(pendingMetadata, "metadata is durable before object dispatch");
            assert.equal(pendingMetadata.detail_state, "missing");
            assert.equal(pendingMetadata.artifact_relpath, null);
            gate.release();
            assert.equal(await callLogs.waitForCallLogSaves(10000), true);
            const [row] = await rows(id);
            assert.equal(row.api_key_id, key);
            assert.equal(row.api_key_name, "captured name");
            assert.deepEqual(JSON.parse(row.resilience_actions), { rotations: 1 });
            assert.equal(row.detail_state, "ready");
            assert.ok(callLogNativeFixture.objects.has(row.artifact_relpath));
            assert.deepEqual(observer!.runInAsyncScope(actions.readResilienceActions), {
              rotations: 1,
              continued: 1,
            });
          } finally {
            gate.release();
            observer?.emitDestroy();
          }
        }
      );

      await t.test(
        "handler rejection owns deferred reason extraction after its request ends",
        async () => {
          const { logHandlerRejection } =
            await import("../../src/sse/handlers/admissionRejectionLog.ts");
          const correlationId = `${prefix}-rejected`,
            key = `${prefix}-rejected-key`;
          await seedKey(key);
          const cloned = barrier();
          let controller!: ReadableStreamDefaultController<Uint8Array>;
          const response = new Response(
            new ReadableStream<Uint8Array>({
              start(value) {
                controller = value;
              },
            }),
            { status: 403, headers: { "Content-Type": "application/json" } }
          );
          const clone = response.clone.bind(response);
          response.clone = () => {
            const result = clone();
            cloned.resolve();
            return result;
          };
          const payload = { error: { code: "model_not_allowed", message: "Policy rejected" } };
          let released = false,
            admitted: Promise<unknown> | undefined;
          const release = () => {
            if (released) return;
            released = true;
            controller.enqueue(new TextEncoder().encode(JSON.stringify(payload)));
            controller.close();
          };
          try {
            await runtime!.runPersistenceRequest(async () => {
              assert.equal(
                logHandlerRejection(response, {
                  path: "/v1/chat/completions",
                  model: "rejected-model",
                  requestBody: { messages: [{ role: "user", content: "private input" }] },
                  apiKeyId: key,
                  apiKeyName: "rejected key",
                  correlationId,
                }),
                response,
                "logging must return the original rejection before reading its body"
              );
              admitted = [...state.background][0];
              assert.equal(state.background.size, 1, "reason extraction must already be owned");
            });
            assert.equal(state.requests.size, 0);
            await cloned.promise;
            assert.equal(state.background.size, 1);
            const idle = await database.transaction((tx) =>
              tx.query<{ count: string }>(
                "SELECT COUNT(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'"
              )
            );
            assert.equal(idle.rows[0].count, "0", "reason extraction must not hold a transaction");
            await database.transaction(async (tx) => {
              await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
              await tx.query("UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1", [key]);
            });
            const puts = callLogNativeFixture.puts;
            release();
            await admitted;
            const result = await database.transaction((tx) =>
              tx.query<{
                api_key_id: string;
                detail_state: string;
                has_request_body: number;
                has_response_body: number;
                error_summary: string;
              }>(
                "SELECT api_key_id,detail_state,has_request_body,has_response_body,error_summary FROM omniroute.call_logs WHERE correlation_id=$1",
                [correlationId]
              )
            );
            assert.equal(result.rows.length, 1);
            assert.equal(result.rows[0].api_key_id, key);
            assert.equal(result.rows[0].detail_state, "none");
            assert.equal(result.rows[0].has_request_body, 0);
            assert.equal(result.rows[0].has_response_body, 0);
            assert.equal(result.rows[0].error_summary, null, "privacy also removes error details");
            assert.equal(
              callLogNativeFixture.puts,
              puts,
              "current privacy forbids payload dispatch"
            );
            assert.equal(response.status, 403);
            assert.deepEqual(await response.json(), payload);
          } finally {
            release();
            await admitted?.catch(() => {});
            if (!response.bodyUsed) await response.arrayBuffer();
          }
        }
      );

      await t.test(
        "accepted call-log producers use one slot through preparation and persistence",
        async () => {
          const prepared = Array.from({ length: 64 }, () => barrier()),
            entered = Array.from({ length: 64 }, () => barrier()),
            jobs: Promise<void>[] = [];
          try {
            await runtime!.runPersistenceRequest(async () => {
              for (let index = 0; index < 64; index++) {
                jobs.push(
                  callLogs.saveCallLogFrom(async () => {
                    entered[index].resolve();
                    await prepared[index].promise;
                    return { id: `${prefix}-prepared-${index}`, status: 403 };
                  })
                );
                // Hold every producer without saturating the independent SQL admission budget.
                await Promise.race([
                  entered[index].promise,
                  jobs[index].then(() => assert.fail("producer ended before preparation")),
                ]);
              }
              assert.equal(state.background.size, 64);
            });
            assert.equal(await callLogs.waitForCallLogSaves(1), false);
            // The first save starts with all 64 background slots occupied and must reuse its slot.
            for (let index = 0; index < jobs.length; index++) {
              prepared[index].resolve();
              await jobs[index];
            }
            const result = await database.transaction((tx) =>
              tx.query<{ count: number }>(
                "SELECT COUNT(*)::int AS count FROM omniroute.call_logs WHERE id LIKE $1",
                [`${prefix}-prepared-%`]
              )
            );
            assert.equal(result.rows[0].count, 64, "preparation cannot consume a second admission");
          } finally {
            for (const gate of prepared) gate.resolve();
            await Promise.allSettled(jobs);
          }
        }
      );

      await t.test("failed or expired preparation leaves no pending write", async () => {
        const messages: string[] = [],
          original = console.error;
        let expired: AsyncResource | undefined,
          produced = false;
        console.error = (...values: unknown[]) => messages.push(values.map(String).join(" "));
        try {
          await runtime!.runPersistenceRequest(async () => {
            await assert.doesNotReject(
              callLogs.saveCallLogFrom(async () => {
                throw new Error("Rejection preparation failed");
              })
            );
            expired = new AsyncResource("expired-call-log-producer");
          });
          await expired!.runInAsyncScope(() =>
            callLogs.saveCallLogFrom(async () => {
              produced = true;
              return { id: `${prefix}-expired` };
            })
          );
          assert.equal(produced, false);
          assert.equal((await rows(`${prefix}-expired`)).length, 0);
          assert.equal(state.background.size, 0);
          assert.equal(await callLogs.waitForCallLogSaves(1000), true);
          assert.ok(messages.some((message) => message.includes("Rejection preparation failed")));
          assert.ok(messages.some((message) => message.includes("scope has ended")));
        } finally {
          expired?.emitDestroy();
          console.error = original;
        }
      });

      await t.test(
        "a foreign standalone authority is awaited directly and never routed through application admission",
        async () => {
          const { initializePostgresControlPlane } =
            await import("../../src/lib/db/postgres/controlPlane.ts");
          const { withPostgresCatalogSnapshot } =
            await import("../../src/lib/db/postgres/catalogSnapshot.ts");
          foreign = await initializePostgresControlPlane({
            connection: await testPostgresOptions(),
            runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
            logger: { warn() {} },
          });
          const id = `${prefix}-foreign`,
            before = callLogNativeFixture.puts;
          while (state.background.size) await Promise.allSettled([...state.background]);
          const slots = Array.from({ length: 64 }, () => barrier());
          for (const slot of slots) state.background.add(slot.promise);
          try {
            await foreign.run((binding) =>
              withPostgresCatalogSnapshot(binding.database!, async () => {
                assert.equal(
                  runtime!.isApplicationPersistenceIdentity(binding.databaseIdentity),
                  false
                );
                // A rerouted save would be refused by the saturated application admission gate.
                await callLogs.saveCallLog({ id, noLog: true });
                await callLogs.saveCallLogFrom(async () => ({
                  id: `${id}-prepared`,
                  noLog: true,
                }));
                assert.equal(state.background.size, 64);
              })
            );
            assert.equal((await rows(id)).length, 1);
            assert.equal((await rows(`${id}-prepared`)).length, 1);
            assert.equal(callLogNativeFixture.puts, before);
          } finally {
            for (const slot of slots) {
              slot.resolve();
              state.background.delete(slot.promise);
            }
          }
        }
      );

      await t.test(
        "cap refusal resolves best-effort, reports admission failure, and performs no artifact or insert",
        async () => {
          const id = `${prefix}-cap`,
            before = callLogNativeFixture.puts;
          while (state.background.size) await Promise.allSettled([...state.background]);
          const slots = Array.from({ length: 64 }, () => barrier());
          const messages: string[] = [],
            original = console.error;
          console.error = (...values: unknown[]) => messages.push(values.map(String).join(" "));
          for (const slot of slots) state.background.add(slot.promise);
          try {
            let produced = false;
            await runtime!.runPersistenceRequest(async () => {
              await assert.doesNotReject(
                callLogs.saveCallLog({ id, requestBody: { private: "not logged on refusal" } })
              );
              await assert.doesNotReject(
                callLogs.saveCallLogFrom(async () => {
                  produced = true;
                  return { id: `${id}-prepared` };
                })
              );
            });
            assert.equal(produced, false, "refused preparation must not read a request body");
            assert.equal((await rows(id)).length, 0);
            assert.equal(callLogNativeFixture.puts, before);
            assert.ok(messages.some((message) => message.includes("background admission")));
            assert.ok(messages.every((message) => !message.includes("not logged on refusal")));
            assert.equal(await callLogs.waitForCallLogSaves(1000), true);
          } finally {
            console.error = original;
            for (const slot of slots) {
              slot.resolve();
              state.background.delete(slot.promise);
            }
          }
        }
      );

      await t.test(
        "changed held identity refuses before privacy/artifact/domain insert",
        async () => {
          const id = `${prefix}-identity`,
            before = callLogNativeFixture.puts,
            identity = state.applicationIdentity;
          const original = console.error;
          const messages: string[] = [];
          console.error = (...values: unknown[]) => messages.push(values.map(String).join(" "));
          try {
            await runtime!.runPersistenceRequest(async () => {
              const operation = callLogs.saveCallLog({
                id,
                requestBody: { secret: "never uploaded" },
              });
              state.applicationIdentity = {};
              await operation;
            });
            assert.equal((await rows(id)).length, 0);
            assert.equal(callLogNativeFixture.puts, before);
            assert.ok(messages.some((message) => message.includes("authority changed")));
          } finally {
            state.applicationIdentity = identity;
            console.error = original;
          }
        }
      );

      await t.test("authority changes during preparation refuse the write", async () => {
        const id = `${prefix}-prepared-identity`,
          identity = state.applicationIdentity,
          prepared = barrier(),
          preparing = barrier(),
          messages: string[] = [],
          original = console.error;
        let saved: Promise<void> | undefined;
        console.error = (...values: unknown[]) => messages.push(values.map(String).join(" "));
        try {
          await runtime!.runPersistenceRequest(async () => {
            saved = callLogs.saveCallLogFrom(async () => {
              preparing.resolve();
              await prepared.promise;
              return { id, requestBody: { secret: "never dispatched" } };
            });
          });
          await preparing.promise;
          state.applicationIdentity = {};
          prepared.resolve();
          await saved;
          assert.equal((await rows(id)).length, 0);
          assert.ok(messages.some((message) => message.includes("authority changed during")));
          assert.ok(messages.every((message) => !message.includes("never dispatched")));
        } finally {
          prepared.resolve();
          await saved?.catch(() => {});
          state.applicationIdentity = identity;
          console.error = original;
        }
      });

      await t.test(
        "runtime close drains an accepted ignored save before closing the artifact store and pool",
        async () => {
          const id = `${prefix}-close`,
            gate = callLogNativeFixture.arm(),
            settled = barrier(),
            preparation = barrier(),
            preparing = barrier();
          let closing: Promise<void> | undefined;
          try {
            await runtime!.runPersistenceRequest(async () => {
              void callLogs.saveCallLog({ id, requestBody: { held: true } }).then(settled.resolve);
              void callLogs.saveCallLogFrom(async () => {
                preparing.resolve();
                await preparation.promise;
                return { id: `${id}-prepared`, requestBody: { held: true } };
              });
            });
            await Promise.race([
              gate.entered,
              settled.promise.then(() =>
                assert.fail("accepted save ended before its artifact boundary")
              ),
            ]);
            await preparing.promise;
            assert.equal(await callLogs.waitForCallLogSaves(1), false);
            closing = runtime!.closePersistenceRuntime();
            void closing.catch(() => {});
            assert.equal(runtime!.getPersistenceRuntimeStatus().phase, "closing");
            assert.equal(nativeRuntimeFixture.plane?.state, "ready");
            assert.equal(callLogNativeFixture.closes, 0);
            preparation.resolve();
            gate.release();
            await closing;
            assert.equal((await rows(id)).length, 1);
            assert.equal((await rows(`${id}-prepared`)).length, 1);
            assert.equal(callLogNativeFixture.closes, 1);
            assert.equal(nativeRuntimeFixture.plane?.state, "closed");
            assert.equal(await callLogs.waitForCallLogSaves(1000), true);
            assert.equal(existsSync(join(directory, "storage.sqlite")), false);
          } finally {
            preparation.resolve();
            gate.release();
            await closing?.catch(() => {});
          }
        }
      );
      await callLogs.closeCallLogSaves(10000);
    } finally {
      callLogNativeFixture.gate?.resolve();
      await foreign?.close();
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      try {
        await database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
          await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
          await tx.query(
            "DELETE FROM omniroute.call_logs WHERE id LIKE $1 OR correlation_id LIKE $1",
            [`${prefix}%`]
          );
          await tx.query("DELETE FROM omniroute.api_keys WHERE id LIKE $1", [`${prefix}%`]);
        });
      } finally {
        await database.close();
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
