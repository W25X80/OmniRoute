import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getEventListeners } from "node:events";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
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
import { closeNativeStreamingFixture } from "../helpers/persistence/nativeStreamingFixtureShutdown.ts";
import type { PersistenceTailPermit } from "../../src/lib/db/persistenceRuntime.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import type { ConnectionScope } from "../../open-sse/services/connectionScope.ts";

// Real runtime, PostgreSQL engine/catalog, handleChatCore, provider executor and stream owners.
// Only provider HTTP and S3 bytes are controlled; this is not AWS/provider acceptance evidence.
test(
  "native chat serving reserves the 64th permit before provider work and conserves it on every outcome",
  { skip: !postgresTestsEnabled, timeout: 45000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "omni-native-tail-admission-"));
    const environment = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "STORAGE_ENCRYPTION_KEY",
      "APP_LOG_TO_FILE",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "STREAM_RECOVERY_ENABLED",
      "OMNIROUTE_BUILDING",
      "NEXT_PHASE",
    ];
    const previous = new Map(environment.map((key) => [key, process.env[key]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.DATA_DIR = directory;
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.APP_LOG_TO_FILE = "false";
    process.env.STREAM_RECOVERY_ENABLED = "false";
    delete process.env.OMNIROUTE_BUILDING;
    delete process.env.NEXT_PHASE;
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const contextUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/controlPlaneContext.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/streamingNativeIntegrationFixture.ts",
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
        const result = nextResolve(specifier, context);
        if (result.url === contextUrl && context.parentURL !== probeUrl)
          return { url: probeUrl, shortCircuit: true };
        return result;
      },
    });
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let verification: PostgresDatabase | undefined;
    let keyId = "",
      connectionId = "",
      openAiConnectionId = "";
    const permits: PersistenceTailPermit[] = [];
    const originalFetch = globalThis.fetch;
    let fetchCalls = 0,
      executorCalls = 0;
    let upstream: () => Response = () => {
      throw new Error("Unexpected paid dispatch in capacity oracle");
    };
    try {
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      verification = await createTestPostgresDatabase();
      const { createApiKey } = await import("../../src/lib/db/apiKeys.ts");
      const { createProviderConnection } = await import("../../src/lib/db/providers.ts");
      const { getCachedSettings } = await import("../../src/lib/db/readCache.ts");
      const { GlmExecutor } = await import("../../open-sse/executors/glm.ts");
      const { DefaultExecutor } = await import("../../open-sse/executors/default.ts");
      const execute = GlmExecutor.prototype.execute;
      t.mock.method(
        GlmExecutor.prototype,
        "execute",
        function (this: InstanceType<typeof GlmExecutor>, input: Parameters<typeof execute>[0]) {
          executorCalls++;
          assert.equal(
            runtime!.getPersistenceRuntimeStatus().streamTails,
            64,
            "the provider sees the already reserved 64th permit"
          );
          return Reflect.apply(execute, this, [input]);
        }
      );
      const executeDefault = DefaultExecutor.prototype.execute;
      t.mock.method(
        DefaultExecutor.prototype,
        "execute",
        function (
          this: InstanceType<typeof DefaultExecutor>,
          input: Parameters<typeof executeDefault>[0]
        ) {
          executorCalls++;
          assert.equal(runtime!.getPersistenceRuntimeStatus().streamTails, 64);
          return Reflect.apply(executeDefault, this, [input]);
        }
      );
      globalThis.fetch = (async () => {
        fetchCalls++;
        assert.equal(runtime!.getPersistenceRuntimeStatus().streamTails, 64);
        return upstream();
      }) as typeof fetch;
      const { handleChatCore } = await import("../../open-sse/handlers/chatCore.ts");
      const tag = randomUUID();
      await runtime.runPersistenceRequest(async () => {
        keyId = String(
          (await createApiKey(`native-tail-${tag}`, "private-native-tail-fixture")).id
        );
        connectionId = String(
          (
            await createProviderConnection({
              provider: "glm",
              authType: "apikey",
              apiKey: "fixture-only",
              isActive: true,
              name: `native-tail-${tag}`,
              providerSpecificData: { primaryTransport: "openai" },
            })
          ).id
        );
        openAiConnectionId = String(
          (
            await createProviderConnection({
              provider: "openai",
              authType: "apikey",
              apiKey: "fixture-only",
              isActive: true,
              name: `native-tail-json-${tag}`,
            })
          ).id
        );
        for (let index = 0; index < 64; index++)
          permits.push(await runtime!.acquirePersistenceTail());
      });
      const serve = (
        stream = true,
        signal?: AbortSignal,
        provider: "glm" | "openai" = "glm",
        connectionScope?: ConnectionScope
      ) =>
        runtime!.runPersistenceRequest(async () => {
          const model = provider === "glm" ? "glm-4.7" : "gpt-4o-mini";
          const selectedConnectionId = provider === "glm" ? connectionId : openAiConnectionId;
          const body = {
            model: `${provider}/${model}`,
            stream,
            messages: [{ role: "user", content: "hi" }],
            max_tokens: 8,
          };
          return handleChatCore({
            body,
            cachedSettings: await getCachedSettings(),
            modelInfo: {
              provider,
              model,
              extendedContext: false,
            },
            credentials: {
              apiKey: "fixture-only",
              connectionId: selectedConnectionId,
              ...(provider === "glm"
                ? { providerSpecificData: { primaryTransport: "openai" } }
                : {}),
            },
            connectionId: selectedConnectionId,
            connectionScope,
            apiKeyInfo: { id: keyId, name: "private-native-tail-fixture" },
            log: { debug() {}, info() {}, warn() {}, error() {} },
            skipResourcePressureGuard: true,
            clientRawRequest: {
              endpoint: "/v1/chat/completions",
              body,
              headers: new Headers({
                accept: stream ? "text/event-stream" : "application/json",
              }),
              signal,
            },
          } as Parameters<typeof handleChatCore>[0]);
        });
      const capacitySignal = new AbortController();
      const abortListenersBefore = getEventListeners(capacitySignal.signal, "abort").length;
      const overflow = await serve(true, capacitySignal.signal);
      assert.equal(
        getEventListeners(capacitySignal.signal, "abort").length,
        abortListenersBefore,
        "the undispatched controller releases its client listener"
      );
      assert.equal(overflow.success, false);
      assert.equal(overflow.response.status, 503);
      const publicFailure = await overflow.response.json();
      assert.equal(publicFailure.error.code, runtime.PERSISTENCE_STREAM_CAPACITY_CODE);
      assert.equal(publicFailure.error.type, "local_queue_capacity");
      assert.equal(publicFailure.error.message.includes("at /"), false);
      assert.equal(
        executorCalls,
        0,
        "64 tails reject the valid small serving request before executor work"
      );
      assert.equal(fetchCalls, 0, "local rejection cannot consume provider HTTP/quota");
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 64);
      await permits.pop()!.release();
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 63);
      const { getPendingById, getPendingRequests } =
        await import("../../src/lib/usage/usageHistory.ts");
      const healthBefore = await verification.query(
        "SELECT test_status,is_active,rate_limited_until,last_error FROM omniroute.provider_connections WHERE id=$1",
        [connectionId]
      );
      for (const stream of [true, false]) {
        const controller = new AbortController();
        const pendingBefore = {
          size: getPendingById().size,
          byModel: JSON.stringify(getPendingRequests().byModel),
          byAccount: JSON.stringify(getPendingRequests().byAccount),
        };
        const local = await serve(stream, controller.signal, "glm", {
          kind: "connections",
          ids: ["unselected-connection"],
        });
        assert.equal(local.success, false);
        assert.equal(local.response.status, 403);
        assert.equal((await local.response.json()).error.code, "connection_scope_denied");
        assert.equal(executorCalls, 0, "a local scope refusal must not reach the executor");
        assert.equal(fetchCalls, 0, "a local scope refusal must not spend upstream work");
        assert.equal(getEventListeners(controller.signal, "abort").length, 0);
        assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 63);
        controller.abort();
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(getPendingById().size, pendingBefore.size);
        assert.equal(JSON.stringify(getPendingRequests().byModel), pendingBefore.byModel);
        assert.equal(JSON.stringify(getPendingRequests().byAccount), pendingBefore.byAccount);
        assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 63);
        assert.deepEqual(
          (
            await verification.query(
              "SELECT test_status,is_active,rate_limited_until,last_error FROM omniroute.provider_connections WHERE id=$1",
              [connectionId]
            )
          ).rows,
          healthBefore.rows
        );
        assert.equal(
          Number(
            (
              await verification.query(
                "SELECT COUNT(*) AS count FROM omniroute.usage_history WHERE api_key_id=$1",
                [keyId]
              )
            ).rows[0].count
          ),
          0,
          "local refusal must not create upstream failure usage"
        );
      }
      for (const mode of [
        "upstream-error",
        "transport-error",
        "json-conversion",
        "normal-eof",
      ] as const) {
        const callsBefore = executorCalls;
        const fetchBefore = fetchCalls;
        upstream = () => {
          if (mode === "transport-error") throw new Error("Fixture transport unavailable");
          if (mode === "upstream-error")
            return new Response(
              JSON.stringify({
                error: {
                  message: "Fixture request refused",
                  type: "invalid_request_error",
                },
              }),
              { status: 400, headers: { "content-type": "application/json" } }
            );
          if (mode === "json-conversion")
            return new Response(
              JSON.stringify({
                id: "chatcmpl-private",
                object: "chat.completion",
                choices: [
                  {
                    index: 0,
                    message: { role: "assistant", content: "hi" },
                    finish_reason: "stop",
                  },
                ],
                usage: {
                  prompt_tokens: 1,
                  completion_tokens: 1,
                  total_tokens: 2,
                },
              }),
              { headers: { "content-type": "application/json" } }
            );
          return new Response(
            'data: {"id":"chatcmpl-private","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}\n\ndata: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}\n\ndata: [DONE]\n\n',
            { headers: { "content-type": "text/event-stream" } }
          );
        };
        const result = await serve(true, undefined, mode === "json-conversion" ? "openai" : "glm");
        assert.equal(executorCalls, callsBefore + 1, `${mode} uses one executor attempt`);
        assert.equal(
          fetchCalls - fetchBefore,
          mode === "transport-error" ? 2 : 1,
          "GLM transport fallback retains the same reservation"
        );
        assert.equal(result.success, mode === "json-conversion" || mode === "normal-eof");
        await result.response.text();
        const completionDeadline = performance.now() + 7000;
        while (runtime.getPersistenceRuntimeStatus().streamTails !== 63) {
          if (performance.now() >= completionDeadline)
            throw new Error("Reserved completion permit did not settle");
          await new Promise<void>((resolve) => setTimeout(resolve, 10));
        }
        assert.equal(
          runtime.getPersistenceRuntimeStatus().streamTails,
          63,
          `${mode} releases exactly its one reserved permit`
        );
      }
      const abort = new AbortController();
      abort.abort();
      const beforeAbort = executorCalls;
      const aborted = await serve(true, abort.signal);
      assert.equal(aborted.response.status, 499);
      assert.equal(
        executorCalls,
        beforeAbort,
        "pre-dispatch abort does not spend an upstream attempt"
      );
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 63);
      assert.equal(
        streamingNativeFixture.sqliteAttempts,
        0,
        streamingNativeFixture.sqliteAttemptStacks.join("\n")
      );
    } finally {
      globalThis.fetch = originalFetch;
      await Promise.allSettled(permits.map((permit) => permit.release()));
      t.mock.restoreAll();
      try {
        await closeNativeStreamingFixture(runtime);
      } finally {
        try {
          if (verification) {
            try {
              await verification.transaction(async (tx) => {
                await tx.query("DELETE FROM omniroute.call_logs WHERE api_key_id=$1", [keyId]);
                await tx.query("DELETE FROM omniroute.usage_history WHERE api_key_id=$1", [keyId]);
                for (const table of [
                  "request_cost_ledger",
                  "domain_cost_receipts",
                  "domain_cost_history",
                  "api_key_quota_counters",
                ])
                  await tx.query(`DELETE FROM omniroute.${table} WHERE api_key_id=$1`, [keyId]);
                await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [keyId]);
                for (const id of [connectionId, openAiConnectionId])
                  await tx.query("DELETE FROM omniroute.provider_connections WHERE id=$1", [id]);
              });
            } finally {
              await verification.close();
            }
          }
        } finally {
          hooks.deregister();
          delete globalThis.__omniroutePersistenceRuntime;
          for (const [key, value] of previous) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
          }
          rmSync(directory, { recursive: true, force: true });
        }
      }
    }
  }
);
