import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { pipeToNodeResponse } from "next/dist/server/pipe-readable.js";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { streamingNativeFixture } from "../helpers/persistence/streamingNativeIntegrationFixture.ts";
import { closeNativeStreamingFixture } from "../helpers/persistence/nativeStreamingFixtureShutdown.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";

declare global {
  var __omniroutePersistenceRequest:
    | ((run: () => unknown | Promise<unknown>, httpLifetime: Promise<void>) => Promise<void>)
    | undefined;
}

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function eventually(predicate: () => boolean | Promise<boolean>, timeout = 7000) {
  const end = performance.now() + timeout;
  while (!(await predicate())) {
    if (performance.now() >= end) throw new Error("Native recovery ownership oracle timed out");
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}
async function deadline<T>(work: Promise<T>, milliseconds = 15000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Native recovery teardown timed out")),
          milliseconds
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
const encodeChat = (delta: unknown, finish_reason: string | null = null) =>
  `data: ${JSON.stringify({ id: "chatcmpl-private-recovery", object: "chat.completion.chunk", choices: [{ index: 0, delta, finish_reason }] })}\n\n`;

// Actual held HTTP boundary, handleChatCore, GLM executor/managed native fence and Antigravity
// executor/credits transform. The redirected private HTTP carrier and memory artifacts exclude
// upstream provider/AWS acceptance; PostgreSQL engine connections use the configured TLS server.
test(
  "one native tail owns recovery and credits after a real completed-tool HTTP disconnect",
  { skip: !postgresTestsEnabled, timeout: 90000 },
  async (t) => {
    const tag = randomUUID();
    const directory = mkdtempSync(join(tmpdir(), "omni-native-recovery-http-"));
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "APP_LOG_TO_FILE",
      "STREAM_RECOVERY_ENABLED",
      "STREAM_RECOVERY_MIDSTREAM_ENABLED",
      "STREAM_DISCONNECT_GRACE_PERIOD_MS",
      "OMNIROUTE_BUILDING",
      "NEXT_PHASE",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.APP_LOG_TO_FILE = "false";
    process.env.STREAM_RECOVERY_ENABLED = "true";
    process.env.STREAM_RECOVERY_MIDSTREAM_ENABLED = "true";
    process.env.STREAM_DISCONNECT_GRACE_PERIOD_MS = "6000";
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
    const previousHook = globalThis.__omniroutePersistenceRequest;
    const originalFetch = globalThis.fetch;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let verification: PostgresDatabase | undefined;
    let current:
      | {
          kind: "flag-on" | "flag-off" | "credits" | "cancel" | "grace-expired";
          calls: number;
          postCloseExecutions?: number;
          eof: ReturnType<typeof barrier>;
          old?: PostgresControlPlaneBinding;
          done: ReturnType<typeof barrier>;
          work?: Promise<void>;
          profile?: { enabled: boolean; continueMidStream: boolean; toolCallOrderFix: boolean };
          client?: {
            bytes: number;
            handoffComplete: boolean;
            terminalSeen: boolean;
            events: string[];
          };
          recoveryOutcomes: Array<{
            level: string;
            attempt: number;
            outcome: string;
            reason?: string;
          }>;
        }
      | undefined;
    let glmCredentials: Record<string, unknown> | undefined,
      creditCredentials: Record<string, unknown> | undefined;
    let priorFlag: string | undefined;
    let flagCaptured = false;
    let keyId = "",
      keyName = "",
      leaseOwnerId = `vlo_${randomBytes(32).toString("base64url")}`,
      generation = 0;
    const connectionIds: string[] = [];
    const handlerWork = new Set<Promise<void>>();
    const gates: Array<ReturnType<typeof barrier>> = [];
    let handleChatCore: typeof import("../../open-sse/handlers/chatCore.ts").handleChatCore;
    let context: typeof import("../../src/lib/db/controlPlaneContext.ts");
    const captureRecovery = (level: string, tag: string, line: string) => {
      if (tag !== "STREAM_RECOVERY" || !current || current.recoveryOutcomes.length >= 12) return;
      const continuation =
        /^mid-stream continuation attempt (\d+)\/\d+(?: outcome=(suffix|overlap-reject|terminal|empty|no-stream|refused)(?: reason=(budget|tool-call|not-continuable))?)?/.exec(
          line
        );
      const early = /^transparent early-retry (\d+)\/\d+ after /.exec(line);
      const attempt = Number(continuation?.[1] ?? early?.[1]);
      if (!Number.isSafeInteger(attempt) || attempt < 0) return;
      current.recoveryOutcomes.push({
        level,
        attempt,
        outcome: continuation ? (continuation[2] ?? "started") : "early-retry",
        ...(continuation?.[3] ? { reason: continuation[3] } : {}),
      });
    };
    const log = {
      debug: (tag: string, line: string) => captureRecovery("debug", tag, line),
      info: (tag: string, line: string) => captureRecovery("info", tag, line),
      warn: (tag: string, line: string) => captureRecovery("warn", tag, line),
      error() {},
    };
    const server = createServer((request, response) => {
      if (request.url?.startsWith("/provider/")) {
        const active = current!;
        active.calls++;
        request.resume();
        response.writeHead(200, { "content-type": "text/event-stream" });
        const producer = (async () => {
          if (active.kind === "credits") {
            response.write(
              `data: ${JSON.stringify({ response: { candidates: [{ content: { role: "model", parts: [{ text: "Preparing tool. " }, { functionCall: { name: "lookup", args: {} } }] }, finishReason: "STOP" }] } })}\n\n`
            );
            await active.eof.promise;
            response.write(
              'data: {"remainingCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"123"}]}\n\n'
            );
            response.end();
            return;
          }
          if (active.calls > 1) {
            // One deliberately accepted continuation under the existing overlap heuristic.
            response.end(
              encodeChat({ content: "Preparing tool. Recovered tail." }, "stop") +
                "data: [DONE]\n\n"
            );
            return;
          }
          response.write(encodeChat({ role: "assistant", content: "Preparing tool. " }));
          // Commit the authored holdback, then send complete tool start/finish in one small write.
          await new Promise<void>((resolve) => setTimeout(resolve, 1100));
          if (active.kind === "cancel") {
            response.write(encodeChat({ content: "Still generating plain text." }));
            await active.eof.promise;
            response.end();
            return;
          }
          response.write(
            encodeChat({
              tool_calls: [
                {
                  index: 0,
                  id: "call_lookup",
                  type: "function",
                  function: { name: "lookup", arguments: "{}" },
                },
              ],
            }) + encodeChat({}, "tool_calls")
          );
          await active.eof.promise;
          response.end(); // Deliberate source-shaped truncation before usage/overall terminal.
        })();
        handlerWork.add(producer);
        void producer.catch(() => response.destroy()).finally(() => handlerWork.delete(producer));
        return;
      }
      const work = (async () => {
        const active = current!;
        context.getSelectedControlPlane()!.assertReady();
        active.old = context.getSelectedControlPlane();
        const signal = new AbortController();
        const { bindPersistenceRequestCallback } =
          await import("../../scripts/dev/persistence-request-boundary.mjs");
        response.once(
          "close",
          bindPersistenceRequestCallback(() => signal.abort("private_client_closed"))
        );
        request.resume();
        const provider = active.kind === "credits" ? "antigravity" : "glm";
        const model = provider === "glm" ? "glm-4.7" : "gemini-2.5-flash";
        const credentials = provider === "glm" ? glmCredentials! : creditCredentials!;
        const body = {
          model: `${provider}/${model}`,
          stream: true,
          input: [{ role: "user", content: "Call lookup." }],
          tools: [
            { type: "function", name: "lookup", parameters: { type: "object", properties: {} } },
          ],
        };
        const { getCachedSettings } = await import("../../src/lib/db/readCache.ts");
        const { resolveResilienceSettings } = await import("../../src/lib/resilience/settings.ts");
        const { isFeatureFlagEnabled } = await import("../../src/shared/utils/featureFlags.ts");
        const cachedSettings = await getCachedSettings();
        const recovery = resolveResilienceSettings(cachedSettings).streamRecovery;
        active.profile = {
          enabled: recovery.enabled,
          continueMidStream: recovery.continueMidStream,
          toolCallOrderFix: isFeatureFlagEnabled("STREAM_RECOVERY_TOOLCALL_ORDER_FIX"),
        };
        assert.equal(
          active.profile.enabled,
          true,
          "actual held-catalog recovery profile is enabled"
        );
        assert.equal(
          active.profile.continueMidStream,
          true,
          "actual held-catalog profile permits continuation"
        );
        assert.equal(active.profile.toolCallOrderFix, active.kind === "flag-on");
        const result = await handleChatCore({
          body,
          cachedSettings,
          modelInfo: { provider, model, extendedContext: false },
          credentials,
          apiKeyInfo: { id: keyId, name: keyName },
          connectionId: String(credentials.connectionId),
          userAgent: "codex/0.162.1",
          log,
          clientRawRequest: {
            endpoint: "/v1/responses",
            body,
            headers: new Headers({ "user-agent": "codex/0.162.1", originator: "codex_cli_rs" }),
            signal: signal.signal,
          },
          ...(provider === "glm"
            ? { managedLease: { apiKeyId: keyId, context: { leaseOwnerId, generation } } }
            : {}),
        } as Parameters<typeof handleChatCore>[0]);
        assert.ok(result.response?.body);
        assert.equal(result.success, true);
        response.writeHead(result.response.status, Object.fromEntries(result.response.headers));
        try {
          // Match Next's abort-aware HTTP pump: socket close cancels a pending body read.
          // The source completed-tool grace keeps its producer on the same accepted tail.
          await pipeToNodeResponse(result.response.body!, response);
        } finally {
          active.done.resolve();
        }
      })();
      handlerWork.add(work);
      void work
        .catch((error) => {
          t.diagnostic(error instanceof Error ? error.name : "Handler error");
          response.destroy();
        })
        .finally(() => handlerWork.delete(work));
      return work;
    });
    try {
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      context = await import(contextUrl);
      const { wrapRequestListenerWithPersistence } =
        await import("../../scripts/dev/persistence-request-boundary.mjs");
      const rawListener = server.listeners("request")[0] as Parameters<
        typeof wrapRequestListenerWithPersistence
      >[0];
      server.removeAllListeners("request");
      // The upstream fixture is outside application routing; public route uses the exact HTTP owner.
      const wrapped = wrapRequestListenerWithPersistence(rawListener);
      globalThis.__omniroutePersistenceRequest = (run, lifetime) =>
        runtime!.runPersistenceRequest(async () => {
          await run();
          await lifetime;
        });
      server.on("request", (request, response) =>
        request.url?.startsWith("/provider/")
          ? rawListener(request, response)
          : wrapped(request, response)
      );
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const address = server.address();
      assert.ok(address && typeof address !== "string");
      const baseUrl = `http://127.0.0.1:${address.port}`;
      globalThis.fetch = ((input, init) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        if (url.startsWith(baseUrl)) return originalFetch(input, init);
        assert.ok(current, "no provider request outside a scenario");
        const target = new URL(url);
        assert.ok(
          target.pathname.endsWith("/chat/completions") ||
            target.pathname.includes(":streamGenerateContent"),
          "unexpected non-provider request in the private carrier"
        );
        assert.equal(String(init?.body ?? "").includes("runNativeStreamingPersistence"), false);
        assert.equal(String(init?.body ?? "").includes("adoptNativeStreamingPersistence"), false);
        return originalFetch(`${baseUrl}/provider/${current!.kind}`, init);
      }) as typeof fetch;
      const { createApiKey } = await import("../../src/lib/db/apiKeys.ts");
      const { createProviderConnection } = await import("../../src/lib/db/providers.ts");
      const { acquireExclusiveConnectionLeaseAsync } =
        await import("../../src/lib/db/exclusiveConnectionLeases.ts");
      const { setFeatureFlagOverrideAsync, getFeatureFlagOverrideAsync } =
        await import("../../src/lib/db/featureFlags.ts");
      const { seedAntigravityIdeVersionCache, seedAntigravityCliVersionCache } =
        await import("../../open-sse/services/antigravityVersion.ts");
      seedAntigravityIdeVersionCache("2026.10.11-private");
      seedAntigravityCliVersionCache("2026.10.11-private");
      const { GlmExecutor } = await import("../../open-sse/executors/glm.ts");
      const originalExecute = GlmExecutor.prototype.execute;
      let postCloseExecutions = 0;
      t.mock.method(
        GlmExecutor.prototype,
        "execute",
        function (
          this: InstanceType<typeof GlmExecutor>,
          input: Parameters<typeof originalExecute>[0]
        ) {
          if (current!.calls >= 1) {
            assert.throws(() => current!.old!.assertReady(), /scope has ended/);
            assert.equal(
              context.getSelectedPersistenceIdentity(),
              globalThis.__omniroutePersistenceRuntime!.applicationIdentity
            );
            postCloseExecutions++;
            current!.postCloseExecutions = (current!.postCloseExecutions ?? 0) + 1;
          }
          return Reflect.apply(originalExecute, this, [input]);
        }
      );
      handleChatCore = (await import("../../open-sse/handlers/chatCore.ts")).handleChatCore;
      verification = await createTestPostgresDatabase();
      await runtime.runPersistenceRequest(async () => {
        priorFlag = await getFeatureFlagOverrideAsync("STREAM_RECOVERY_TOOLCALL_ORDER_FIX");
        flagCaptured = true;
        const key = await createApiKey(`native-recovery-${tag}`, "private-recovery-fixture");
        keyId = String(key.id);
        keyName = String(key.name);
        for (const provider of ["glm", "antigravity"]) {
          const connection = await createProviderConnection({
            provider,
            authType: provider === "glm" ? "apikey" : "oauth",
            apiKey: "fixture-only",
            accessToken: "fixture-token",
            isActive: true,
            providerSpecificData:
              provider === "glm"
                ? { primaryTransport: "openai" }
                : { projectId: "private-fixture" },
            name: `native-${provider}-${tag}`,
          });
          connectionIds.push(String(connection.id));
          const credentials = {
            apiKey: "fixture-only",
            accessToken: "fixture-token",
            projectId: "private-fixture",
            connectionId: String(connection.id),
            providerSpecificData:
              provider === "glm"
                ? { primaryTransport: "openai" }
                : { projectId: "private-fixture" },
          };
          if (provider === "glm") glmCredentials = credentials;
          else creditCredentials = credentials;
        }
        const acquired = await acquireExclusiveConnectionLeaseAsync({
          leaseOwnerId,
          apiKeyId: keyId,
          provider: "glm",
          connectionId: connectionIds[0],
          ttlMs: 120000,
        });
        assert.equal(acquired.kind, "ACQUIRED");
        if (acquired.kind === "ACQUIRED") generation = acquired.lease.generation;
      });
      for (const kind of ["flag-on", "flag-off", "credits", "cancel", "grace-expired"] as const) {
        await runtime.runPersistenceRequest(() =>
          setFeatureFlagOverrideAsync(
            "STREAM_RECOVERY_TOOLCALL_ORDER_FIX",
            kind === "flag-on" ? "true" : "false"
          )
        );
        current = { kind, calls: 0, eof: barrier(), done: barrier(), recoveryOutcomes: [] };
        gates.push(current.eof);
        const abort = new AbortController();
        const client = await originalFetch(`${baseUrl}/${kind}`, {
          method: "POST",
          body: "{}",
          signal: abort.signal,
        });
        const reader = client.body!.getReader();
        let data = "";
        const decoder = new TextDecoder();
        const { createCompletedResponsesToolHandoffWatcher } =
          await import("../../open-sse/utils/responsesToolHandoff.ts");
        const handoff = createCompletedResponsesToolHandoffWatcher();
        const observedClient = {
          bytes: 0,
          handoffComplete: false,
          terminalSeen: false,
          events: [] as string[],
        };
        current.client = observedClient;
        const prefixEventTypes = new Set([
          "response.created",
          "response.in_progress",
          "response.output_item.added",
          "response.content_part.added",
          "response.output_text.delta",
          "response.output_text.done",
          "response.content_part.done",
          "response.output_item.done",
          "response.function_call_arguments.delta",
          "response.function_call_arguments.done",
          "response.custom_tool_call_input.delta",
          "response.custom_tool_call_input.done",
          "response.completed",
          "response.incomplete",
          "response.failed",
        ]);
        try {
          while (
            kind === "cancel"
              ? !data.includes("response.output_text.delta")
              : !observedClient.handoffComplete
          ) {
            const chunk = await deadline(reader.read());
            assert.equal(
              chunk.done,
              false,
              "client stream ended before its required observed prefix"
            );
            const text = decoder.decode(chunk.value, { stream: true });
            observedClient.bytes += chunk.value.byteLength;
            // Feed only bytes returned to this client; no hidden upstream/handler state qualifies handoff.
            observedClient.handoffComplete = handoff.note(text);
            data = (data + text).slice(-4096);
            observedClient.terminalSeen ||= [
              "response.completed",
              "response.incomplete",
              "response.failed",
              "[DONE]",
            ].some((marker) => data.includes(marker));
            for (const match of data.matchAll(/"type"\s*:\s*"([a-z_.]+)"/g)) {
              if (
                prefixEventTypes.has(match[1]) &&
                !observedClient.events.includes(match[1]) &&
                observedClient.events.length < 12
              )
                observedClient.events.push(match[1]);
            }
          }
          if (kind !== "cancel") {
            assert.equal(
              observedClient.handoffComplete,
              true,
              "client observed matching tool arguments and completed item"
            );
            assert.equal(
              observedClient.terminalSeen,
              false,
              "the controlled upstream EOF has not been released"
            );
          }
          abort.abort();
          await deadline(current.done.promise);
          await eventually(() => runtime!.getPersistenceRuntimeStatus().activeRequests === 0);
          assert.throws(() => current!.old!.assertReady(), /scope has ended/);
          if (kind === "cancel") {
            assert.ok(runtime.getPersistenceRuntimeStatus().streamTails <= 1);
          } else {
            assert.equal(
              runtime.getPersistenceRuntimeStatus().streamTails,
              1,
              "the completed-tool pipeline shares the accounting tail, not a second permit"
            );
          }
          if (kind === "grace-expired") {
            await eventually(() => runtime!.getPersistenceRuntimeStatus().streamTails === 0, 12000);
            assert.equal(current.calls, 1, "grace expiry aborts the producer without a paid retry");
          }
          current.eof.resolve();
          await eventually(() => runtime!.getPersistenceRuntimeStatus().streamTails === 0);
          await runtime.drainPersistenceAcceptedWork();
          if (kind === "flag-on" || kind === "cancel" || kind === "grace-expired")
            assert.equal(current.calls, 1);
          if (kind === "flag-off") {
            assert.equal(current.calls, 2);
            assert.equal(postCloseExecutions, 1);
            assert.deepEqual(
              current.recoveryOutcomes.map(({ attempt, outcome }) => [attempt, outcome]),
              [
                [1, "started"],
                [1, "suffix"],
              ],
              "one post-close continuation accepts its coherent suffix"
            );
          }
          if (kind === "credits") {
            const rows = await verification.transaction((tx) =>
              tx.query<{ value: string }>(
                "SELECT value FROM omniroute.key_value WHERE namespace='antigravityCreditBalance' AND key=$1",
                [connectionIds[1]]
              )
            );
            assert.equal(JSON.parse(rows.rows[0]?.value ?? "null")?.balance, 123);
            const { getAntigravityRemainingCredits } =
              await import("../../open-sse/executors/antigravity.ts");
            assert.equal(
              await runtime.runPersistenceRequest(() =>
                getAntigravityRemainingCredits(connectionIds[1])
              ),
              123,
              "the actual captured executor cache agrees with its source EOF observation"
            );
          }
        } finally {
          current.eof.resolve();
          abort.abort();
          await reader.cancel().catch(() => {});
          reader.releaseLock();
        }
      }
      assert.equal(
        streamingNativeFixture.sqliteAttempts,
        0,
        streamingNativeFixture.sqliteAttemptStacks.join("\n")
      );
    } catch (error) {
      const errorName =
        error instanceof Error &&
        ["AssertionError", "Error", "TypeError", "AbortError", "TimeoutError"].includes(error.name)
          ? error.name
          : "Error";
      t.diagnostic(
        `Native recovery failure operands: ${JSON.stringify({
          kind: current?.kind,
          errorName,
          calls: current?.calls,
          postCloseExecutions: current?.postCloseExecutions,
          profile: current?.profile,
          observedClientPrefix: current?.client,
          recoveryOutcomes: current?.recoveryOutcomes,
        })}`
      );
      throw error;
    } finally {
      for (const gate of gates) gate.resolve();
      server.closeAllConnections();
      await deadline(Promise.allSettled([...handlerWork])).catch(() => {});
      await deadline(closeNativeStreamingFixture(runtime)).catch(() => {});
      globalThis.fetch = originalFetch;
      globalThis.__omniroutePersistenceRequest = previousHook;
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      try {
        if (verification)
          await verification.transaction(async (tx) => {
            if (flagCaptured) {
              if (priorFlag === undefined)
                await tx.query(
                  "DELETE FROM omniroute.key_value WHERE namespace='feature_flags' AND key='STREAM_RECOVERY_TOOLCALL_ORDER_FIX'"
                );
              else
                await tx.query(
                  "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('feature_flags','STREAM_RECOVERY_TOOLCALL_ORDER_FIX',$1) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value",
                  [priorFlag]
                );
            }
            await tx.query("DELETE FROM omniroute.call_logs WHERE api_key_id=$1", [keyId]);
            await tx.query("DELETE FROM omniroute.usage_history WHERE api_key_id=$1", [keyId]);
            await tx.query(
              "DELETE FROM omniroute.exclusive_connection_leases WHERE api_key_id=$1",
              [keyId]
            );
            await tx.query(
              "DELETE FROM omniroute.key_value WHERE namespace='antigravityCreditBalance' AND key=ANY($1::text[])",
              [connectionIds]
            );
            await tx.query("DELETE FROM omniroute.provider_connections WHERE id=ANY($1::text[])", [
              connectionIds,
            ]);
            await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [keyId]);
          });
      } finally {
        await verification?.close();
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
