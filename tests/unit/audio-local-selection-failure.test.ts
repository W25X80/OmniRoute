import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { registerHooks } from "node:module";
import { Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { MockAgent, getGlobalDispatcher, setGlobalDispatcher } from "undici";
import { useDecollidedMigrationsDir } from "./helpers/decollidedMigrationsDir.ts";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "audio-local-selection-failure-"));
Object.assign(process.env, {
  DATA_DIR: directory,
  OMNIROUTE_DATABASE_BACKEND: "sqlite",
  DISABLE_SQLITE_AUTO_BACKUP: "true",
  APP_LOG_TO_FILE: "false",
  API_KEY_SECRET: "audio-local-failure-fixture-secret",
  HTTP_PROXY: "",
  HTTPS_PROXY: "",
  ALL_PROXY: "",
});
useDecollidedMigrationsDir();

const helperUrl = new URL("../../src/sse/services/audioTargetSelection.ts", import.meta.url).href;
const portsUrl = new URL("./_fixtures/audioSelectionFailurePorts.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL?.split("?")[0] === helperUrl && specifier === "./scopedCredentials") {
      return { url: portsUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});

const dispatcher = getGlobalDispatcher();
const network = new MockAgent();
network.disableNetConnect();
setGlobalDispatcher(network);
let socketAttempts = 0;
const socketGuard = test.mock.method(Socket.prototype, "connect", () => {
  socketAttempts += 1;
  throw new Error("Audio failure fixture prohibits sockets");
});
const outbound: string[] = [];
let upstreamFailure: "503" | "throw" | "spoof" | null = null;
network
  .get("http://127.0.0.1:19101")
  .intercept({ method: "POST", path: () => true })
  .reply((options) => {
    outbound.push(options.path);
    const failed = upstreamFailure && options.path.startsWith("/target-A/");
    if (failed && upstreamFailure === "throw") {
      throw new Error("Unmarked upstream transport failure");
    }
    return {
      statusCode: failed ? 503 : 200,
      data: JSON.stringify(
        failed
          ? {
              error: {
                message: "Upstream unavailable",
                ...(upstreamFailure === "spoof"
                  ? {
                      code: "local_credential_selection_failure",
                      type: "local_credential_selection_failure",
                    }
                  : {}),
              },
            }
          : { text: "ok" }
      ),
      responseOptions: {
        headers: {
          "content-type": "application/json",
          ...(upstreamFailure === "spoof"
            ? { "x-omniroute-local-credential-selection-failure": "true" }
            : {}),
        },
      },
    };
  })
  .persist();

const { audioSelectionFailurePorts: ports } =
  await import("./_fixtures/audioSelectionFailurePorts.ts");
const core = await import("../../src/lib/db/core.ts");
const nodes = await import("../../src/lib/db/providers/nodes.ts");
const providers = await import("../../src/lib/db/providers.ts");
const keys = await import("../../src/lib/db/apiKeys.ts");
const combos = await import("../../src/lib/db/combos.ts");
const callLogs = await import("../../src/lib/usage/callLogs.ts");
const speech = await import("../../src/app/api/v1/audio/speech/route.ts");
const transcription = await import("../../src/app/api/v1/audio/transcriptions/route.ts");
const translation = await import("../../src/app/api/v1/audio/translations/route.ts");
const { resolveKeylessAudioTarget, withAudioSelectionErrorBoundary } =
  await import("../../src/sse/services/audioTargetSelection.ts");
const { markLocalCredentialSelectionFailure } =
  await import("../../open-sse/services/localCredentialSelectionFailure.ts");
const fallback = await import("../../open-sse/services/accountFallback.ts");
const cooldown = await import("../../open-sse/services/providerCooldownTracker.ts");
const breakers = await import("../../src/shared/utils/circuitBreaker.ts");
const metrics = await import("../../open-sse/services/comboMetrics.ts");
const semaphore = await import("../../open-sse/services/rateLimitSemaphore.ts");
const { PostgresError } = await import("../../src/lib/db/postgres/database.ts");
const { PersistenceStreamCapacityError } = await import("../../src/lib/db/persistenceRuntime.ts");
const productionFetch = globalThis.fetch;

test.beforeEach(() => {
  ports.failure = null;
  ports.calls.length = 0;
  ports.onSelection = null;
  outbound.length = 0;
  upstreamFailure = null;
  fallback.clearAllModelLockouts();
  cooldown.clearCooldownState();
  breakers.resetAllCircuitBreakers();
});
test.after(async () => {
  await callLogs.closeCallLogSaves(2_000);
  hooks.deregister();
  socketGuard.mock.restore();
  setGlobalDispatcher(dispatcher);
  await network.close();
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
  assert.equal(globalThis.fetch, productionFetch, "the test never replaces global fetch");
  assert.equal(socketAttempts, 0, "transport stays inside MockAgent");
});

async function fixture(strategy = "priority", maxRetries = 1) {
  const suffix = randomUUID();
  const targets = [];
  for (const label of ["A", "B"]) {
    const node = await nodes.createProviderNode({
      id: `openai-compatible-chat-${label}-${suffix}`,
      type: "openai-compatible",
      apiType: "chat",
      prefix: `local-audio-${label}-${suffix}`,
      name: `Local audio ${label}`,
      baseUrl: `http://127.0.0.1:19101/target-${label}/v1`,
    });
    const connection = await providers.createProviderConnection({
      provider: String(node.id),
      authType: "apikey",
      name: `Audio ${label}`,
      apiKey: "unused-keyless-fixture-key",
      isActive: true,
      testStatus: "active",
      providerSpecificData: { baseUrl: node.baseUrl },
    });
    targets.push({ node, connection });
  }
  const name = `audio-local-failure-${suffix}`;
  const ids = targets.map(({ connection }) => String(connection.id));
  await combos.createCombo({
    name,
    strategy,
    config: {
      maxRetries,
      retryDelayMs: 0,
      maxSetRetries: 0,
      queueTimeoutMs: 50,
      maxConcurrency: 1,
    },
    models: targets.map(({ node, connection }) => ({
      kind: "model",
      model: `${node.prefix}/fixture-whisper`,
      connectionId: String(connection.id),
      allowedConnectionIds: [String(connection.id)],
    })),
  });
  const key = await keys.createApiKey(`audio-local-${suffix}`, "fixture", [], {
    allowedConnections: ids,
  });
  return { name, targets, ids, bearer: String(key.key) };
}

function request(model: string, bearer: string, operation: string) {
  const form = new FormData();
  form.set("model", model);
  form.set("file", new Blob(["fixture audio"], { type: "audio/wav" }), "fixture.wav");
  return new Request(`http://localhost/api/v1/audio/${operation}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${bearer}` },
    body: form,
  });
}

for (const operation of ["transcriptions", "translations"] as const) {
  const route = operation === "transcriptions" ? transcription : translation;
  for (const strategy of ["priority", "round-robin"]) {
    test(`${operation} ${strategy}: local selection exception stops before sibling or provider failure`, async (t) => {
      const f = await fixture(strategy);
      const before = await Promise.all(f.ids.map((id) => providers.getProviderConnectionById(id)));
      let occupiedSlotObserved = false;
      if (strategy === "round-robin") {
        ports.onSelection = () => {
          const gates = Object.values(semaphore.getStats(`combo:${f.name}:`));
          assert.ok(
            gates.some((gate) => gate.running === 1),
            "selection holds the real RR slot"
          );
          occupiedSlotObserved = true;
        };
      }
      ports.failure = { value: new Error("Local credential repository unavailable") };
      const response = await route.POST(request(f.name, f.bearer, operation));
      await response.arrayBuffer();
      t.diagnostic(
        JSON.stringify({
          status: response.status,
          selections: ports.calls.map((args) => args[4]?.forcedConnectionId),
          outbound,
          failures: metrics.getComboMetrics(f.name)?.totalFailures ?? 0,
          fallbacks: metrics.getComboMetrics(f.name)?.totalFallbacks ?? 0,
          cooldownEntries: cooldown.getCooldownEntryCount(),
          modelLockouts: fallback.getModelLockoutSize(),
          breakerFailures: breakers.getAllCircuitBreakerStatuses().map((status) => ({
            name: status.name,
            failures: status.failureCount,
          })),
        })
      );
      assert.equal(ports.calls.length, 1, "a local failure cannot retry or advance to B");
      assert.equal(response.status, strategy === "priority" ? 502 : 500);
      assert.deepEqual(outbound, [], "selection failure is before the actual audio handler");
      const first = ports.calls[0];
      assert.equal(first[0], f.targets[0].node.id, "first node lookup must have no default escape");
      assert.deepEqual(first[2], { kind: "connections", ids: [f.ids[0]] });
      assert.equal(first[3], "fixture-whisper");
      assert.equal(first[4]?.forcedConnectionId, f.ids[0]);
      const comboMetrics = metrics.getComboMetrics(f.name);
      assert.equal(comboMetrics?.totalFailures ?? 0, 0);
      assert.equal(comboMetrics?.totalFallbacks ?? 0, 0);
      assert.equal(cooldown.getCooldownEntryCount(), 0);
      assert.equal(fallback.getModelLockoutSize(), 0);
      for (const status of breakers.getAllCircuitBreakerStatuses()) {
        assert.equal(status.failureCount, 0, `local exception polluted ${status.name}`);
      }
      const after = await Promise.all(f.ids.map((id) => providers.getProviderConnectionById(id)));
      assert.deepEqual(after, before, "selection failure cannot mutate account health");
      for (const gate of Object.values(semaphore.getStats(`combo:${f.name}:`))) {
        assert.equal(gate.running, 0, "round-robin must release its concurrency slot");
        assert.equal(gate.queued, 0);
      }
      if (strategy === "round-robin") {
        assert.equal(occupiedSlotObserved, true);
        assert.deepEqual(
          semaphore.getStats(`combo:${f.name}:`),
          {},
          "release deletes the idle gate"
        );
        ports.onSelection = null;
        ports.failure = null;
        ports.calls.length = 0;
        const next = await route.POST(request(f.name, f.bearer, operation));
        assert.equal(next.status, 200, "a subsequent request can acquire the released slot");
        await next.arrayBuffer();
        assert.equal(outbound.length, 1);
      }
    });
  }

  for (const failure of ["503", "throw", "spoof"] as const) {
    test(`${operation}: upstream ${failure} still advances to the permitted sibling`, async () => {
      const f = await fixture("priority", 0);
      upstreamFailure = failure;
      const response = await route.POST(request(f.name, f.bearer, operation));
      assert.equal(response.status, 200, await response.clone().text());
      await response.arrayBuffer();
      assert.deepEqual(outbound, [
        `/target-A/v1/audio/${operation}`,
        `/target-B/v1/audio/${operation}`,
      ]);
      assert.equal(ports.calls.length, 2, "real upstream failures retain bounded fallback");
      assert.equal(ports.calls[0][4]?.forcedConnectionId, f.ids[0]);
      assert.equal(ports.calls[1][4]?.forcedConnectionId, f.ids[1]);
      assert.equal(metrics.getComboMetrics(f.name)?.totalFailures, 1);
    });
  }
}

test("audio helper preserves the exact local selector exception object", async () => {
  const error = new Error("Identity sentinel for a local credential failure");
  ports.failure = { value: error };
  await assert.rejects(
    resolveKeylessAudioTarget({
      provider: {
        id: "local-failure",
        credentialProviderId: "openai-compatible-chat-local-failure",
        baseUrl: "http://127.0.0.1:19101/v1/audio/transcriptions",
        authType: "none",
        authHeader: "none",
        models: [],
      },
      operation: "transcriptions",
      requestedModel: "fixture-whisper",
      connectionScope: { kind: "connections", ids: ["physical-A"] },
      forcedConnectionId: "physical-A",
    }),
    (caught: unknown) => caught === error
  );
  assert.equal(ports.calls.length, 1);
  assert.deepEqual(outbound, []);
});

const directFailures: Array<[string, () => unknown]> = [
  ["native-scope", () => new PostgresError("invalid_scope")],
  ["native-capacity", () => new PersistenceStreamCapacityError()],
  ["primitive-null", () => null],
  ["primitive-undefined", () => undefined],
  ["primitive-zero", () => 0],
  ["private-error", () => new Error("fixture-private-token at /private/provider/secret.ts:1")],
];

for (const operation of ["speech", "transcriptions", "translations"] as const) {
  for (const [failure, createFailure] of directFailures) {
    test(`direct ${operation} ${failure}: exported POST resolves a fixed private503`, async () => {
      const f = await fixture();
      const before = await Promise.all(f.ids.map((id) => providers.getProviderConnectionById(id)));
      const key = await keys.createApiKey(`direct-${randomUUID()}`, "fixture", [], {
        allowedConnections: [f.ids[0]],
      });
      ports.failure = { value: createFailure() };
      const model = `${f.targets[0].node.prefix}/fixture-whisper`;
      const response =
        operation === "speech"
          ? await speech.POST(
              new Request("http://localhost/api/v1/audio/speech", {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  Authorization: `Bearer ${key.key}`,
                },
                body: JSON.stringify({ model, input: "fixture words", voice: "fixture" }),
              }),
              null
            )
          : await (operation === "transcriptions" ? transcription : translation).POST(
              request(model, String(key.key), operation)
            );
      assert.equal(response.status, 503);
      const text = await response.text();
      const body = JSON.parse(text) as { error: Record<string, unknown> };
      assert.equal(body.error.message, "Audio credentials unavailable");
      assert.equal("stack" in body.error, false);
      assert.ok(!text.includes("fixture-private-token"));
      assert.ok(!text.includes("at /"));
      assert.equal(ports.calls.length, 1);
      assert.equal(ports.calls[0][0], f.targets[0].node.id);
      assert.deepEqual(ports.calls[0][2], { kind: "connections", ids: [f.ids[0]] });
      assert.deepEqual(outbound, []);
      const after = await Promise.all(f.ids.map((id) => providers.getProviderConnectionById(id)));
      assert.deepEqual(after, before);
      assert.equal(metrics.getComboMetrics(f.name), null);
      assert.equal(cooldown.getCooldownEntryCount(), 0);
      assert.equal(fallback.getModelLockoutSize(), 0);
      assert.ok(
        breakers.getAllCircuitBreakerStatuses().every((status) => status.failureCount === 0)
      );
    });
  }
}

test("HTTP selection boundary awaits trusted rejection without inspecting getters", async () => {
  let reads = 0;
  const error = new Error();
  for (const field of ["message", "kind", "stack"]) {
    Object.defineProperty(error, field, {
      get() {
        reads += 1;
        throw new Error("Private error details must stay unread");
      },
    });
  }
  markLocalCredentialSelectionFailure(error);
  const handler = withAudioSelectionErrorBoundary(async () => {
    await Promise.resolve();
    throw error;
  });
  const response = await handler();
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error.message, "Audio credentials unavailable");
  assert.equal(reads, 0);
});

test("HTTP selection boundary rethrows an unmarked error by exact identity", async () => {
  const error = new Error("Unmarked failure must remain the caller's outcome");
  const handler = withAudioSelectionErrorBoundary(async () => {
    await Promise.resolve();
    throw error;
  });
  await assert.rejects(handler(), (caught: unknown) => caught === error);
});

test("HTTP selection boundary forwards the full tuple and preserves a returned Response", async () => {
  const incoming = new Request("http://localhost/api/v1/audio/speech");
  const context = { params: Promise.resolve({ id: "fixture-context" }) };
  const parsedBody = { input: "fixture words" };
  const expected = Response.json(
    { error: { message: "Original terminal response" } },
    { status: 503 }
  );
  const handler = withAudioSelectionErrorBoundary(
    async (received: Request, receivedContext: typeof context, body?: typeof parsedBody) => {
      assert.equal(received, incoming);
      assert.equal(receivedContext, context);
      assert.equal(body, parsedBody);
      return expected;
    }
  );
  const response = await handler(incoming, context, parsedBody);
  assert.equal(response, expected);
  assert.equal((await response.json()).error.message, "Original terminal response");
});
