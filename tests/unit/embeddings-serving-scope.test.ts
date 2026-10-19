import assert from "node:assert/strict";
import fs from "node:fs";
import { registerHooks } from "node:module";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import ts from "typescript";
import { useDecollidedMigrationsDir } from "./helpers/decollidedMigrationsDir.ts";

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-embedding-serving-scope-"));
process.env.DATA_DIR = dataDir;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.API_KEY_SECRET = "embedding-serving-scope-secret";
process.env.REQUIRE_API_KEY = "true";
process.env.NODE_ENV = "test";
useDecollidedMigrationsDir();

const handlerUrl = new URL("../../open-sse/handlers/embeddings.ts", import.meta.url).href;
const serviceUrl = new URL("../../src/lib/embeddings/service.ts", import.meta.url).href;
const portsUrl = new URL("./_fixtures/embeddingScopePorts.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      context.parentURL?.split("?")[0] === serviceUrl &&
      specifier === "@/sse/services/scopedCredentials"
    ) {
      return { url: portsUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
  load(url, context, nextLoad) {
    if (url.split("?")[0] === serviceUrl && process.env.A2_EMBEDDING_TEST_PREIMAGE) {
      // Causal predecessor runs bind the exact saved owner bytes at the same module URL.
      return {
        format: "module",
        shortCircuit: true,
        source: ts.transpileModule(
          fs.readFileSync(process.env.A2_EMBEDDING_TEST_PREIMAGE, "utf8"),
          {
            compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
          }
        ).outputText,
      };
    }
    if (url.split("?")[0] !== handlerUrl) return nextLoad(url, context);
    // Execute every actual handler statement, controlling only its free fetch transport.
    const source =
      `import { embeddingFetch as fetch } from ${JSON.stringify(portsUrl)};\n` +
      fs.readFileSync(new URL(handlerUrl), "utf8");
    return {
      format: "module",
      shortCircuit: true,
      source: ts.transpileModule(source, {
        compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
      }).outputText,
    };
  },
});

const { embeddingScopePorts: ports } = await import("./_fixtures/embeddingScopePorts.ts");
const core = await import("../../src/lib/db/core.ts");
const providers = await import("../../src/lib/db/providers.ts");
const models = await import("../../src/lib/db/models.ts");
const nodePrefixes = await import("../../src/lib/providerNodePrefixes.ts");
const apiKeys = await import("../../src/lib/db/apiKeys.ts");
const combos = await import("../../src/lib/db/combos.ts");
const quotaGroups = await import("../../src/lib/db/quotaGroups.ts");
const quotaPools = await import("../../src/lib/db/quotaPools.ts");
const keyScope = await import("../../src/sse/services/keyConnectionScope.ts");
const route = await import("../../src/app/api/v1/embeddings/route.ts");
const callLogs = await import("../../src/lib/usage/callLogs.ts");
// The production proxy module installs its own wrapper during imports; the test never replaces it.
const productionFetch = globalThis.fetch;

test.after(async () => {
  await callLogs.closeCallLogSaves(2_000);
  hooks.deregister();
  core.resetDbInstance();
  fs.rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  assert.equal(globalThis.fetch, productionFetch);
});
test.beforeEach(() => {
  ports.calls.length = 0;
  ports.diagnostic = null;
  ports.selected.length = 0;
});

async function key(
  allowedConnections: string[],
  blockedModels?: string[],
  allowedQuotas?: string[]
) {
  const created = await apiKeys.createApiKey("embedding-scope", "embedding-scope-machine", [], {
    allowedConnections,
  });
  if (blockedModels || allowedQuotas) {
    await apiKeys.updateApiKeyPermissions(created.id, { blockedModels, allowedQuotas });
  }
  return String(created.key);
}
async function request(apiKey: string, model = "llama-cpp/model-B") {
  return route.POST(
    new Request("http://localhost/v1/embeddings", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({ model, input: "hello scope" }),
    })
  );
}
async function refuse(response: Response) {
  const body = await response.text();
  assert.notEqual(response.status, 200, body);
  assert.equal(ports.calls.length, 0, "refusal must precede every outbound dispatch");
  assert.ok(!body.includes("at /"), body);
}

async function dynamicNode(prefix: string, name: string, baseUrl: string) {
  return providers.createProviderNode({
    type: "openai-compatible",
    name,
    prefix,
    apiType: "embeddings",
    baseUrl,
  });
}

async function dynamicConnection(provider: string, name: string, apiKey?: string) {
  return providers.createProviderConnection({
    provider,
    authType: "apikey",
    name,
    isActive: true,
    ...(apiKey ? { apiKey } : {}),
  });
}

let accountA: string;
let accountB: string;
test("empty local inventory denies a key scoped to a physical account", async () => {
  await refuse(await request(await key(["missing-physical-account"])));
});
test("empty local inventory retains explicit noauth and unrestricted defaults", async () => {
  for (const scope of [["noauth"], []]) {
    ports.calls.length = 0;
    const response = await request(await key(scope));
    assert.equal(response.status, 200, await response.text());
    assert.deepEqual(
      ports.calls.map((call) => call.url),
      ["http://127.0.0.1:8080/v1/embeddings"]
    );
    assert.equal(ports.calls[0].headers.get("authorization"), null);
  }
});
test("dynamic private nodes retain explicit noauth and unrestricted defaults", async () => {
  const node = await dynamicNode(
    "dynamic-noauth-default",
    "dynamic-noauth-default",
    "http://10.0.0.43:18081/v1"
  );
  for (const scope of [["noauth"], []]) {
    ports.calls.length = 0;
    const response = await request(await key(scope), "dynamic-noauth-default/model");
    assert.equal(response.status, 200, await response.text());
    assert.deepEqual(
      ports.calls.map((call) => call.url),
      [`${node.baseUrl}/embeddings`]
    );
    assert.equal(ports.calls[0].headers.get("authorization"), null);
  }
});
test("a dynamic key scoped only to noauth keeps the node default beside an excluded physical key", async () => {
  const node = await dynamicNode(
    "dynamic-noauth-scope-only",
    "dynamic-noauth-scope-only",
    "http://10.0.0.50:18088/v1"
  );
  await dynamicConnection(
    String(node.id),
    "dynamic-noauth-scope-only-physical-key",
    "must-not-be-sent"
  );
  const response = await request(await key(["noauth"]), `${node.prefix}/model`);
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.selected,
    [],
    "a noauth-only scope must skip physical credential selection"
  );
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    [`${node.baseUrl}/embeddings`]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), null);
});
test("a forced dynamic synthetic noauth combo keeps the node default beside an excluded physical key", async () => {
  const node = await dynamicNode(
    "dynamic-forced-noauth-scope-only",
    "dynamic-forced-noauth-scope-only",
    "http://10.0.0.51:18089/v1"
  );
  await dynamicConnection(
    String(node.id),
    "dynamic-forced-noauth-physical-key",
    "must-not-be-sent"
  );
  const comboName = "dynamic-forced-noauth-scope-only-combo";
  await combos.createCombo({
    name: comboName,
    models: [{ model: `${node.prefix}/model`, connectionId: "noauth" }],
    strategy: "priority",
  });
  const response = await request(await key(["noauth"]), comboName);
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.selected,
    [],
    "a synthetic noauth pin must skip physical credential selection"
  );
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    [`${node.baseUrl}/embeddings`]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), null);
});
test("a reserved dynamic prefix retains the static provider host and credentials", async () => {
  await dynamicNode("openai", "reserved-openai-node", "http://10.0.0.49:18087/v1");
  const connection = await providers.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: "static-openai-embedding",
    isActive: true,
    apiKey: "static-openai-token",
  });
  const connectionId = String(connection.id);
  const response = await request(await key([connectionId]), "openai/text-embedding-3-small");
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(ports.selected, [connectionId]);
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["https://api.openai.com/v1/embeddings"]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), "Bearer static-openai-token");
});
test("a non-serving-only key cannot authorize an unrelated static default", async () => {
  const a = await providers.createProviderConnection({
    provider: "llama-cpp",
    authType: "apikey",
    name: "embedding-model-A",
    isActive: true,
    apiKey: "selected-A-secret",
    defaultModel: "model-A",
    priority: 1,
    providerSpecificData: { baseUrl: "http://model-a.internal:5067/v1" },
  });
  const b = await providers.createProviderConnection({
    provider: "llama-cpp",
    authType: "apikey",
    name: "embedding-model-B",
    isActive: true,
    apiKey: "selected-B-secret",
    defaultModel: "/model-B",
    priority: 2,
    providerSpecificData: { baseUrl: "http://model-b.internal:8080/v1" },
  });
  accountA = String(a.id);
  accountB = String(b.id);
  // No synced model inventory is seeded: defaultModel alone must discriminate these accounts.
  await refuse(await request(await key([accountA])));
});
test("computed empty serving pool denies without reselecting a non-serving account", async () => {
  await refuse(await request(await key([accountA, accountB]), "llama-cpp/model-C"));
});
test("a matching scoped account binds its actual host, token and call-log identity", async () => {
  const response = await request(await key([accountB]));
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://model-b.internal:8080/v1/embeddings"]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), "Bearer selected-B-secret");
  assert.equal(ports.calls[0].body.model, "model-B");
  assert.equal(await callLogs.waitForCallLogSaves(2_000), true);
  const logs = await callLogs.getCallLogs({ limit: 10 });
  assert.ok(
    logs.some((entry) => entry.connectionId === accountB && entry.model === "llama-cpp/model-B")
  );
});
test("unrestricted selection still excludes a different defaultModel and keeps the selected host", async () => {
  const response = await request(await key([]));
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://model-b.internal:8080/v1/embeddings"]
  );
});
test("explicit noauth permits the synthetic default when no physical account serves the model", async () => {
  const response = await request(await key([accountA, "noauth"]), "llama-cpp/model-C");
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://127.0.0.1:8080/v1/embeddings"]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), null);
});
test("model policy and an unresolved quota scope deny before transport", async () => {
  await refuse(await request(await key([accountB], ["llama-cpp/model-B"])));
  await refuse(await request(await key([accountB], undefined, ["missing-quota-group"])));
});
test("resolved quota scope intersects the key and local serving model before selection", async () => {
  const groupA = await quotaGroups.createGroupAsync("embedding-serving-group-A");
  const groupB = await quotaGroups.createGroupAsync("embedding-serving-group-B");
  const poolA = await quotaPools.createPoolAsync({
    connectionId: accountA,
    name: "embedding-scope-A",
    groupId: groupA.id,
  });
  const poolB = await quotaPools.createPoolAsync({
    connectionId: accountB,
    name: "embedding-scope-B",
    groupId: groupB.id,
  });
  const quotaRequest = async (ids: string[], poolId: string) => {
    const apiKey = await key(ids, undefined, [poolId]);
    const scope = await keyScope.resolveKeyConnectionScope(await apiKeys.getApiKeyMetadata(apiKey));
    if (scope.rejection) return scope.rejection;
    return route.handleValidatedEmbeddingRequestBody(
      { model: "llama-cpp/model-B", input: "resolved quota scope" },
      { connectionScope: scope.connectionScope }
    );
  };
  // Public quota-exclusive keys intentionally require a quota model alias. Keep that guard;
  // this separate helper path qualifies real quota projection + service, not whole-route qtSd.
  const direct = await request(await key([accountA, accountB], undefined, [poolB.id]));
  assert.equal(direct.status, 403);
  assert.equal((await direct.json()).error.code, "QUOTA_ONLY");
  await refuse(await quotaRequest([accountA, accountB], poolA.id));
  await refuse(await quotaRequest([accountA], poolB.id));
  const response = await quotaRequest([accountA, accountB], poolB.id);
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://model-b.internal:8080/v1/embeddings"]
  );
});
test("combo target pin intersects model-serving scope without sibling dispatch", async () => {
  await combos.createCombo({
    name: "embedding-serving-pin-A",
    models: [{ model: "llama-cpp/model-B", connectionId: accountA }],
    strategy: "priority",
  });
  await refuse(await request(await key([accountA, accountB]), "embedding-serving-pin-A"));
  await combos.createCombo({
    name: "embedding-serving-pin-B",
    models: [{ model: "llama-cpp/model-B", connectionId: accountB }],
    strategy: "priority",
  });
  const response = await request(await key([accountA, accountB]), "embedding-serving-pin-B");
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://model-b.internal:8080/v1/embeddings"]
  );
});
for (const [diagnostic, status] of [
  [{ allRateLimited: true, retryAfter: 1 }, 429],
  [{ allExpired: true, expiredCount: 1 }, 401],
  [{ blockedByKeyPolicy: true }, 403],
  [{ leaseConnectionMismatch: true }, 409],
] as const) {
  test(`local ${Object.keys(diagnostic)[0]} diagnostic denies even an unrestricted caller`, async () => {
    ports.diagnostic = diagnostic;
    const before = await providers.getProviderConnections({ provider: "llama-cpp" });
    const response = await request(await key([]));
    assert.equal(response.status, status);
    await refuse(response);
    assert.deepEqual(await providers.getProviderConnections({ provider: "llama-cpp" }), before);
  });
}
for (const [diagnostic, status] of [
  [{ allRateLimited: true, retryAfter: 1 }, 429],
  [{ allExpired: true, expiredCount: 1 }, 401],
  [{ blockedByKeyPolicy: true }, 403],
  [{ leaseConnectionMismatch: true }, 409],
] as const) {
  test(`dynamic ${Object.keys(diagnostic)[0]} diagnostic denies before dispatch`, async () => {
    const node = await dynamicNode(
      `dynamic-diagnostic-${Object.keys(diagnostic)[0]}`,
      `dynamic-diagnostic-${Object.keys(diagnostic)[0]}`,
      "http://10.0.0.44:18082/v1"
    );
    ports.diagnostic = diagnostic;
    const response = await request(await key([]), `${node.prefix}/model`);
    assert.equal(response.status, status);
    await refuse(response);
  });
}
test("a cooling dynamic physical pin cannot fall through to an allowed noauth default", async () => {
  const node = await dynamicNode(
    "dynamic-cooling-pin",
    "dynamic-cooling-pin",
    "http://10.0.0.45:18083/v1"
  );
  const connection = await dynamicConnection(
    String(node.id),
    "dynamic-cooling-pin",
    "cooling-token"
  );
  const connectionId = String(connection.id);
  await providers.updateProviderConnection(connectionId, {
    rateLimitedUntil: new Date(Date.now() + 60_000).toISOString(),
  });
  const apiKey = await key([connectionId, "noauth"]);
  const resolvedScope = await keyScope.resolveKeyConnectionScope(
    await apiKeys.getApiKeyMetadata(apiKey)
  );
  assert.equal(resolvedScope.rejection, null);
  const response = await route.handleValidatedEmbeddingRequestBody(
    { model: `${node.prefix}/model`, input: "cooling pinned account" },
    { connectionScope: resolvedScope.connectionScope, forcedConnectionId: connectionId }
  );
  assert.equal(
    response.status,
    429,
    `unexpected status ${response.status} after ${ports.calls.length} outbound dispatches`
  );
  await refuse(response);
});
test("a tokenless dynamic account remains selected for host and call-log identity", async () => {
  const node = await dynamicNode(
    "dynamic-tokenless-account",
    "dynamic-tokenless-account",
    "http://10.0.0.46:18084/v1"
  );
  const connection = await dynamicConnection(String(node.id), "dynamic-tokenless-account");
  const connectionId = String(connection.id);
  const response = await request(await key([connectionId]), `${node.prefix}/model`);
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(ports.selected, [connectionId]);
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    [`${node.baseUrl}/embeddings`]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), null);
  assert.equal(await callLogs.waitForCallLogSaves(2_000), true);
  const logs = await callLogs.getCallLogs({ limit: 20 });
  assert.ok(
    logs.some(
      (entry) => entry.connectionId === connectionId && entry.model === `${node.prefix}/model`
    )
  );
});
test("duplicate dynamic prefixes reject loser ownership and bind winner host and credentials", async () => {
  const prefix = "dynamic-duplicate-prefix";
  const first = await dynamicNode(prefix, "dynamic-duplicate-first", "http://10.0.0.47:18085/v1");
  const second = await dynamicNode(prefix, "dynamic-duplicate-second", "http://10.0.0.48:18086/v1");
  const nodes = await providers.getProviderNodes();
  const winner = nodePrefixes.selectCompatibleNodeForPrefix(nodes, prefix);
  assert.ok(winner?.id);
  const loser = String(winner.id) === String(first.id) ? second : first;
  const winnerNode = String(winner.id) === String(first.id) ? first : second;
  const winnerConnection = await dynamicConnection(
    String(winnerNode.id),
    "dynamic-prefix-winner",
    "winner-token"
  );
  const loserConnection = await dynamicConnection(
    String(loser.id),
    "dynamic-prefix-loser",
    "loser-token"
  );
  const winnerConnectionId = String(winnerConnection.id);
  const loserConnectionId = String(loserConnection.id);

  await refuse(await request(await key([loserConnectionId]), `${prefix}/model`));
  await combos.createCombo({
    name: "dynamic-prefix-loser-pin",
    models: [
      {
        model: `${prefix}/model`,
        connectionId: loserConnectionId,
        allowedConnectionIds: [loserConnectionId, winnerConnectionId],
      },
    ],
    strategy: "priority",
  });
  await refuse(
    await request(await key([loserConnectionId, winnerConnectionId]), "dynamic-prefix-loser-pin")
  );

  ports.selected.length = 0;
  const response = await request(await key([winnerConnectionId]), `${prefix}/model`);
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(ports.selected, [winnerConnectionId]);
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    [`${winnerNode.baseUrl}/embeddings`]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), "Bearer winner-token");
  assert.equal(await callLogs.waitForCallLogSaves(2_000), true);
  const logs = await callLogs.getCallLogs({ limit: 20 });
  assert.ok(
    logs.some(
      (entry) => entry.connectionId === winnerConnectionId && entry.model === `${prefix}/model`
    )
  );
});
test("tokenless configured local accounts retain their selected host", async () => {
  const account = await providers.createProviderConnection({
    provider: "lemonade",
    authType: "apikey",
    name: "tokenless-selected-local",
    isActive: true,
    defaultModel: "model-D",
    providerSpecificData: { baseUrl: "http://tokenless.internal:13305/v1" },
  });
  const response = await request(await key([String(account.id)]), "lemonade/model-D");
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://tokenless.internal:13305/v1/embeddings"]
  );
  assert.equal(ports.calls[0].headers.get("authorization"), null);
});
test("a local account without defaultModel keeps legacy model compatibility", async () => {
  const account = await providers.createProviderConnection({
    provider: "lemonade",
    authType: "apikey",
    name: "model-unspecified-local",
    isActive: true,
    providerSpecificData: { baseUrl: "http://unspecified.internal:13305/v1" },
  });
  const response = await request(await key([String(account.id)]), "lemonade/model-E");
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://unspecified.internal:13305/v1/embeddings"]
  );
});
test("an already selected synced endpoint is retained without a second account selection", async () => {
  await models.replaceSyncedAvailableModelsForConnection("llama-cpp", accountB, [
    { id: "model-B", name: "model-B", supportedEndpoints: ["embeddings"] },
  ]);
  const response = await request(await key([accountB]));
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(ports.selected, [accountB]);
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://model-b.internal:8080/v1/embeddings"]
  );
});

test("a model-compatible forced automatic inactive account retains its explicit probe", async () => {
  const account = await providers.createProviderConnection({
    provider: "lemonade",
    authType: "apikey",
    name: "automatic-inactive-probe",
    isActive: false,
    testStatus: "active",
    defaultModel: "probe-model",
    providerSpecificData: { baseUrl: "http://probe.internal:13305/v1" },
  });
  const id = String(account.id);
  await combos.createCombo({
    name: "embedding-automatic-inactive-probe",
    models: [{ model: "lemonade/probe-model", connectionId: id }],
    strategy: "priority",
  });
  const response = await request(await key([id]), "embedding-automatic-inactive-probe");
  assert.equal(response.status, 200, await response.text());
  assert.deepEqual(ports.selected, [id]);
  assert.deepEqual(
    ports.calls.map((call) => call.url),
    ["http://probe.internal:13305/v1/embeddings"]
  );
});

test("a missing physical combo pin cannot become a synthetic noauth default", async () => {
  const account = await providers.createProviderConnection({
    provider: "lemonade",
    authType: "apikey",
    name: "deleted-physical-pin",
    isActive: true,
    defaultModel: "deleted-model",
  });
  const id = String(account.id);
  await providers.deleteProviderConnection(id);
  // A persisted/imported target can still name a row that no longer exists.
  await combos.createCombo({
    name: "embedding-deleted-pin-with-noauth",
    models: [
      { model: "lemonade/deleted-model", connectionId: id, allowedConnectionIds: [id, "noauth"] },
    ],
    strategy: "priority",
  });
  await refuse(await request(await key([id, "noauth"]), "embedding-deleted-pin-with-noauth"));
});

test("an explicit synthetic noauth pin retains the local default", async () => {
  await combos.createCombo({
    name: "embedding-synthetic-noauth-pin",
    models: [{ model: "lemonade/synthetic-model", connectionId: "noauth" }],
    strategy: "priority",
  });
  const response = await request(await key(["noauth"]), "embedding-synthetic-noauth-pin");
  assert.equal(response.status, 200, await response.text());
  assert.equal(ports.calls.length, 1);
  assert.equal(ports.calls[0].headers.get("authorization"), null);
});
