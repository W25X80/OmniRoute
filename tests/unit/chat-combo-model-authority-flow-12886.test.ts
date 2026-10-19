/**
 * #12886 / #12899 — a restricted key may name a combo, while the concrete
 * target still has to pass every downstream policy check.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const COMBO = "combo-authority-flow-12886";
const TARGET = "openai/gpt-4.1";
const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-combo-authority-flow-"));
const TEST_PLUGIN_DIR = path.join(TEST_DATA_DIR, "plugins");
process.env.DATA_DIR = TEST_DATA_DIR;
process.env.OMNIROUTE_PLUGINS_DIR = TEST_PLUGIN_DIR;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.REQUIRE_API_KEY = "false";
process.env.DASHBOARD_PASSWORD = "";
process.env.INITIAL_PASSWORD = "";
process.env.API_KEY_SECRET = process.env.API_KEY_SECRET || "combo-authority-flow-12886";
delete process.env.JWT_SECRET;
delete process.env.QUOTA_STORE_DRIVER;
delete process.env.QUOTA_STORE_REDIS_URL;

const core = await import("../../src/lib/db/core.ts");
const apiKeysDb = await import("../../src/lib/db/apiKeys.ts");
const combosDb = await import("../../src/lib/db/combos.ts");
const providersDb = await import("../../src/lib/db/providers.ts");
const callLogs = await import("../../src/lib/usage/callLogs.ts");
const proxyLogger = await import("../../src/lib/proxyLogger.ts");
const { enforceClientApiRouteAuth } = await import("../../src/shared/utils/clientApiRouteAuth.ts");
const { handleChat } = await import("../../src/sse/handlers/chat.ts");
const originalFetch = globalThis.fetch;

type CallLogRow = {
  apiKeyId?: string | null;
  correlationId?: string | null;
  status?: number;
  error?: string | null;
};

function uniqueCorrelationId(label: string): string {
  return `combo-authority-flow-${label}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
}

async function rowsByCorrelation(correlationId: string): Promise<Array<CallLogRow>> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    await callLogs.waitForCallLogSaves(1_000);
    const found = await callLogs.getCallLogs({ correlationId, limit: 50 });
    const rows = (Array.isArray(found) ? found : []).filter(
      (row: CallLogRow) => row.correlationId === correlationId
    );
    if (rows.length > 0) return rows;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return [];
}

async function resetStorage() {
  globalThis.fetch = originalFetch;
  proxyLogger.flushProxyLogsSync();
  apiKeysDb.resetApiKeyState();
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, {
    recursive: true,
    force: true,
    maxRetries: 5,
    retryDelay: 100,
  });
  fs.mkdirSync(TEST_PLUGIN_DIR, { recursive: true });
}

test.beforeEach(async () => {
  await resetStorage();
});

test.after(() => {
  globalThis.fetch = originalFetch;
  proxyLogger.flushProxyLogsSync();
  apiKeysDb.resetApiKeyState();
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, {
    recursive: true,
    force: true,
    maxRetries: 5,
    retryDelay: 100,
  });
});

async function seedNamedComboKey() {
  await providersDb.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: "openai-combo-authority-flow",
    apiKey: "sk-combo-authority-flow",
    isActive: true,
    testStatus: "active",
  });
  await combosDb.createCombo({
    name: COMBO,
    strategy: "priority",
    models: [TARGET],
  });
  const key = await apiKeysDb.createApiKey("Combo authority flow", "machine-12886");
  await apiKeysDb.updateApiKeyPermissions(key.id, { allowedModels: [COMBO] });
  return key;
}

function comboRequest(key: string, headers: Record<string, string> = {}) {
  return new Request("http://localhost/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${key}`,
      ...headers,
    },
    body: JSON.stringify({
      model: COMBO,
      messages: [{ role: "user", content: "Reply with OK only." }],
      max_tokens: 16,
      stream: false,
    }),
  });
}

function directModelRequest(key: string, headers: Record<string, string> = {}) {
  return new Request("http://localhost/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${key}`,
      ...headers,
    },
    body: JSON.stringify({
      model: TARGET,
      messages: [{ role: "user", content: "Reply with OK only." }],
      max_tokens: 16,
      stream: false,
    }),
  });
}

function upstreamResponse(content: string) {
  return Response.json({
    id: "chatcmpl-combo-authority-flow",
    choices: [{ message: { role: "assistant", content } }],
  });
}

test("named-combo allow-list reaches its configured concrete target through handleChat", async () => {
  const key = await seedNamedComboKey();
  let upstreamCalls = 0;
  globalThis.fetch = async () => {
    upstreamCalls += 1;
    return upstreamResponse("combo policy passed");
  };

  const response = await handleChat(comboRequest(key.key));
  const body = (await response.json()) as {
    choices: Array<{ message: { content: string } }>;
  };

  assert.equal(response.status, 200);
  assert.equal(upstreamCalls, 1);
  assert.equal(body.choices[0].message.content, "combo policy passed");
});

// A stored combo named in the key's allowedCombos (default `combo/*`) grants its
// own targets, overriding blockedModels (#14197 — see the next test). The
// live-test marker must still grant nothing when the combo is NOT granted.
test("live-test marker does not let a non-granted named combo dispatch a blocked target", async () => {
  const key = await seedNamedComboKey();
  await apiKeysDb.updateApiKeyPermissions(key.id, {
    allowedCombos: ["other-combo"],
    blockedModels: [TARGET],
  });
  let upstreamCalls = 0;
  globalThis.fetch = async () => {
    upstreamCalls += 1;
    return upstreamResponse("must not dispatch");
  };

  const response = await handleChat(
    comboRequest(key.key, { "X-Internal-Test": "combo-health-check" })
  );

  assert.equal(response.status, 403);
  assert.equal(upstreamCalls, 0);
});

test("allowedCombos still grants a combo's blocked direct-model target at dispatch (#14197)", async () => {
  const key = await seedNamedComboKey();
  await apiKeysDb.updateApiKeyPermissions(key.id, {
    modelAccessMode: "restricted",
    allowedModels: ["gemini/*"],
    allowedCombos: [COMBO],
    blockedModels: [TARGET],
  });
  let upstreamCalls = 0;
  globalThis.fetch = async () => {
    upstreamCalls += 1;
    return upstreamResponse("explicit combo grant");
  };

  const response = await handleChat(comboRequest(key.key));
  const body = (await response.json()) as {
    choices: Array<{ message: { content: string } }>;
  };

  assert.equal(response.status, 200);
  assert.equal(upstreamCalls, 1);
  assert.equal(body.choices[0].message.content, "explicit combo grant");
});

test("resolved direct-model dispatch keeps the accepted x-api-key when Authorization is stale", async () => {
  const key = await seedNamedComboKey();
  await apiKeysDb.updateApiKeyPermissions(key.id, {
    modelAccessMode: "restricted",
    allowedModels: [TARGET],
  });
  const correlationId = uniqueCorrelationId("accepted-x-api-key");
  let upstreamCalls = 0;
  globalThis.fetch = async () => {
    upstreamCalls += 1;
    return upstreamResponse("accepted x-api-key dispatched");
  };

  const response = await handleChat(
    directModelRequest("stale-bearer-that-is-not-a-real-key", {
      "x-api-key": key.key,
      "x-correlation-id": correlationId,
    }),
    null,
    null,
    correlationId
  );
  const payload = (await response.clone().json()) as {
    choices?: Array<{ message: { content: string } }>;
    error?: { message?: string };
  };
  const rows = await rowsByCorrelation(correlationId);

  assert.equal(
    response.status,
    200,
    `expected dispatch under accepted key ${key.id}; received ${response.status} ${JSON.stringify(payload)}; ` +
      `upstreamCalls=${upstreamCalls}; callLogKeys=${rows.map((row) => row.apiKeyId).join(",")}`
  );
  assert.equal(upstreamCalls, 1);
  assert.equal(payload.choices?.[0]?.message.content, "accepted x-api-key dispatched");
  assert.ok(rows.length > 0, "the accepted dispatch should leave a call log row");
  assert.ok(rows.every((row) => row.apiKeyId === key.id));
});

test("accepted x-api-key still cannot dispatch a model blocked by that key", async () => {
  const key = await seedNamedComboKey();
  await apiKeysDb.updateApiKeyPermissions(key.id, {
    modelAccessMode: "restricted",
    allowedModels: [TARGET],
    blockedModels: [TARGET],
  });
  const correlationId = uniqueCorrelationId("blocked-combo-target");
  let upstreamCalls = 0;
  globalThis.fetch = async () => {
    upstreamCalls += 1;
    return upstreamResponse("blocked target must not dispatch");
  };

  const response = await handleChat(
    directModelRequest("stale-bearer-that-is-not-a-real-key", {
      "x-api-key": key.key,
      "x-correlation-id": correlationId,
    }),
    null,
    null,
    correlationId
  );
  const payload = (await response.clone().json()) as { error?: { message?: string } };
  const rows = await rowsByCorrelation(correlationId);

  assert.notEqual(response.status, 200);
  assert.equal(upstreamCalls, 0);
  assert.match(JSON.stringify(payload), /not allowed|blocked|unavailable/i);
  assert.ok(rows.some((row) => row.apiKeyId === key.id));
});

test("route auth rejects a stale Bearer without any accepted credential when API keys are required", async () => {
  const previousRequireApiKey = process.env.REQUIRE_API_KEY;
  process.env.REQUIRE_API_KEY = "true";

  try {
    const rejection = await enforceClientApiRouteAuth(
      comboRequest("stale-bearer-that-is-not-a-real-key", {
        "x-correlation-id": uniqueCorrelationId("no-accepted-credential"),
      })
    );
    assert.equal(rejection?.status, 401);
  } finally {
    if (previousRequireApiKey === undefined) delete process.env.REQUIRE_API_KEY;
    else process.env.REQUIRE_API_KEY = previousRequireApiKey;
  }
});
