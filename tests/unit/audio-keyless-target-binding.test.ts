import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { Agent, MockAgent, getGlobalDispatcher, setGlobalDispatcher } from "undici";
import { useDecollidedMigrationsDir } from "./helpers/decollidedMigrationsDir.ts";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "audio-target-binding-"));
Object.assign(process.env, {
  DATA_DIR: directory,
  OMNIROUTE_DATABASE_BACKEND: "sqlite",
  DISABLE_SQLITE_AUTO_BACKUP: "true",
  APP_LOG_TO_FILE: "false",
  API_KEY_SECRET: "audio-target-binding-fixture-secret",
  JWT_SECRET: "audio-target-binding-fixture-session",
  HTTP_PROXY: "",
  HTTPS_PROXY: "",
  ALL_PROXY: "",
});
useDecollidedMigrationsDir();
const dispatcher = getGlobalDispatcher();
const network = new MockAgent();
network.disableNetConnect();
setGlobalDispatcher(network);
const sockets: string[] = [];
let activeCase = "imports";
const connect = test.mock.method(Socket.prototype, "connect", (...args: unknown[]) => {
  const options = (args[0] || {}) as Record<string, unknown>;
  sockets.push(`${activeCase}: ${String(options.host)}:${String(options.port)}`);
  throw new Error("Audio fixture prohibits real sockets");
});

type Call = { path: string; headers: Record<string, string>; body: string };
const calls: Call[] = [];
network
  .get("http://127.0.0.1:19091")
  .intercept({ method: "POST", path: () => true })
  .reply((options) => {
    const headers: Record<string, string> = {};
    if (Array.isArray(options.headers)) {
      for (let i = 0; i < options.headers.length; i += 2) {
        headers[String(options.headers[i]).toLowerCase()] = String(options.headers[i + 1]);
      }
    } else if (options.headers && typeof (options.headers as Headers).forEach === "function") {
      (options.headers as Headers).forEach((value, name) => (headers[name.toLowerCase()] = value));
    } else {
      for (const [key, value] of Object.entries(options.headers || {})) {
        headers[key.toLowerCase()] = String(value);
      }
    }
    calls.push({ path: options.path, headers, body: String(options.body || "") });
    const speech = options.path.includes("speech") || options.path.endsWith("/api/tts");
    return {
      statusCode: 200,
      data: speech ? "audio fixture" : JSON.stringify({ text: "fixture transcript" }),
      responseOptions: {
        headers: { "content-type": speech ? "audio/wav" : "application/json" },
      },
    };
  })
  .persist();

network
  .get("http://127.0.0.1:19092")
  .intercept({ method: "POST", path: () => true })
  .reply((options) => {
    calls.push({
      path: `host-19092:${options.path}`,
      headers: Object.fromEntries(new Headers(options.headers as HeadersInit).entries()),
      body: String(options.body || ""),
    });
    return {
      statusCode: 200,
      data: "alternate local host fixture",
      responseOptions: { headers: { "content-type": "audio/wav" } },
    };
  })
  .persist();

const googleTransport = network.get("https://translate.google.com");
googleTransport
  .intercept({ method: "POST", path: () => true })
  .reply((options) => {
    calls.push({
      path: `gtts:${options.path}`,
      headers: Object.fromEntries(new Headers(options.headers as HeadersInit).entries()),
      body: String(options.body || ""),
    });
    const inner = JSON.stringify([
      Buffer.from("gtts fixture").toString("base64"),
      null,
      null,
      null,
      null,
      null,
      [],
    ]);
    const outer = JSON.stringify([["wrb.fr", "jQ1olc", inner, null, null, null, "generic"]]);
    return {
      statusCode: 200,
      data: `)]}'\n\n${outer.length}\n${outer}\n`,
      responseOptions: { headers: { "content-type": "text/plain" } },
    };
  })
  .persist();
// The product proxy transport may allocate an explicit TLS Agent for gtts.
// Intercept its normal dispatcher boundary too, without replacing global fetch.
const dispatch = Agent.prototype.dispatch;
const explicitDispatcher = test.mock.method(
  Agent.prototype,
  "dispatch",
  function (
    this: Agent,
    options: Parameters<Agent["dispatch"]>[0],
    handler: Parameters<Agent["dispatch"]>[1]
  ) {
    return String(options.origin) === "https://translate.google.com"
      ? googleTransport.dispatch(options, handler)
      : dispatch.call(this, options, handler);
  }
);

const core = await import("../../src/lib/db/core.ts");
const nodes = await import("../../src/lib/db/providers/nodes.ts");
const providers = await import("../../src/lib/db/providers.ts");
const keys = await import("../../src/lib/db/apiKeys.ts");
const combos = await import("../../src/lib/db/combos.ts");
const logs = await import("../../src/lib/usage/callLogs.ts");
const speech = await import("../../src/app/api/v1/audio/speech/route.ts");
const transcription = await import("../../src/app/api/v1/audio/transcriptions/route.ts");
const translation = await import("../../src/app/api/v1/audio/translations/route.ts");
const management = await import("../../src/app/api/providers/[id]/route.ts");
const { makeManagementSessionRequest } = await import("../helpers/managementSession.ts");
const deletion = await import("../../src/lib/db/providers/deletion.ts");
// Product imports install the existing proxy/logging wrapper. The test never replaces it.
const originalFetch = globalThis.fetch;

test.beforeEach((context) => {
  activeCase = context.name;
  calls.length = 0;
});
test.after(async () => {
  await logs.waitForCallLogSaves(2_000);
  connect.mock.restore();
  explicitDispatcher.mock.restore();
  setGlobalDispatcher(dispatcher);
  await network.close();
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
  assert.equal(globalThis.fetch, originalFetch, "transport must not replace global fetch");
  assert.deepEqual(sockets, [], "all transport must remain inside MockAgent");
});

async function fixture() {
  const suffix = randomUUID();
  const node = await nodes.createProviderNode({
    id: `openai-compatible-chat-${suffix}`,
    type: "openai-compatible",
    apiType: "chat",
    prefix: `audio-${suffix}`,
    name: "Local audio fixture",
    baseUrl: "http://127.0.0.1:19091/default/v1",
  });
  const create = async (name: string, priority: number, root: string) => {
    const connection = await providers.createProviderConnection({
      provider: String(node.id),
      authType: "apikey",
      name,
      apiKey: `must-not-send-${name}`,
      isActive: true,
      testStatus: "active",
      priority,
      providerSpecificData: {
        baseUrl: node.baseUrl,
        customHeaders: { Authorization: "must-not-send-custom" },
      },
    });
    await updateConnection(String(connection.id), {
      providerSpecificData: { baseUrl: `http://127.0.0.1:19091/${root}/v1` },
    });
    return connection;
  };
  const b = await create("B", 1, "selected-B");
  const a = await create("A", 2, "selected-A");
  return { node, a, b };
}

async function updateConnection(id: string, body: Record<string, unknown>) {
  const response = await management.PUT(
    await makeManagementSessionRequest(`http://localhost/api/providers/${id}`, {
      method: "PUT",
      body,
    }),
    { params: Promise.resolve({ id }) }
  );
  assert.equal(response.status, 200, await response.clone().text());
}

async function key(ids?: string[]) {
  const result = await keys.createApiKey(`audio-${randomUUID()}`, "audio-fixture", [], {
    allowedConnections: ids,
  });
  return String(result.key);
}

async function combo(
  f: Awaited<ReturnType<typeof fixture>>,
  ids: string[],
  force = String(f.a.id)
) {
  const name = `audio-combo-${randomUUID()}`;
  await combos.createCombo({
    name,
    strategy: "priority",
    models: [
      {
        kind: "model",
        model: `${f.node.prefix}/fixture-voice`,
        connectionId: force,
        allowedConnectionIds: ids,
      },
    ],
  });
  return name;
}

function speechRequest(model: string, bearer: string) {
  return new Request("http://localhost/api/v1/audio/speech", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${bearer}` },
    body: JSON.stringify({ model, input: "fixture words", voice: "fixture" }),
  });
}

function multipartRequest(model: string, bearer: string, operation: string) {
  const form = new FormData();
  form.set("model", model);
  form.set("file", new Blob(["fixture audio"], { type: "audio/wav" }), "fixture.wav");
  return new Request(`http://localhost/api/v1/audio/${operation}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${bearer}` },
    body: form,
  });
}

async function assertSuccess(response: Response) {
  assert.equal(response.status, 200, await response.clone().text());
  await response.arrayBuffer();
}

function assertNoCredentialHeaders() {
  for (const call of calls) {
    assert.ok(
      call.headers["content-type"],
      "observe real request headers before asserting absence"
    );
    assert.equal(call.headers.authorization, undefined);
    assert.equal(call.headers["x-api-key"], undefined);
    assert.ok(!JSON.stringify(call).includes("must-not-send"));
  }
}

test("eligible combo pin A wins over higher-priority B and binds A's root", async () => {
  const f = await fixture();
  const ids = [String(f.a.id), String(f.b.id)];
  await assertSuccess(await speech.POST(speechRequest(await combo(f, ids), await key(ids)), null));
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/selected-A/v1/audio/speech"]
  );
  assertNoCredentialHeaders();
});

test("cooling A releases to permitted B and binds the actual B root", async () => {
  const f = await fixture();
  await providers.updateProviderConnection(String(f.a.id), {
    rateLimitedUntil: new Date(Date.now() + 60_000).toISOString(),
  });
  const ids = [String(f.a.id), String(f.b.id)];
  await assertSuccess(await speech.POST(speechRequest(await combo(f, ids), await key(ids)), null));
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/selected-B/v1/audio/speech"]
  );
  assertNoCredentialHeaders();
});

test("deactivated concrete A cannot become a noauth default", async () => {
  const f = await fixture();
  const ids = [String(f.a.id), "noauth"];
  const model = await combo(f, ids);
  await updateConnection(String(f.a.id), { isActive: false });
  const response = await speech.POST(speechRequest(model, await key(ids)), null);
  assert.ok(response.status >= 400, await response.clone().text());
  assert.deepEqual(calls, []);
  const disabled = await providers.getProviderConnectionById(String(f.a.id));
  assert.equal(disabled?.isActive, false);
  assert.ok(disabled?.providerSpecificData?.operatorDisabledAt);
});

test("unmarked recoverable inactive A keeps the supported explicit probe", async () => {
  const f = await fixture();
  // Domain write models an automatic inactive state, not an operator toggle.
  await providers.updateProviderConnection(String(f.a.id), { isActive: false });
  const ids = [String(f.a.id), String(f.b.id)];
  await assertSuccess(await speech.POST(speechRequest(await combo(f, ids), await key(ids)), null));
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/selected-A/v1/audio/speech"]
  );
  assertNoCredentialHeaders();
});

test("deleted concrete A cannot release to an active sibling", async () => {
  const f = await fixture();
  const ids = [String(f.a.id), String(f.b.id)];
  await deletion.deleteProviderConnection(String(f.a.id));
  // Deletion cleans existing combo references. Persist a stale target afterwards
  // so this request actually presents the missing pin to the selector.
  const model = await combo(f, ids);
  const response = await speech.POST(speechRequest(model, await key(ids)), null);
  assert.ok(response.status >= 400, await response.clone().text());
  assert.deepEqual(calls, []);
});

test("eligible A is not bypassed by an explicit noauth pool member", async () => {
  const f = await fixture();
  const ids = [String(f.a.id), "noauth"];
  await assertSuccess(await speech.POST(speechRequest(await combo(f, ids), await key(ids)), null));
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/selected-A/v1/audio/speech"]
  );
  assertNoCredentialHeaders();
});

test("pin-only cooling A cannot widen to B", async () => {
  const f = await fixture();
  const ids = [String(f.a.id)];
  await providers.updateProviderConnection(String(f.a.id), {
    rateLimitedUntil: new Date(Date.now() + 60_000).toISOString(),
  });
  const response = await speech.POST(speechRequest(await combo(f, ids), await key(ids)), null);
  assert.equal(response.status, 429);
  assert.deepEqual(calls, []);
});

for (const allowed of [undefined, ["noauth"]]) {
  test(`unforced ${allowed ? "explicit noauth" : "unrestricted"} keeps the node default`, async () => {
    const f = await fixture();
    await assertSuccess(
      await speech.POST(speechRequest(`${f.node.prefix}/fixture-voice`, await key(allowed)), null)
    );
    assert.deepEqual(
      calls.map((call) => call.path),
      ["/default/v1/audio/speech"]
    );
    assertNoCredentialHeaders();
  });
}

test("an explicit synthetic combo pin keeps the admitted default", async () => {
  const f = await fixture();
  const ids = ["noauth"];
  await assertSuccess(
    await speech.POST(speechRequest(await combo(f, ids, "noauth"), await key(ids)), null)
  );
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/default/v1/audio/speech"]
  );
  assertNoCredentialHeaders();
});

test("requested model reaches the actual pinned selection even with noauth in the pool", async () => {
  const f = await fixture();
  await updateConnection(String(f.a.id), {
    providerSpecificData: { excludedModels: ["fixture-voice"] },
  });
  const ids = [String(f.a.id), "noauth"];
  const response = await speech.POST(speechRequest(await combo(f, ids), await key(ids)), null);
  assert.ok(response.status >= 400, await response.clone().text());
  assert.deepEqual(calls, []);
});

for (const [condition, status] of [
  ["cooling", 429],
  ["expired", 401],
  ["scope", 403],
] as const) {
  test(`${condition} selection refuses before the actual audio transport`, async () => {
    const f = await fixture();
    let ids = [String(f.a.id)];
    if (condition === "cooling") {
      await providers.updateProviderConnection(String(f.a.id), {
        rateLimitedUntil: new Date(Date.now() + 60_000).toISOString(),
      });
    } else if (condition === "expired") {
      await providers.updateProviderConnection(String(f.a.id), { testStatus: "expired" });
    } else {
      ids = [String((await fixture()).a.id)];
    }
    const response = await speech.POST(
      speechRequest(`${f.node.prefix}/fixture-voice`, await key(ids)),
      null
    );
    assert.equal(response.status, status, await response.clone().text());
    assert.deepEqual(calls, []);
  });
}

test("selected row URL keeps its prefix and query for node-ID addressing", async () => {
  const f = await fixture();
  await updateConnection(String(f.a.id), {
    providerSpecificData: { baseUrl: "http://127.0.0.1:19091/prefix/v1/?fixture=1#discard" },
  });
  await assertSuccess(
    await speech.POST(
      speechRequest(`${f.node.id}/fixture-voice`, await key([String(f.a.id)])),
      null
    )
  );
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/prefix/v1/audio/speech?fixture=1"]
  );
  assertNoCredentialHeaders();
});

test("selected local origin replaces the cached node origin in the actual handler", async () => {
  const f = await fixture();
  await updateConnection(String(f.a.id), {
    providerSpecificData: { baseUrl: "http://127.0.0.1:19092/alternate/v1" },
  });
  await assertSuccess(
    await speech.POST(
      speechRequest(`${f.node.prefix}/fixture-voice`, await key([String(f.a.id)])),
      null
    )
  );
  assert.deepEqual(
    calls.map((call) => call.path),
    ["host-19092:/alternate/v1/audio/speech"]
  );
  assertNoCredentialHeaders();
});

test("request-local A/B configs never replace the cached node default", async () => {
  const f = await fixture();
  await Promise.all(
    [String(f.a.id), String(f.b.id)].map(async (id) => {
      await assertSuccess(
        await speech.POST(speechRequest(`${f.node.prefix}/fixture-voice`, await key([id])), null)
      );
    })
  );
  await assertSuccess(
    await speech.POST(speechRequest(`${f.node.prefix}/fixture-voice`, await key()), null)
  );
  assert.deepEqual(calls.map((call) => call.path).sort(), [
    "/default/v1/audio/speech",
    "/selected-A/v1/audio/speech",
    "/selected-B/v1/audio/speech",
  ]);
  assert.equal((await nodes.getProviderNodeById(String(f.node.id)))?.baseUrl, f.node.baseUrl);
});

for (const provider of ["qwen", "coqui", "tortoise"]) {
  test(`actual ${provider} handler uses the selected complete format endpoint without auth`, async () => {
    // Static legacy-provider rows are repository fixtures, not a claimed management creation API.
    const endpoint = provider === "qwen" ? "/qwen/v1/audio/speech" : `/${provider}/api/tts`;
    const row = await providers.createProviderConnection({
      provider,
      authType: "apikey",
      name: `static-${randomUUID()}`,
      apiKey: `must-not-send-${provider}`,
      isActive: true,
      testStatus: "active",
      providerSpecificData: { baseUrl: `http://127.0.0.1:19091${endpoint}` },
    });
    const model = provider === "qwen" ? "qwen/qwen3-tts" : `${provider}/fixture-voice`;
    await assertSuccess(await speech.POST(speechRequest(model, await key([String(row.id)])), null));
    assert.deepEqual(
      calls.map((call) => call.path),
      [endpoint]
    );
    const body = JSON.parse(calls[0].body) as Record<string, unknown>;
    if (provider === "coqui")
      assert.deepEqual(body, { text: "fixture words", speaker_id: "fixture" });
    else if (provider === "tortoise")
      assert.deepEqual(body, { text: "fixture words", voice: "fixture" });
    else assert.equal(body.model, "qwen3-tts");
    assertNoCredentialHeaders();
  });
}

test("fixed gtts default works, but a concrete row never authorizes fake host binding", async () => {
  await assertSuccess(
    await speech.POST(speechRequest("gtts/default", await key(["noauth"])), null)
  );
  assert.equal(calls.length, 1);
  assert.match(calls[0].path, /^gtts:/);
  assertNoCredentialHeaders();
  calls.length = 0;
  const row = await providers.createProviderConnection({
    provider: "gtts",
    authType: "apikey",
    name: `gtts-${randomUUID()}`,
    apiKey: "must-not-send-gtts",
    isActive: true,
    testStatus: "active",
    providerSpecificData: { baseUrl: "http://127.0.0.1:19091/fake-gtts" },
  });
  const response = await speech.POST(
    speechRequest("gtts/default", await key([String(row.id)])),
    null
  );
  assert.equal(response.status, 400, await response.clone().text());
  assert.deepEqual(calls, []);
});

for (const baseUrl of [
  "https://example.com/v1",
  "http://user:secret@127.0.0.1:19091/v1",
  "http://127.0.0.1:19091/v1/audio/speech",
]) {
  test(`actual row override ${baseUrl} refuses without any default send`, async () => {
    const f = await fixture();
    await updateConnection(String(f.a.id), { providerSpecificData: { baseUrl } });
    const response = await speech.POST(
      speechRequest(`${f.node.prefix}/fixture-voice`, await key([String(f.a.id)])),
      null
    );
    assert.equal(response.status, 400, await response.clone().text());
    assert.deepEqual(calls, []);
    assert.ok(!(await response.clone().text()).includes("secret"));
  });
}

for (const operation of ["speech", "transcriptions", "translations"] as const) {
  test(`direct ${operation} retains a scoped local row and its actual transport root`, async () => {
    const f = await fixture();
    const bearer = await key([String(f.a.id)]);
    const model = `${f.node.prefix}/fixture-voice`;
    let response: Response;
    if (operation === "speech") {
      response = await speech.POST(speechRequest(model, bearer), null);
    } else {
      response = await (operation === "transcriptions" ? transcription : translation).POST(
        multipartRequest(model, bearer, operation)
      );
    }
    await assertSuccess(response);
    assert.deepEqual(
      calls.map((call) => call.path),
      [`/selected-A/v1/audio/${operation}`]
    );
    assertNoCredentialHeaders();
    await logs.waitForCallLogSaves(2_000);
    const saved = await logs.getCallLogs({ provider: String(f.node.prefix) });
    assert.equal(saved[0]?.connectionId, f.a.id, "actual route attribution belongs to selected A");
  });
}

for (const operation of ["transcriptions", "translations"] as const) {
  for (const state of ["eligible", "cooling", "missing"] as const) {
    test(`${operation} combo preserves its ${state} pin contract`, async () => {
      const f = await fixture();
      const ids = [String(f.a.id), String(f.b.id)];
      if (state === "cooling") {
        await providers.updateProviderConnection(String(f.a.id), {
          rateLimitedUntil: new Date(Date.now() + 60_000).toISOString(),
        });
      } else if (state === "missing") {
        await deletion.deleteProviderConnection(String(f.a.id));
      }
      const response = await (operation === "transcriptions" ? transcription : translation).POST(
        multipartRequest(await combo(f, ids), await key(ids), operation)
      );
      if (state === "missing") {
        assert.ok(response.status >= 400, await response.clone().text());
        assert.deepEqual(calls, []);
      } else if (state === "cooling") {
        // Generic combo gates skip a persisted cooldown before the audio callback.
        // SpeechCombo has no such gate and separately proves selector A -> B release.
        assert.equal(response.status, 429);
        const body = (await response.json()) as { error: { code: string } };
        assert.equal(body.error.code, "ALL_TARGETS_SKIPPED");
        assert.deepEqual(calls, []);
      } else {
        await assertSuccess(response);
        assert.deepEqual(
          calls.map((call) => call.path),
          [`/selected-A/v1/audio/${operation}`]
        );
        assertNoCredentialHeaders();
      }
    });
  }
}
