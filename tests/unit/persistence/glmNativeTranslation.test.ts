import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import {
  withPostgresCatalogSnapshot,
  CATALOG_TABLES,
} from "../../../src/lib/db/postgres/catalogSnapshot.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import { translationFixture } from "../../helpers/persistence/glmTranslationFixture.ts";
import type { ExecuteInput, ProviderCredentials } from "../../../open-sse/executors/base.ts";

const executorUrl = new URL("../../../open-sse/executors/glm.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/glmTranslationFixture.ts", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === executorUrl && specifier === "../translator/index.ts")
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const { GlmExecutor } = await import("../../../open-sse/executors/glm.ts");
class CountingGlmExecutor extends GlmExecutor {
  passes = 0;
  transformRequest(
    model: string,
    body: unknown,
    stream: boolean,
    credentials: ProviderCredentials
  ) {
    this.passes++;
    return super.transformRequest(model, body, stream, credentials);
  }
}
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function selectedBinding(assertReady: () => void = () => {}) {
  const database = {
    transaction: async (body: (tx: { query: () => Promise<unknown> }) => Promise<unknown>) =>
      body({
        query: async () => ({
          rows: [
            {
              values: [],
              tables: Object.fromEntries(CATALOG_TABLES.map((table) => [table, []])),
              generation: "1",
              authority: "glm-test-catalog",
            },
          ],
        }),
      }),
  } as unknown as PostgresDatabase;
  return {
    settings: {},
    providers: {},
    nodes: {},
    apiKeys: {},
    keyGroups: {},
    secrets: {},
    database,
    databaseIdentity: {},
    assertReady,
  } as unknown as PostgresControlPlaneBinding;
}
async function native<T>(body: () => T | Promise<T>): Promise<T> {
  const binding = selectedBinding();
  return withPostgresControlPlane(binding, () =>
    withPostgresCatalogSnapshot(binding.database!, body)
  );
}
const credentials = { apiKey: "glm-test-key" };
function input(model = "glm-5.2-high"): ExecuteInput {
  return {
    model,
    body: { messages: [{ role: "user", content: "question" }] },
    stream: false,
    credentials,
  };
}
const originalFetch = globalThis.fetch;
const originalBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
let fetchBodies: Array<Record<string, unknown>> = [];
test.beforeEach(() => {
  translationFixture.reset();
  fetchBodies = [];
  globalThis.fetch = async (_url, options) => {
    fetchBodies.push(JSON.parse(String(options?.body)));
    return new Response("accepted", { headers: { "content-type": "text/plain" } });
  };
});
test.afterEach(() => {
  globalThis.fetch = originalFetch;
});
test.after(() => {
  hooks.deregister();
  if (originalBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = originalBackend;
});

test("legacy GLM transport returns synchronously and processes its payload once", () => {
  const executor = new CountingGlmExecutor("glm");
  const value = executor.transformForTransport(
    "glm-5.2-high",
    input().body,
    false,
    credentials,
    "anthropic"
  );
  assert.equal(value instanceof Promise, false);
  assert.equal(executor.passes, 1);
  assert.equal(translationFixture.syncCalls.length, 1);
  assert.equal(translationFixture.asyncCalls.length, 0);
  const args = translationFixture.syncCalls[0];
  assert.equal(args[2], "glm-5.2");
  assert.equal(args[5], credentials);
  assert.deepEqual(args[8], { preserveCacheControl: false });
  assert.equal((args[3] as Record<string, unknown>)._disableToolPrefix, true);
  assert.deepEqual((value as Record<string, unknown>).thinking, {
    type: "enabled",
    budget_tokens: 1234,
  });
  assert.equal((value as Record<string, unknown>).effort, "high");
});

test("Async GLM translation waits at one shared payload boundary before effort normalization", async () => {
  const executor = new CountingGlmExecutor("glm");
  const entered = barrier(),
    finished = barrier();
  translationFixture.entered = entered.resolve;
  translationFixture.pending = finished.promise;
  const pending = executor.transformForTransportAsync(
    "glm-5.2-max",
    input().body,
    false,
    credentials,
    "anthropic"
  );
  try {
    await entered.promise;
    assert.equal(executor.passes, 1);
    assert.equal(translationFixture.asyncCalls.length, 1);
    assert.equal(translationFixture.syncCalls.length, 0);
    finished.resolve();
    const value = (await pending) as Record<string, unknown>;
    assert.equal(value.effort, "max");
    assert.equal(value.max_tokens, 131072);
    assert.deepEqual(value.thinking, { type: "enabled", budget_tokens: 1234 });
    assert.equal(executor.passes, 1);
  } finally {
    finished.resolve();
    await pending.catch(() => {});
  }
});

test("both GLM OpenAI transform APIs skip translation and preserve tool streaming and effort", async () => {
  const executor = new CountingGlmExecutor("glm");
  const body = {
    ...(input().body as Record<string, unknown>),
    thinking: { type: "disabled" },
    tools: [{ type: "function", function: { name: "lookup" } }],
  };
  const sync = executor.transformForTransport(
    "glm-5.3-low",
    structuredClone(body),
    true,
    credentials,
    "openai"
  );
  const async = await executor.transformForTransportAsync(
    "glm-5.3-low",
    structuredClone(body),
    true,
    credentials,
    "openai"
  );
  assert.deepEqual(async, sync);
  assert.equal(executor.passes, 2);
  assert.equal(translationFixture.syncCalls.length, 0);
  assert.equal(translationFixture.asyncCalls.length, 0);
  assert.equal((sync as Record<string, unknown>).tool_stream, true);
  assert.equal((sync as Record<string, unknown>).reasoning_effort, "low");
  assert.equal((sync as Record<string, unknown>).model, "glm-5.3");
  assert.deepEqual((sync as Record<string, unknown>).thinking, { type: "enabled" });
});

test("selected native GLM execute hydrates before dispatch even with default SQLite environment", async () => {
  const executor = new CountingGlmExecutor("glm");
  const entered = barrier(),
    finished = barrier();
  translationFixture.entered = entered.resolve;
  translationFixture.pending = finished.promise;
  const pending = native(() => executor.execute(input()));
  try {
    await entered.promise;
    assert.equal(fetchBodies.length, 0);
    assert.equal(executor.passes, 1);
    finished.resolve();
    const result = await pending;
    assert.equal(result.response.status, 200);
    assert.equal(fetchBodies.length, 1);
    assert.equal(fetchBodies[0].translated, true);
    assert.equal(fetchBodies[0].effort, "high");
    assert.equal(translationFixture.syncCalls.length, 0);
    assert.equal(translationFixture.asyncCalls.length, 1);
    assert.equal(executor.passes, 1);
  } finally {
    finished.resolve();
    await pending.catch(() => {});
  }
});

test("native translation failure cannot dispatch a partially prepared GLM request", async () => {
  const executor = new CountingGlmExecutor("glm");
  translationFixture.failure = new Error("controlled translation failure");
  await assert.rejects(
    native(() => executor.execute(input())),
    /controlled translation failure/
  );
  assert.equal(fetchBodies.length, 0);
  assert.equal(translationFixture.syncCalls.length, 0);
  assert.equal(translationFixture.asyncCalls.length, 1);
  assert.equal(executor.passes, 1);
});

test("default SQLite GLM execute retains its synchronous translation call", async () => {
  const executor = new CountingGlmExecutor("glm");
  translationFixture.pending = new Promise<void>(() => {});
  const pending = executor.execute(input());
  assert.equal(fetchBodies.length, 1);
  await pending;
  assert.equal(translationFixture.syncCalls.length, 1);
  assert.equal(translationFixture.asyncCalls.length, 0);
  assert.equal(executor.passes, 1);
});

test("revoked native authority rejects GLM dispatch without SQLite fallback", async () => {
  const executor = new CountingGlmExecutor("glm");
  let ready = true;
  await withPostgresControlPlane(
    selectedBinding(() => {
      if (!ready) throw new Error("controlled scope ended");
    }),
    async () => {
      ready = false;
      await assert.rejects(executor.execute(input()), /controlled scope ended/);
    }
  );
  assert.equal(fetchBodies.length, 0);
  assert.equal(executor.passes, 0);
  assert.equal(translationFixture.syncCalls.length, 0);
  assert.equal(translationFixture.asyncCalls.length, 0);
});
