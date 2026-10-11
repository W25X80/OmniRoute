import assert from "node:assert/strict";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import test from "node:test";

import { preparePostgresServingFileCaches } from "../../src/lib/startup/nativeFileCaches.ts";
import { markServerReady, markServerStarting } from "../../src/lib/serverLifecycle.ts";
import * as providers from "../../open-sse/config/constants.ts";
import * as lifecycle from "../../open-sse/services/modelLifecycle.ts";
import * as fitness from "../../open-sse/services/autoCombo/taskFitness.ts";
import * as alibaba from "../../open-sse/services/alibabaFreeTierAllowlist.ts";
import { loadProviderCredentials } from "../../open-sse/config/credentialLoader.ts";

const STATE_KEYS = [
  "omniroute.nativePreparedProviders",
  "omniroute.nativeModelLifecycleSnapshot",
  "omniroute.nativeFitnessRetiredModels",
  "omniroute.nativeAlibabaFreeTierAllowlist",
];
const savedEnvironment = {
  OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
  DATA_DIR: process.env.DATA_DIR,
  ALIBABA_FREE_TIER_ALLOWLIST_PATH: process.env.ALIBABA_FREE_TIER_ALLOWLIST_PATH,
};
const readFile = fsPromises.readFile.bind(fsPromises);
const initialClock = Date.now();
let caseIndex = 0;

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

function noSynchronousFileCalls() {
  let count = 0;
  const methods = ["existsSync", "statSync", "readFileSync", "mkdirSync", "writeFileSync"] as const;
  for (const method of methods) {
    const original = fs[method];
    test.mock.method(fs, method, (...args: unknown[]) => {
      count++;
      return Reflect.apply(original, fs, args);
    });
  }
  syncBuiltinESMExports();
  return () => count;
}

test.beforeEach(() => {
  const caseClock = initialClock + ++caseIndex * 60001;
  test.mock.method(Date, "now", () => caseClock);
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.DATA_DIR = "/nonexistent-native-serving-cache-test";
  process.env.ALIBABA_FREE_TIER_ALLOWLIST_PATH = "/native-cache-fixture/alibaba.json";
  markServerStarting();
  for (const key of STATE_KEYS) Reflect.deleteProperty(globalThis, Symbol.for(key));
});

test.afterEach(() => {
  test.mock.restoreAll();
  syncBuiltinESMExports();
  for (const [key, value] of Object.entries(savedEnvironment)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  markServerStarting();
  for (const key of STATE_KEYS) Reflect.deleteProperty(globalThis, Symbol.for(key));
});

test("native barrier awaits all four inputs and preserves pending provider mutations", async () => {
  const gate = deferred();
  const reading = deferred();
  const reads: string[] = [];
  test.mock.method(fsPromises, "access", async () => undefined);
  test.mock.method(fsPromises, "readFile", async (file: unknown) => {
    const filename = String(file);
    reads.push(filename);
    if (reads.length === 4) reading.release();
    await gate.promise;
    if (filename.endsWith("provider-credentials.json")) {
      return JSON.stringify({
        codex: { clientId: "native-test-client", forbiddenField: "ignored" },
      });
    }
    if (filename.endsWith("model-lifecycle.json")) {
      return JSON.stringify({
        retired: { "native-retired-model": { status: "retired", vendor: "native" } },
      });
    }
    return JSON.stringify({
      asOf: "2026-01-01",
      capable: ["native-free-model"],
      noFreeTier: ["native-paid-model"],
    });
  });
  const syncCalls = noSynchronousFileCalls();
  let finished = false;
  const preparation = preparePostgresServingFileCaches().then(() => {
    finished = true;
  });
  await reading.promise;
  const alias = {
    id: "pending-test-provider",
    format: "openai",
  } as unknown as typeof providers.PROVIDERS.openai;
  providers.PROVIDERS["pending-test-provider"] = alias;
  delete providers.PROVIDERS.openai;
  let ticked = false;
  await new Promise<void>((resolve) =>
    setImmediate(() => {
      ticked = true;
      resolve();
    })
  );
  assert.equal(ticked, true);
  assert.equal(
    finished,
    false,
    "readiness predecessor cannot complete before each file input resolves"
  );
  gate.release();
  await preparation;
  markServerReady();
  assert.equal(providers.PROVIDERS.codex.clientId, "native-test-client");
  assert.equal(Reflect.has(providers.PROVIDERS.codex, "forbiddenField"), false);
  assert.equal(providers.PROVIDERS["pending-test-provider"], alias);
  assert.equal("openai" in providers.PROVIDERS, false);
  assert.equal(
    lifecycle.getModelLifecycleDecision("native", "native-retired-model").action,
    "reject"
  );
  assert.equal(fitness.getModelsDevTierFitnessWithSource("native-retired-model", "coding"), null);
  assert.deepEqual(alibaba.getAlibabaBuiltinFreeTierTextCapableModels(), ["native-free-model"]);
  assert.deepEqual(alibaba.getAlibabaBuiltinNoFreeTierTextModels(), ["native-paid-model"]);
  const priorReads = reads.length;
  await preparePostgresServingFileCaches();
  assert.equal(reads.length, priorReads, "the prepared barrier is idempotent");
  assert.equal(syncCalls(), 0);
});

test("a different provider module instance consumes the shared prepared map", async () => {
  test.mock.method(fsPromises, "access", async () => undefined);
  test.mock.method(fsPromises, "readFile", async (file: unknown) => {
    if (String(file).endsWith("provider-credentials.json"))
      return JSON.stringify({ codex: { clientId: "shared-client" } });
    throw Object.assign(new Error("fixture missing"), { code: "ENOENT" });
  });
  await preparePostgresServingFileCaches();
  markServerReady();
  const duplicate = await import(`../../open-sse/config/constants.ts?nativeBundle=${Date.now()}`);
  assert.notEqual(
    duplicate.PROVIDERS,
    providers.PROVIDERS,
    "the oracle must create a distinct Proxy instance"
  );
  const syncCalls = noSynchronousFileCalls();
  assert.equal(duplicate.PROVIDERS.codex, providers.PROVIDERS.codex);
  assert.equal(duplicate.PROVIDERS.codex.clientId, "shared-client");
  const value = { id: "shared-provider" } as unknown as typeof providers.PROVIDERS.openai;
  duplicate.PROVIDERS["shared-provider"] = value;
  assert.equal(providers.PROVIDERS["shared-provider"], value);
  delete duplicate.PROVIDERS["shared-provider"];
  assert.equal("shared-provider" in providers.PROVIDERS, false);
  assert.equal(syncCalls(), 0);
});

test("ready native getters reject an unprepared cold module instead of synchronous I/O", () => {
  markServerReady();
  const syncCalls = noSynchronousFileCalls();
  assert.throws(() => providers.PROVIDERS.codex, /not prepared/);
  assert.throws(() => lifecycle.isVendorRetiredId("native-unknown-model"), /not prepared/);
  assert.throws(
    () => fitness.getModelsDevTierFitnessWithSource("native-unknown-model", "coding"),
    /not prepared/
  );
  assert.throws(() => alibaba.getAlibabaBuiltinFreeTierTextCapableModels(), /not prepared/);
  assert.throws(() => loadProviderCredentials({}), /asynchronous preparation/);
  assert.equal(syncCalls(), 0);
});

test("missing and corrupt native lifecycle inputs keep neutral fallback and embedded allowlist", async () => {
  test.mock.method(fsPromises, "access", async () => {
    throw Object.assign(new Error("missing"), { code: "ENOENT" });
  });
  test.mock.method(fsPromises, "readFile", async (file: unknown) => {
    if (String(file).endsWith("model-lifecycle.json")) return "{broken-json";
    throw Object.assign(new Error("missing"), { code: "ENOENT" });
  });
  const syncCalls = noSynchronousFileCalls();
  await preparePostgresServingFileCaches();
  markServerReady();
  assert.equal(lifecycle.isVendorRetiredId("native-unknown-model"), false);
  assert.equal(
    lifecycle.getModelLifecycleDecision("openai", "gpt-5.2-codex").action,
    "reject",
    "dated source record remains active"
  );
  assert.deepEqual(alibaba.getAlibabaBuiltinFreeTierTextCapableModels(), [
    ...alibaba.ALIBABA_FREE_TIER_TEXT_CAPABLE_MODELS,
  ]);
  assert.ok(providers.PROVIDERS.codex);
  assert.equal(syncCalls(), 0);
});

test("Alibaba native override priority, expired candidate fallback and sticky cache match source", async () => {
  const requests: string[] = [];
  test.mock.method(fsPromises, "readFile", async (file: unknown) => {
    const filename = String(file);
    requests.push(filename);
    if (filename === process.env.ALIBABA_FREE_TIER_ALLOWLIST_PATH) {
      return JSON.stringify({
        asOf: "2020-01-01",
        validUntil: "2020-01-02",
        capable: ["expired"],
        noFreeTier: [],
      });
    }
    if (
      filename === path.join(process.env.DATA_DIR as string, "alibaba-free-tier-allowlist.json")
    ) {
      return JSON.stringify({
        asOf: "2026-01-01",
        validUntil: "2099-01-01",
        capable: ["data-dir-winner"],
        noFreeTier: [],
      });
    }
    return readFile(file as Parameters<typeof fsPromises.readFile>[0], "utf8");
  });
  await alibaba.prepareNativeAlibabaFreeTierAllowlist();
  markServerReady();
  assert.deepEqual(requests, [
    process.env.ALIBABA_FREE_TIER_ALLOWLIST_PATH,
    path.join(process.env.DATA_DIR as string, "alibaba-free-tier-allowlist.json"),
  ]);
  assert.deepEqual(alibaba.getAlibabaBuiltinFreeTierTextCapableModels(), ["data-dir-winner"]);
  const count = requests.length;
  const pack = alibaba.loadAlibabaFreeTierAllowlistPack();
  assert.ok(pack);
  pack.validUntil = "2000-01-01";
  assert.deepEqual(
    alibaba.getAlibabaBuiltinFreeTierTextCapableModels(),
    ["data-dir-winner"],
    "source validates expiry at first load, not each read"
  );
  assert.equal(requests.length, count);
});

test("reset during native Alibaba preparation cannot publish its obsolete file result", async () => {
  const gate = deferred();
  const reading = deferred();
  test.mock.method(fsPromises, "readFile", async () => {
    reading.release();
    await gate.promise;
    return JSON.stringify({ asOf: "2026-01-01", capable: ["obsolete"], noFreeTier: [] });
  });
  const preparation = alibaba.prepareNativeAlibabaFreeTierAllowlist();
  await reading.promise;
  alibaba.resetAlibabaFreeTierAllowlistCache();
  gate.release();
  await preparation;
  markServerReady();
  assert.throws(() => alibaba.getAlibabaBuiltinFreeTierTextCapableModels(), /not prepared/);
});
