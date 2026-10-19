import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { registerHooks } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const data = fs.mkdtempSync(path.join(os.tmpdir(), "omni-concurrency429-"));
process.env.DATA_DIR = data;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.APP_LOG_TO_FILE = "false";
const { concurrencyPorts } = await import("../helpers/concurrency429ProviderPorts.ts");
const authUrl = new URL("../../src/sse/services/auth.ts", import.meta.url).href;
const fixtureUrl = new URL("../helpers/concurrency429ProviderPorts.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL?.split("?")[0] === authUrl && specifier === "@/lib/db/providers") {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return next(specifier, context);
  },
});
const core = await import("../../src/lib/db/core.ts");
const providers = await import("../../src/lib/db/providers.ts");
const auth = await import(authUrl);
const fallback = await import("../../open-sse/services/accountFallback.ts");
const { runAsProbe } = await import("../../src/shared/utils/probeOrigin.ts");
const { getCodexChildCooldown } = await import("../../open-sse/services/codexAccount/index.ts");
const profile = { ...fallback.getProviderProfile("openai"), useUpstreamRetryHints: false };
const message = "Too many concurrent requests";

async function connection(provider = "openai", psd: Record<string, unknown> = {}) {
  const identity = randomUUID();
  const row = await providers.createProviderConnection({
    provider,
    authType: "apikey",
    name: `concurrency-${identity}`,
    apiKey: `fixture-${identity}`,
    isActive: true,
    testStatus: "active",
    backoffLevel: 2,
    providerSpecificData: psd,
  });
  assert.equal(typeof row.id, "string");
  await providers.updateProviderConnection(String(row.id), { backoffLevel: 2 });
  return String(row.id);
}

test.beforeEach(() => {
  fallback.clearAllModelLockouts();
  concurrencyPorts.afterRead = null;
  concurrencyPorts.patches.length = 0;
});
test.after(async () => {
  hooks.deregister();
  await (await import("../../src/lib/usage/callLogs.ts")).closeCallLogSaves(5_000);
  core.resetDbInstance();
  fs.rmSync(data, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("actual auth omits stale backoff after another writer commits N+1", async () => {
  const id = await connection();
  concurrencyPorts.afterRead = async () => {
    await providers.updateProviderConnection(id, { backoffLevel: 7 });
  };
  const result = await auth.markAccountUnavailable(id, 429, message, "openai", "gpt-4o", profile);
  assert.ok(concurrencyPorts.patches.length > 0, "actual auth provider port must be observed");
  const row = await providers.getProviderConnectionById(id);
  assert.equal(row?.backoffLevel, 7);
  assert.ok(concurrencyPorts.patches.every((patch) => !Object.hasOwn(patch, "backoffLevel")));
  assert.equal(result.cooldownMs, 1_000);
  assert.equal(row?.lastErrorType, "concurrency_limited");
});

test("actual auth uses selected fixed override without enabling account rotation thresholds", async () => {
  const id = await connection("openai", {
    rotationOverrides: { rateLimitResetSeconds: 7, rotateOn429: false, error429Threshold: 99 },
  });
  const result = await auth.markAccountUnavailable(id, 429, message, "openai", "gpt-4o", profile);
  assert.equal(result.shouldFallback, true);
  assert.equal(result.cooldownMs, 7_000);
  const row = await providers.getProviderConnectionById(id);
  assert.equal(row?.backoffLevel, 2);
  assert.ok(Date.parse(String(row?.rateLimitedUntil)) > Date.now() + 6_000);
});

test("actual auth enabled positive hint outranks selected fixed override", async () => {
  const id = await connection("openai", { rotationOverrides: { rateLimitResetSeconds: 7 } });
  const result = await auth.markAccountUnavailable(
    id,
    429,
    message,
    "openai",
    "gpt-4o",
    { ...profile, useUpstreamRetryHints: true },
    { headers: new Headers({ "retry-after": "2" }) }
  );
  assert.equal(result.cooldownMs, 2_000);
  assert.equal((await providers.getProviderConnectionById(id))?.backoffLevel, 2);
});

test("actual auth honors exhausted quota headers before concurrency wording", async () => {
  const id = await connection("opencode");
  await auth.markAccountUnavailable(id, 429, message, "opencode", "test-model", profile, {
    headers: new Headers({ "x-ratelimit-remaining-requests": "0" }),
  });
  const row = await providers.getProviderConnectionById(id);
  assert.equal(row?.lastErrorType, "quota_exhausted");
  assert.equal(row?.testStatus, "unavailable");
  assert.equal(fallback.getModelLockoutInfo("opencode", id, "test-model"), null);
});

test("actual auth concurrency never enters per-model exponential failure accounting", async () => {
  const id = await connection("gemini");
  for (let attempt = 0; attempt < 3; attempt++) {
    const result = await auth.markAccountUnavailable(
      id,
      429,
      message,
      "gemini",
      "gemini-2.5-pro",
      profile
    );
    assert.equal(result.cooldownMs, 1_000);
    assert.equal(fallback.getModelLockoutInfo("gemini", id, "gemini-2.5-pro"), null);
    await providers.updateProviderConnection(id, { rateLimitedUntil: null, testStatus: "active" });
  }
});

test("probe, cooling opt-out, and terminal state retain their owner contracts", async () => {
  const probe = await connection();
  await runAsProbe(() =>
    auth.markAccountUnavailable(probe, 429, message, "openai", "gpt-4o", profile)
  );
  const probeRow = await providers.getProviderConnectionById(probe);
  assert.equal(probeRow?.testStatus, "active");
  assert.equal(probeRow?.rateLimitedUntil, undefined);
  assert.equal(probeRow?.backoffLevel, 2);
  assert.equal(probeRow?.lastErrorType, "concurrency_limited");

  const optedOut = await connection("openai", { disableCooling: true });
  await auth.markAccountUnavailable(optedOut, 429, message, "openai", "gpt-4o", profile);
  assert.equal((await providers.getProviderConnectionById(optedOut))?.rateLimitedUntil, undefined);

  const terminal = await connection();
  await providers.updateProviderConnection(terminal, { testStatus: "banned" });
  await auth.markAccountUnavailable(terminal, 429, message, "openai", "gpt-4o", profile);
  assert.equal((await providers.getProviderConnectionById(terminal))?.testStatus, "banned");
});

test("Codex child scopes stay independent and model-less concurrency creates no parent cooldown", async () => {
  const id = await connection("codex");
  await auth.markAccountUnavailable(id, 429, message, "codex", "gpt-6.1-sol", profile);
  let row = await providers.getProviderConnectionById(id);
  assert.ok(getCodexChildCooldown(row, "gpt-6.1-sol"));
  assert.equal(getCodexChildCooldown(row, "gpt-6.1-sol-spark"), null);
  assert.equal(row?.rateLimitedUntil, undefined);
  assert.equal(row?.testStatus, "active");
  await auth.markAccountUnavailable(id, 429, message, "codex", "gpt-6.1-sol-spark", profile);
  row = await providers.getProviderConnectionById(id);
  assert.ok(getCodexChildCooldown(row, "gpt-6.1-sol-spark"));
  await auth.markAccountUnavailable(id, 429, message, "codex", null, profile);
  assert.equal((await providers.getProviderConnectionById(id))?.rateLimitedUntil, undefined);
});

test("ordinary quota and 503 still use their existing longer cooldown/backoff", async () => {
  const quota = await connection("gemini");
  await auth.markAccountUnavailable(
    quota,
    429,
    "Daily quota exceeded",
    "gemini",
    "gemini-2.5-pro",
    profile
  );
  assert.equal(
    fallback.getModelLockoutInfo("gemini", quota, "gemini-2.5-pro")?.reason,
    "quota_exhausted"
  );
  const server = await connection();
  await auth.markAccountUnavailable(
    server,
    503,
    "temporarily unavailable",
    "openai",
    "gpt-4o",
    profile
  );
  assert.equal((await providers.getProviderConnectionById(server))?.backoffLevel, 3);
});
