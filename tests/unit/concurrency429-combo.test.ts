import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { registerHooks } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const data = fs.mkdtempSync(path.join(os.tmpdir(), "omni-concurrency-combo-"));
process.env.DATA_DIR = data;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.APP_LOG_TO_FILE = "false";
const attemptUrl = new URL("../../open-sse/services/combo/executeTargetAttempt.ts", import.meta.url)
  .href;
const fixtureUrl = new URL("../helpers/concurrency429FallbackPorts.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL?.split("?")[0] === attemptUrl && specifier === "../accountFallback.ts") {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return next(specifier, context);
  },
});
const { concurrencyFallbackCalls } = await import("../helpers/concurrency429FallbackPorts.ts");
const { handleComboChat } = await import("../../open-sse/services/combo.ts");
const fallback = await import("../../open-sse/services/accountFallback.ts");
const cooldown = await import("../../open-sse/services/providerCooldownTracker.ts");
const providers = await import("../../src/lib/db/providers.ts");
const settingsDb = await import("../../src/lib/db/settings.ts");
const { invalidateDbCache } = await import("../../src/lib/db/readCache.ts");
const { resetAllCircuitBreakers } = await import("../../src/shared/utils/circuitBreaker.ts");
const core = await import("../../src/lib/db/core.ts");
const log = { info() {}, warn() {}, debug() {}, error() {} };
const message = "Too many concurrent requests";

test.beforeEach(() => {
  concurrencyFallbackCalls.length = 0;
  fallback.clearAllModelLockouts();
  cooldown.clearCooldownState();
  resetAllCircuitBreakers();
});
test.after(async () => {
  hooks.deregister();
  await (await import("../../src/lib/usage/callLogs.ts")).closeCallLogSaves(5_000);
  core.resetDbInstance();
  fs.rmSync(data, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

async function seed() {
  const id = randomUUID();
  const row = await providers.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: id,
    apiKey: `fixture-${id}`,
    isActive: true,
    testStatus: "active",
    providerSpecificData: { rotationOverrides: { rateLimitResetSeconds: 7, rotateOn429: false } },
  });
  return String(row.id);
}

async function dispatch(
  id: string,
  options: {
    retries?: number;
    status?: number;
    text?: string;
    hints?: boolean;
    pin?: boolean;
    retryAfter?: string;
  } = {}
) {
  await settingsDb.updateSettings({
    resilienceSettings: {
      connectionCooldown: { apikey: { useUpstreamRetryHints: options.hints === true } },
    },
  });
  invalidateDbCache("settings");
  const targets: unknown[] = [];
  let attempts = 0;
  const response = await handleComboChat({
    body: { model: "concurrency-combo", messages: [{ role: "user", content: "hi" }] },
    combo: {
      name: `concurrency-${randomUUID()}`,
      strategy: "priority",
      models: [
        { model: "openai/gpt-4o", ...(options.pin ? { connectionId: id } : {}) },
        { model: "claude/sonnet" },
      ],
      config: {
        maxRetries: options.retries ?? 0,
        maxSetRetries: 0,
        retryDelayMs: 0,
        fallbackDelayMs: 0,
      },
    },
    settings: {
      modelLockout: { enabled: true, baseCooldownMs: 10_000 },
      resilienceSettings: {
        providerCooldown: { enabled: true },
        connectionCooldown: { apikey: { useUpstreamRetryHints: options.hints === true } },
      },
    },
    handleSingleModel: async (_body, model, target) => {
      targets.push(target);
      if (model.startsWith("openai/")) {
        attempts++;
        return new Response(JSON.stringify({ error: { message: options.text ?? message } }), {
          status: options.status ?? 429,
          headers: {
            "content-type": "application/json",
            "x-omniroute-selected-connection-id": id,
            ...(options.retryAfter ? { "retry-after": options.retryAfter } : {}),
          },
        });
      }
      return new Response(JSON.stringify({ choices: [{ message: { content: "healthy" } }] }), {
        headers: { "content-type": "application/json" },
      });
    },
    log,
    allCombos: [],
  });
  return { response, attempts, targets };
}

test("actual combo retry/final paths leave opt-in model and provider counters unchanged", async () => {
  const id = await seed();
  for (const retries of [0, 1, 0]) {
    const result = await dispatch(id, { retries });
    assert.equal(result.response.status, 200);
    assert.equal(result.attempts, retries + 1);
    assert.equal(fallback.getModelLockoutInfo("openai", id, "gpt-4o"), null);
    assert.equal(cooldown.getCooldownEntryCount(), 0);
  }
});

test("actual combo classifies with selected account fixed override and enabled hint precedence", async () => {
  const id = await seed();
  await dispatch(id, { retryAfter: "2", hints: false });
  assert.equal(concurrencyFallbackCalls.at(-1)?.cooldownMs, 7_000);
  assert.equal(concurrencyFallbackCalls.at(-1)?.reason, "concurrency_limited");
  await dispatch(id, { retryAfter: "2", hints: true });
  assert.equal(concurrencyFallbackCalls.at(-1)?.cooldownMs, 2_000);
  assert.equal(concurrencyFallbackCalls.at(-1)?.usedUpstreamRetryHint, true);
});

test("pin-only target retains its implicit allowlist after concurrency failure", async () => {
  const id = await seed();
  const result = await dispatch(id, { pin: true });
  assert.equal(result.response.status, 200);
  const target = result.targets[0] as { connectionId?: string; allowedConnectionIds?: string[] };
  assert.equal(target.connectionId, id);
  assert.deepEqual(target.allowedConnectionIds, [id]);
  assert.equal(result.attempts, 1);
});

test("ordinary rate limit and 503 still record opt-in model/provider failure state", async () => {
  const id = await seed();
  await dispatch(id, { text: "Too many requests per minute" });
  assert.ok(fallback.getModelLockoutInfo("openai", id, "gpt-4o"));
  assert.ok(cooldown.getCooldownEntryCount() > 0);
  fallback.clearAllModelLockouts();
  cooldown.clearCooldownState();
  await dispatch(id, { status: 503, text: "temporarily unavailable" });
  assert.ok(fallback.getModelLockoutInfo("openai", id, "gpt-4o"));
  assert.ok(cooldown.getCooldownEntryCount() > 0);
});
