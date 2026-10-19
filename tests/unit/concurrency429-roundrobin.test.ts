import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { registerHooks } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const data = fs.mkdtempSync(path.join(os.tmpdir(), "omni-concurrency-rr-"));
process.env.DATA_DIR = data;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.APP_LOG_TO_FILE = "false";
const rrUrl = new URL("../../open-sse/services/combo/roundRobinCombo.ts", import.meta.url).href;
const fixtureUrl = new URL("../helpers/concurrency429FallbackPorts.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL?.split("?")[0] === rrUrl && specifier === "../accountFallback.ts") {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return next(specifier, context);
  },
});
const { concurrencyFallbackCalls } = await import("../helpers/concurrency429FallbackPorts.ts");
const { handleComboChat } = await import("../../open-sse/services/combo.ts");
const fallback = await import("../../open-sse/services/accountFallback.ts");
const cooldown = await import("../../open-sse/services/providerCooldownTracker.ts");
const semaphore = await import("../../open-sse/services/rateLimitSemaphore.ts");
const providers = await import("../../src/lib/db/providers.ts");
const settingsDb = await import("../../src/lib/db/settings.ts");
const { invalidateDbCache } = await import("../../src/lib/db/readCache.ts");
const { resetAllCircuitBreakers } = await import("../../src/shared/utils/circuitBreaker.ts");
const core = await import("../../src/lib/db/core.ts");
const message = "Too many concurrent requests";

test.beforeEach((context) => {
  // RR schedules semaphore recovery even for long quota windows. Fake only timers
  // inside this serial, process-isolated suite; node:test restores them after each test.
  context.mock.timers.enable({ apis: ["setTimeout"] });
  concurrencyFallbackCalls.length = 0;
  fallback.clearAllModelLockouts();
  cooldown.clearCooldownState();
  semaphore.resetAll();
  resetAllCircuitBreakers();
});
test.after(async () => {
  hooks.deregister();
  semaphore.resetAll();
  await (await import("../../src/lib/usage/callLogs.ts")).closeCallLogSaves(5_000);
  core.resetDbInstance();
  fs.rmSync(data, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

async function seed(provider = "openai", fixedSeconds = 7) {
  const id = randomUUID();
  const row = await providers.createProviderConnection({
    provider,
    authType: "apikey",
    name: id,
    apiKey: `fixture-${id}`,
    isActive: true,
    testStatus: "active",
    providerSpecificData: {
      rotationOverrides: {
        rateLimitResetSeconds: fixedSeconds,
        rotateOn429: false,
        error429Threshold: 99,
      },
    },
  });
  return String(row.id);
}

async function dispatch(
  selectedId: string,
  options: {
    provider?: string;
    status?: number;
    text?: string;
    hints?: boolean;
    pin?: string;
    omitSelectedHeader?: boolean;
    retryAfter?: string;
    headers?: Record<string, string>;
    providerCooldown?: boolean;
  } = {}
) {
  const provider = options.provider ?? "openai";
  const profileSettings = {
    connectionCooldown: {
      apikey: { useUpstreamRetryHints: options.hints === true },
      oauth: { useUpstreamRetryHints: options.hints === true },
    },
  };
  await settingsDb.updateSettings({ resilienceSettings: profileSettings });
  invalidateDbCache("settings");
  const targets: {
    connectionId?: string | null;
    allowedConnectionIds?: string[];
    effectiveComboStrategy?: string;
  }[] = [];
  const warnings: string[] = [];
  let attempts = 0;
  const response = await handleComboChat({
    body: { model: "concurrency-rr", messages: [{ role: "user", content: "hi" }] },
    combo: {
      name: `concurrency-rr-${randomUUID()}`,
      strategy: "round-robin",
      models: [
        {
          model: `${provider}/fixture-model`,
          ...(options.pin ? { connectionId: options.pin } : {}),
        },
        { model: "claude/healthy-fixture" },
      ],
      config: {
        maxRetries: 0,
        maxSetRetries: 0,
        retryDelayMs: 0,
        fallbackDelayMs: 0,
        connectionAwareExpansion: false,
        disableSessionStickiness: true,
      },
    },
    settings: {
      resilienceSettings: {
        ...profileSettings,
        providerCooldown: { enabled: options.providerCooldown !== false },
      },
    },
    handleSingleModel: async (_body, model, target) => {
      const resolvedTarget = target && "connectionId" in target ? target : null;
      if (resolvedTarget?.provider === provider || model.startsWith(`${provider}/`)) {
        targets.push(resolvedTarget ?? {});
        attempts++;
        return new Response(JSON.stringify({ error: { message: options.text ?? message } }), {
          status: options.status ?? 429,
          headers: {
            "content-type": "application/json",
            ...(options.omitSelectedHeader
              ? {}
              : { "x-omniroute-selected-connection-id": selectedId }),
            ...(options.retryAfter !== undefined ? { "retry-after": options.retryAfter } : {}),
            ...options.headers,
          },
        });
      }
      return new Response(JSON.stringify({ choices: [{ message: { content: "healthy" } }] }), {
        headers: { "content-type": "application/json" },
      });
    },
    log: {
      info() {},
      debug() {},
      error() {},
      warn(_tag, msg) {
        warnings.push(String(msg));
      },
    },
    allCombos: [],
  });
  assert.equal(response.status, 200);
  assert.equal(attempts, 1, "new RR requests must execute the failed target before its sibling");
  assert.equal(
    targets[0]?.effectiveComboStrategy,
    "round-robin",
    "specialized RR owner must execute"
  );
  return { result: concurrencyFallbackCalls.at(-1), targets, warnings };
}

test("actual RR resolves the selected response account fixed override without account rotation gates", async () => {
  await seed("openai", 11);
  const selected = await seed("openai", 7);
  const { result, targets, warnings } = await dispatch(selected);
  assert.equal(targets[0]?.connectionId, null);
  assert.equal(result?.reason, "concurrency_limited");
  assert.equal(result?.cooldownMs, 7_000);
  assert.ok(warnings.some((line) => line.includes("cooldown 7000ms")));
});

test("actual RR enabled positive hint wins; disabled/nonpositive hints retain selected fixed", async () => {
  const selected = await seed();
  assert.equal(
    (await dispatch(selected, { hints: true, retryAfter: "2" })).result?.cooldownMs,
    2_000
  );
  for (const options of [
    { hints: false, retryAfter: "2" },
    { hints: true, retryAfter: "0" },
    { hints: true, retryAfter: "invalid" },
  ]) {
    const { result } = await dispatch(selected, options);
    assert.equal(result?.cooldownMs, 7_000);
    assert.equal(result?.usedUpstreamRetryHint, false);
  }
});

test("actual RR pin fallback retains its implicit allowlist when response has no selected header", async () => {
  const pinned = await seed("openai", 11);
  const { result, targets } = await dispatch(pinned, { pin: pinned, omitSelectedHeader: true });
  assert.equal(result?.cooldownMs, 11_000);
  assert.equal(targets[0]?.connectionId, pinned);
  assert.deepEqual(targets[0]?.allowedConnectionIds, [pinned]);
});

test("actual RR repeated concurrency leaves opted-in provider counters empty", async () => {
  const selected = await seed();
  for (let attempt = 0; attempt < 3; attempt++) {
    await dispatch(selected);
    assert.equal(cooldown.getCooldownEntryCount(), 0);
  }
  await dispatch(selected, { providerCooldown: false });
  assert.equal(cooldown.getCooldownEntryCount(), 0);
});

test("actual RR explicit provider quota headers retain precedence over concurrency", async () => {
  for (const [provider, headers] of [
    ["opencode", { "x-ratelimit-remaining-requests": "0" }],
    ["minimax", { "x-model-quota-remaining": "haiku=0,sonnet=42" }],
  ] as const) {
    const selected = await seed(provider);
    const { result } = await dispatch(selected, { provider, headers });
    assert.equal(result?.reason, "quota_exhausted");
    assert.notEqual(result?.cooldownMs, 1_000);
    cooldown.clearCooldownState();
  }
});

test("actual RR ordinary minute429 and 503 retain opted-in provider failure recording", async () => {
  const selected = await seed();
  for (const [status, text] of [
    [429, "Too many requests per minute"],
    [503, "temporarily unavailable"],
  ] as const) {
    cooldown.clearCooldownState();
    const { result } = await dispatch(selected, { status, text });
    assert.notEqual(result?.reason, "concurrency_limited");
    assert.ok(cooldown.getCooldownEntryCount() > 0);
  }
});

test("actual RR existing per-model429 provider counter exemption remains status-scoped", async () => {
  const selected = await seed("gemini");
  const ordinary = await dispatch(selected, {
    provider: "gemini",
    text: "Too many requests per minute",
  });
  assert.notEqual(ordinary.result?.reason, "concurrency_limited");
  assert.equal(cooldown.getCooldownEntryCount(), 0);
  await dispatch(selected, { provider: "gemini", status: 503, text: "temporarily unavailable" });
  assert.ok(cooldown.getCooldownEntryCount() > 0);
});
