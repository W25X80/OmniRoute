import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
const data = fs.mkdtempSync(path.join(os.tmpdir(), "omni-concurrency-classify-"));
process.env.DATA_DIR = data;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const { checkFallbackError, getProviderProfile } =
  await import("../../open-sse/services/accountFallback.ts");
const { shouldWaitForComboCooldown } =
  await import("../../open-sse/services/combo/comboCooldownRetry.ts");
test.after(() => fs.rmSync(data, { recursive: true, force: true }));

const message = "Too many concurrent requests";
const profile = { ...getProviderProfile("openai"), useUpstreamRetryHints: false };

test("generic concurrency429 stays fixed across existing backoff levels", () => {
  for (const level of [0, 2, 8]) {
    const result = checkFallbackError(429, message, level, null, "openai", null, profile);
    assert.equal(result.reason, "concurrency_limited");
    assert.equal(result.cooldownMs, 1_000);
    assert.equal(result.skipProviderBreaker, true);
    assert.equal(Object.hasOwn(result, "newBackoffLevel"), false);
  }
});

test("positive enabled upstream hints outrank an explicit fixed concurrency cooldown", () => {
  const fixedOnly = { concurrencyCooldownMs: 7_000 };
  const headers = new Headers({ "retry-after": "2" });
  const result = checkFallbackError(
    429,
    message,
    8,
    null,
    "openai",
    headers,
    { ...profile, useUpstreamRetryHints: true },
    null,
    fixedOnly
  );
  assert.equal(result.reason, "concurrency_limited");
  assert.equal(result.cooldownMs, 2_000);
  assert.equal(result.retryHintSource, "header");
  assert.equal(result.usedUpstreamRetryHint, true);
  assert.equal(Object.hasOwn(result, "newBackoffLevel"), false);
  const disabled = checkFallbackError(
    429,
    message,
    8,
    null,
    "openai",
    headers,
    profile,
    null,
    fixedOnly
  );
  assert.equal(disabled.cooldownMs, 7_000);
  assert.equal(disabled.usedUpstreamRetryHint, false);
  for (const hint of ["0", "invalid"]) {
    const result = checkFallbackError(
      429,
      message,
      8,
      null,
      "openai",
      new Headers({ "retry-after": hint }),
      { ...profile, useUpstreamRetryHints: true },
      null,
      fixedOnly
    );
    assert.equal(result.cooldownMs, 7_000);
  }
});

test("specific text/terminal and provider exhausted headers outrank generic concurrency", () => {
  for (const [provider, headers, scope] of [
    ["opencode", { "x-ratelimit-remaining-requests": "0" }, "connection"],
    ["minimax", { "x-model-quota-remaining": "haiku=0,sonnet=42" }, undefined],
  ] as const) {
    const result = checkFallbackError(
      429,
      message,
      0,
      null,
      provider,
      new Headers(headers),
      profile
    );
    assert.equal(result.reason, "quota_exhausted");
    assert.equal(result.ruleScope, scope);
  }
  const exhausted = checkFallbackError(
    429,
    `${message}; daily quota exceeded`,
    0,
    null,
    "openai",
    null,
    profile
  );
  assert.notEqual(exhausted.reason, "concurrency_limited");
  const terminal = checkFallbackError(
    429,
    `${message}; account has been deactivated`,
    0,
    null,
    "openai",
    null,
    profile
  );
  assert.equal(terminal.permanent, true);
  const ordinary = checkFallbackError(
    429,
    "Too many requests per minute",
    2,
    null,
    "openai",
    null,
    profile
  );
  assert.equal(ordinary.newBackoffLevel, 3);
  const server = checkFallbackError(503, message, 2, null, "openai", null, profile);
  assert.notEqual(server.reason, "concurrency_limited");
  assert.equal(server.newBackoffLevel, 3);
});

test("new concurrency retry reason retains every existing wait/budget gate", () => {
  const input = {
    reason: "concurrency_limited",
    waitMs: 1_000,
    attempt: 0,
    budgetLeftMs: 1_000,
    settings: { enabled: true, maxWaitMs: 1_000, maxAttempts: 1, budgetMs: 1_000 },
  };
  assert.equal(shouldWaitForComboCooldown(input).wait, true);
  for (const variation of [
    { attempt: 1 },
    { budgetLeftMs: 999 },
    { waitMs: 1_001 },
    { reason: "quota_exhausted" },
    { settings: { ...input.settings, enabled: false } },
  ])
    assert.equal(shouldWaitForComboCooldown({ ...input, ...variation }).wait, false);
});
