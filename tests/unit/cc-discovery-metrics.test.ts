import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-cc-discovery-metrics-"));
process.env.DATA_DIR = TEST_DATA_DIR;

const core = await import("../../src/lib/db/core.ts");

test.beforeEach(() => {
  core.resetDbInstance();
  fs.mkdirSync(TEST_DATA_DIR, { recursive: true });
});

test.after(() => {
  core.resetDbInstance();
  try {
    fs.rmSync(TEST_DATA_DIR, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 100,
    });
  } catch {
    // best-effort cleanup
  }
});

test("getCcDiscoveryMetrics starts at zero", async () => {
  const { getCcDiscoveryMetrics } = await import("../../src/lib/db/ccDiscoveryMetrics.ts");
  const metrics = getCcDiscoveryMetrics();
  assert.deepEqual(metrics, {
    aliasRequests: 0,
    discoveryHits: 0,
    byModel: {},
  });
});

test("incrementCcAliasRequestCount bumps total and per-model counters", async () => {
  const { incrementCcAliasRequestCount, getCcDiscoveryMetrics } =
    await import("../../src/lib/db/ccDiscoveryMetrics.ts");

  incrementCcAliasRequestCount("kimi/kimi-k2.6");
  incrementCcAliasRequestCount("kimi/kimi-k2.6");
  incrementCcAliasRequestCount("openai/gpt-5.2");

  const metrics = getCcDiscoveryMetrics();
  assert.equal(metrics.aliasRequests, 3);
  assert.deepEqual(metrics.byModel, {
    "kimi/kimi-k2.6": 2,
    "openai/gpt-5.2": 1,
  });
});

test("incrementCcDiscoveryHitCount bumps the discovery-hit counter", async () => {
  const { incrementCcDiscoveryHitCount, getCcDiscoveryMetrics } =
    await import("../../src/lib/db/ccDiscoveryMetrics.ts");

  incrementCcDiscoveryHitCount();
  incrementCcDiscoveryHitCount();
  incrementCcDiscoveryHitCount();

  const metrics = getCcDiscoveryMetrics();
  assert.equal(metrics.discoveryHits, 3);
});

test("getCcDiscoveryMetrics aggregates both counters together and repeated increments sum", async () => {
  const { incrementCcAliasRequestCount, incrementCcDiscoveryHitCount, getCcDiscoveryMetrics } =
    await import("../../src/lib/db/ccDiscoveryMetrics.ts");

  const before = getCcDiscoveryMetrics();

  incrementCcAliasRequestCount("combo/my-combo");
  incrementCcAliasRequestCount("combo/my-combo");
  incrementCcAliasRequestCount("combo/my-combo");
  incrementCcDiscoveryHitCount();

  const after = getCcDiscoveryMetrics();
  assert.equal(after.aliasRequests - before.aliasRequests, 3);
  assert.equal(after.discoveryHits - before.discoveryHits, 1);
  assert.equal(after.byModel["combo/my-combo"], 3);
});

test("async discovery hits preserve immediate SQLite writes without allocating native runtime", async () => {
  const { incrementCcDiscoveryHitCountAsync, getCcDiscoveryMetrics } =
    await import("../../src/lib/db/ccDiscoveryMetrics.ts");
  const originalRuntime = globalThis.__omniroutePersistenceRuntime;
  assert.equal(originalRuntime, undefined);
  const before = getCcDiscoveryMetrics().discoveryHits;
  const pending = incrementCcDiscoveryHitCountAsync();
  assert.ok(pending instanceof Promise);
  assert.equal(getCcDiscoveryMetrics().discoveryHits, before + 1);
  assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
  await pending;
});

test("sync and async SQLite hit writes keep corrupt-value recovery and sync void signature", async () => {
  const { incrementCcDiscoveryHitCount, incrementCcDiscoveryHitCountAsync, getCcDiscoveryMetrics } =
    await import("../../src/lib/db/ccDiscoveryMetrics.ts");
  core
    .getDbInstance()
    .prepare(
      "INSERT INTO key_value(namespace,key,value) VALUES('ccDiscoveryMetrics','discovery:hits','invalid') " +
        "ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value"
    )
    .run();
  assert.equal(incrementCcDiscoveryHitCount(), undefined);
  assert.equal(getCcDiscoveryMetrics().discoveryHits, 1);
  await incrementCcDiscoveryHitCountAsync();
  assert.equal(getCcDiscoveryMetrics().discoveryHits, 2);
});
