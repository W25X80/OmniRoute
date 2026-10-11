import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import test, { mock } from "node:test";

const directory = mkdtempSync(join(tmpdir(), "omniroute-credential-disable-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES = "true";
process.env.CREDENTIAL_HEALTH_CHECK_INTERVAL = "10000";
const core = await import("../../src/lib/db/core.ts");
const settings = await import("../../src/lib/db/settings.ts");
const providers = await import("../../src/lib/db/providers.ts");
const scheduler = await import("../../src/lib/credentialHealth/scheduler.ts");

test("global zero disables absent overrides but keeps explicit positive connection cadence", () => {
  assert.equal(scheduler.getConnIntervalMs({}, 0), null);
  assert.equal(scheduler.getConnIntervalMs({ healthCheckInterval: null }, 0), null);
  assert.equal(scheduler.getConnIntervalMs({ healthCheckInterval: 0 }, 0), null);
  assert.equal(scheduler.getConnIntervalMs({ healthCheckInterval: 5 }, 0), 300000);
});

test("disabled health probes use a finite config cadence, resume after settings change, and stop", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response(
      JSON.stringify({ data: [{ id: "gpt-4.1-mini" }], choices: [{ message: { content: "OK" } }] }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  };
  mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  try {
    await providers.createProviderConnection({
      provider: "openai",
      authType: "apikey",
      name: "disabled health test",
      apiKey: "synthetic-only-test-key",
      isActive: true,
    });
    await settings.updateSettings({
      resilienceSettings: { credentialHealthCheck: { intervalMinutes: 0 } },
    });
    await scheduler.forceSweep();
    assert.equal(calls, 0);
    mock.timers.tick(9999);
    await setImmediate();
    assert.equal(calls, 0);
    // The next finite config check observes the operator's re-enabled cadence.
    await settings.updateSettings({
      resilienceSettings: { credentialHealthCheck: { intervalMinutes: 1 } },
    });
    mock.timers.tick(1);
    for (
      let attempt = 0;
      attempt < 20 && globalThis.__omnirouteCredentialHC?.sweepInProgress;
      attempt++
    )
      await setImmediate();
    assert.ok(calls > 0, "re-enabled credentials must be probed");
    scheduler.stopCredentialHealthCheck();
    const afterStop = calls;
    mock.timers.tick(600000);
    await setImmediate();
    assert.equal(calls, afterStop);
  } finally {
    scheduler.stopCredentialHealthCheck();
    mock.timers.reset();
    globalThis.fetch = originalFetch;
  }
});

test("an explicit positive connection override is probed while the global cadence is disabled", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response(
      JSON.stringify({ data: [{ id: "gpt-4.1-mini" }], choices: [{ message: { content: "OK" } }] }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  };
  try {
    await providers.createProviderConnection({
      provider: "openai",
      authType: "apikey",
      name: "explicit health cadence",
      apiKey: "second-synthetic-only-test-key",
      isActive: true,
      healthCheckInterval: 1,
    });
    await settings.updateSettings({
      resilienceSettings: { credentialHealthCheck: { intervalMinutes: 0 } },
    });
    await scheduler.forceSweep();
    assert.ok(calls > 0, "an explicit positive connection override must remain eligible");
  } finally {
    scheduler.stopCredentialHealthCheck();
    globalThis.fetch = originalFetch;
  }
});

test.after(() => {
  scheduler.stopCredentialHealthCheck();
  core.resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});
