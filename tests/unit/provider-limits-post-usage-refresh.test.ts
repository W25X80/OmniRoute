import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function immediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

test("SQLite post-usage refresh retains coalescing, deferred start and failure reporting", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "sqlite-post-usage-refresh-"));
  const saved = {
    DATA_DIR: process.env.DATA_DIR,
    OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
    PROVIDER_LIMITS_POST_USAGE_REFRESH_DELAY_MS:
      process.env.PROVIDER_LIMITS_POST_USAGE_REFRESH_DELAY_MS,
  };
  process.env.DATA_DIR = directory;
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  process.env.PROVIDER_LIMITS_POST_USAGE_REFRESH_DELAY_MS = "123";
  const timers: ReturnType<typeof setTimeout>[] = [];
  let core: typeof import("../../src/lib/db/core.ts") | undefined;
  try {
    core = await import("../../src/lib/db/core.ts");
    await core.ensureDbInitialized();
    const { notifyProviderUsageRecorded } = await import("../../src/lib/usage/providerLimits.ts");
    const callbacks: Array<() => void> = [];
    const warnings: string[] = [];
    const originalTimeout = globalThis.setTimeout;
    t.mock.method(
      globalThis,
      "setTimeout",
      (callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
        if (delay !== 123) return originalTimeout(callback, delay, ...args);
        callbacks.push(() => callback(...args));
        const timer = originalTimeout(() => {}, 60_000);
        timer.unref();
        timers.push(timer);
        return timer;
      }
    );
    t.mock.method(console, "warn", (...args: unknown[]) => {
      warnings.push(args.map(String).join(" "));
    });
    const missing = randomUUID();
    notifyProviderUsageRecorded("openai", missing);
    assert.equal(callbacks.length, 0);
    notifyProviderUsageRecorded("antigravity", missing);
    notifyProviderUsageRecorded("agy", missing);
    assert.equal(callbacks.length, 1);
    assert.equal(warnings.length, 0);
    callbacks[0]();
    for (let attempt = 0; warnings.length === 0 && attempt < 100; attempt++) await immediate();
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /Post-usage refresh failed.*Connection not found/);
    notifyProviderUsageRecorded("antigravity", missing);
    assert.equal(callbacks.length, 2);
    callbacks[1]();
    for (let attempt = 0; warnings.length < 2 && attempt < 100; attempt++) await immediate();
    assert.equal(warnings.length, 2);
  } finally {
    for (const timer of timers) clearTimeout(timer);
    t.mock.restoreAll();
    core?.closeDbInstance();
    for (const [name, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    rmSync(directory, { recursive: true, force: true });
  }
});
