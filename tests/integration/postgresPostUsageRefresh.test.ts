import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "post-usage timer refresh re-enters native persistence after its originating request ends",
  {
    skip: !postgresTestsEnabled,
    timeout: 30_000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const directory = mkdtempSync(join(tmpdir(), "pg-post-usage-refresh-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
      PROVIDER_LIMITS_POST_USAGE_REFRESH_DELAY_MS:
        process.env.PROVIDER_LIMITS_POST_USAGE_REFRESH_DELAY_MS,
    };
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    const timers: ReturnType<typeof setTimeout>[] = [];
    const resources: AsyncResource[] = [];
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-post-usage-storage-secret";
      process.env.JWT_SECRET = "isolated-post-usage-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-post-usage-api-secret";
      process.env.PROVIDER_LIMITS_POST_USAGE_REFRESH_DELAY_MS = "123";
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      const { notifyProviderUsageRecorded } = await runtime!.runPersistenceRequest(
        () => import("../../src/lib/usage/providerLimits.ts")
      );
      const callbacks: Array<() => void> = [];
      const warnings: string[] = [];
      let warningReceived!: () => void;
      const reported = new Promise<void>((resolve) => {
        warningReceived = resolve;
      });
      const originalTimeout = globalThis.setTimeout;
      t.mock.method(
        globalThis,
        "setTimeout",
        (callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
          if (delay !== 123) return originalTimeout(callback, delay, ...args);
          const resource = new AsyncResource("post-usage-timer-originating-context");
          resources.push(resource);
          callbacks.push(() => resource.runInAsyncScope(() => callback(...args)));
          const timer = originalTimeout(() => {}, 60_000);
          timer.unref();
          timers.push(timer);
          return timer;
        }
      );
      t.mock.method(console, "warn", (...args: unknown[]) => {
        warnings.push(args.map(String).join(" "));
        warningReceived();
      });
      let expired: ReturnType<typeof getSelectedPostgresDatabase>;
      const missing = randomUUID();
      await runtime!.runPersistenceRequest(async () => {
        expired = getSelectedPostgresDatabase();
        notifyProviderUsageRecorded("antigravity", missing);
        notifyProviderUsageRecorded("agy", missing);
      });
      assert.equal(callbacks.length, 1);
      assert.equal(runtime!.getPersistenceRuntimeStatus().activeRequests, 0);
      assert.throws(() => expired!.transaction(async () => {}), /scope has ended/);
      callbacks[0]();
      await reported;
      // This error can only occur after the real native provider SELECT returns no row.
      // An inherited expired binding fails earlier with a scope-ended error instead.
      assert.equal(warnings.length, 1);
      assert.match(warnings[0], /Post-usage refresh failed.*Connection not found/);
      assert.doesNotMatch(warnings[0], /scope has ended|at \/|STORAGE_ENCRYPTION_KEY/);
      await runtime!.closePersistenceRuntime();
      assert.equal(runtime!.getPersistenceRuntimeStatus().backgroundOperations, 0);
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      for (const timer of timers) clearTimeout(timer);
      for (const resource of resources) resource.emitDestroy();
      t.mock.restoreAll();
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
