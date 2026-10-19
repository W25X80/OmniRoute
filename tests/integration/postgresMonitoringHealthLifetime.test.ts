import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "monitoring SWR reads native PostgreSQL after the scheduling request closes",
  {
    skip: !postgresTestsEnabled,
    timeout: 30000,
  },
  async (t) => {
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const installHooks = () =>
      registerHooks({
        resolve(specifier, context, next) {
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
          return next(specifier, context);
        },
      });
    const keys = [
      "OMNIROUTE_DATABASE_BACKEND",
      "OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK",
      "DEEP_HEALTH_CHECK_ENABLED",
      "DATA_DIR",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const previous = new Map(keys.map((key) => [key, process.env[key]]));
    const scratch = await mkdtemp(join(tmpdir(), "omniroute-native-monitoring-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK = "1";
    process.env.DATA_DIR = scratch;
    process.env.STORAGE_ENCRYPTION_KEY = "4b".repeat(32);
    delete process.env.DEEP_HEALTH_CHECK_ENABLED;
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let health: typeof import("../../src/app/api/monitoring/health/route.ts") | undefined;
    let localHealth: typeof import("../../src/lib/localHealthCheck.ts") | undefined;
    let hooks: ReturnType<typeof installHooks> | undefined;
    const failures: unknown[][] = [];
    for (const method of ["warn", "error"] as const) {
      const original = console[method].bind(console);
      t.mock.method(console, method, (...args: unknown[]) => {
        if (String(args[0]).startsWith("[API] GET /api/monitoring/health")) failures.push(args);
        else original(...args);
      });
    }
    try {
      hooks = installHooks();
      runtime = await import("../../src/lib/db/persistenceRuntime.ts");
      const context = await import("../../src/lib/db/controlPlaneContext.ts");
      health = await import("../../src/app/api/monitoring/health/route.ts");
      localHealth = await import("../../src/lib/localHealthCheck.ts");
      await runtime.ensurePersistenceReady();
      const { nativeRuntimeFixture } =
        await import("../helpers/persistence/runtimeNativeIntegrationFixture.ts");
      const plane = nativeRuntimeFixture.plane!;
      const nativeRun = plane.run.bind(plane);
      const readBindings: Array<ReturnType<typeof context.getSelectedControlPlane>> = [];
      let creator: ReturnType<typeof context.getSelectedControlPlane>;
      t.mock.method(plane, "run", <T>(body: Parameters<typeof plane.run<T>>[0]) => {
        return nativeRun((binding) => {
          const observed: typeof binding = {
            ...binding,
            providers: {
              ...binding.providers,
              async list(...args) {
                binding.assertReady();
                const result = await binding.providers.list(...args);
                binding.assertReady();
                assert.equal(context.getSelectedControlPlane(), observed);
                if (creator) assert.throws(() => creator.assertReady(), /scope has ended/);
                readBindings.push(observed);
                return result;
              },
            },
          };
          return context.withPostgresControlPlane(observed, () => body(observed));
        });
      });
      health.__test_resetMonitoringHealthPayloadCache();
      let now = Date.now();
      t.mock.method(Date, "now", () => now);
      const metrics = () => runtime.getPersistenceBackgroundMetrics()["monitoring.refresh"];
      const initial = metrics()?.completed ?? 0;
      const request = () => new Request("http://localhost/api/monitoring/health");
      assert.equal((await runtime.runPersistenceRequest(() => health.GET(request()))).status, 200);
      assert.equal(metrics()?.completed, initial + 1);
      now += 2000;
      const stale = await runtime.runPersistenceRequest(async () => {
        creator = context.getSelectedControlPlane();
        return health.GET(request());
      });
      assert.equal(stale.status, 200);
      assert.throws(() => creator!.assertReady(), /scope has ended/);
      const deadline = performance.now() + 5000;
      while ((metrics()?.completed ?? 0) < initial + 2 && performance.now() < deadline)
        await delay(5);
      assert.equal(metrics()?.completed, initial + 2);
      assert.equal(metrics()?.failed ?? 0, 0);
      assert.deepEqual(failures, []);
      assert.ok(
        readBindings.length >= 2,
        "both builders must complete real native provider queries"
      );
      assert.ok(readBindings.every((binding) => binding !== creator));
      assert.ok(new Set(readBindings).size >= 2, "each builder needs its own fresh binding");
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    } finally {
      try {
        localHealth?.stopLocalHealthCheck();
        await runtime?.closePersistenceRuntime();
      } finally {
        health?.__test_resetMonitoringHealthPayloadCache();
        hooks?.deregister();
        delete globalThis.__omniroutePersistenceRuntime;
        for (const [key, value] of previous) {
          if (value === undefined) delete process.env[key];
          else process.env[key] = value;
        }
        await rm(scratch, { recursive: true, force: true });
      }
    }
  }
);
