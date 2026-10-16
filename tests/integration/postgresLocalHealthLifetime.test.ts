import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "a local health sweep dispatched after its native request ends reads real PostgreSQL",
  {
    skip: !postgresTestsEnabled,
    timeout: 30000,
  },
  async () => {
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
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
    const previous = new Map(
      [
        "OMNIROUTE_DATABASE_BACKEND",
        "OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK",
        "STORAGE_ENCRYPTION_KEY",
        "JWT_SECRET",
        "API_KEY_SECRET",
      ].map((key) => [key, process.env[key]])
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK = "1";
    process.env.STORAGE_ENCRYPTION_KEY = "4b".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const runtime = await import("../../src/lib/db/persistenceRuntime.ts");
    const context = await import("../../src/lib/db/controlPlaneContext.ts");
    const health = await import("../../src/lib/localHealthCheck.ts");
    const metrics = () => runtime.getPersistenceBackgroundMetrics()["local-health.sweep"];
    const errors: unknown[][] = [];
    const originalError = console.error;
    console.error = (...args: unknown[]) => {
      errors.push(args);
    };
    try {
      await runtime.ensurePersistenceReady();
      let creator: ReturnType<typeof context.getSelectedControlPlane>;
      // No local nodes means the real nodes repository is read without an outbound probe.
      await runtime.runPersistenceRequest(async () => {
        assert.deepEqual(await context.getSelectedControlPlane()!.nodes.list(), []);
      });
      for (let index = 0; index < 2; index++) {
        const completed = Promise.withResolvers<void>();
        await runtime.runPersistenceRequest(() => {
          creator = context.getSelectedControlPlane();
          setTimeout(() => {
            void health.sweep().then(completed.resolve, completed.reject);
          }, 1);
        });
        assert.throws(() => creator!.assertReady(), /scope has ended/);
        await completed.promise;
      }
      assert.equal(metrics()?.completed, 2);
      assert.equal(metrics()?.failed, 0);
      assert.deepEqual(errors, []);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    } finally {
      health.stopLocalHealthCheck();
      await runtime.closePersistenceRuntime();
      console.error = originalError;
      hooks.deregister();
      delete globalThis.__omniroutePersistenceRuntime;
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }
);
