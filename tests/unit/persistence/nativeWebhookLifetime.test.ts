import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import { registerHooks } from "node:module";
import { realpathSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { setImmediate as nextTurn } from "node:timers/promises";
import { fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { webhookFixture } from "../../helpers/persistence/webhookLifetimeFixture.ts";

test(
  "detached native webhook owns bookkeeping after request end and joins drain",
  { timeout: 10000 },
  async () => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const dispatcherUrl = pathToFileURL(
      realpathSync(new URL("../../../src/lib/webhookDispatcher.ts", import.meta.url))
    ).href;
    const boot = new URL("../../helpers/persistence/runtimeOwnedWorkFixture.ts", import.meta.url)
      .href;
    const delivery = new URL("../../helpers/persistence/webhookLifetimeFixture.ts", import.meta.url)
      .href;
    const bootDependencies = new Set([
      "./postgres/environment",
      "./postgres/controlPlane",
      "../artifacts/s3ArtifactConfiguration",
      "../artifacts/s3ArtifactStore",
      "@/shared/utils/circuitBreaker",
      "@/domain/quotaCache",
      "./postgres/catalogSnapshot",
    ]);
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (context.parentURL === runtimeUrl && bootDependencies.has(specifier))
          return { url: boot, shortCircuit: true };
        if (
          context.parentURL === dispatcherUrl &&
          ["./db/webhooks", "./db/webhookDeliveries", "@/shared/network/webhookFetch"].includes(
            specifier
          )
        )
          return { url: delivery, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const previous = new Map(
      ["OMNIROUTE_DATABASE_BACKEND", "STORAGE_ENCRYPTION_KEY"].map((name) => [
        name,
        process.env[name],
      ])
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "7e".repeat(32);
    let runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") | undefined;
    try {
      fixture.reset();
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const { notifyWebhookEvent } = await import(dispatcherUrl);
      await runtime.runPersistenceRequest(() => notifyWebhookEvent("provider.error", {}));
      await webhookFixture.fetched;
      let closed = false;
      const closing = runtime.closePersistenceRuntime().then(() => {
        closed = true;
      });
      await nextTurn();
      assert.equal(closed, false);
      webhookFixture.release();
      await closing;
      assert.deepEqual(webhookFixture.writes, ["read", "delivery", "record", "disable"]);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    } finally {
      webhookFixture.release();
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      fixture.reset();
      delete globalThis.__omniroutePersistenceRuntime;
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
