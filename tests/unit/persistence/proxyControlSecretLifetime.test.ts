import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import test, { type TestContext } from "node:test";
import { encrypt } from "../../../src/lib/db/encryption.ts";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import {
  fixture,
  proxySecretDatabaseFixture,
  initializePostgresControlPlane,
  barrier,
} from "../../helpers/persistence/proxyControlSecretRuntimeFixture.ts";
import { proxyControlSecretProbe } from "../../helpers/persistence/proxyControlSecretProbe.ts";

const runtimeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
).href;
const reloadUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/proxySubscription/reloadSecret.ts", import.meta.url))
).href;
const fixtureUrl = new URL(
  "../../helpers/persistence/proxyControlSecretRuntimeFixture.ts",
  import.meta.url
).href;
const probeUrl = new URL("../../helpers/persistence/proxyControlSecretProbe.ts", import.meta.url)
  .href;
const dependencies = new Set([
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
    if (context.parentURL === runtimeUrl && dependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (context.parentURL === reloadUrl && specifier === "../db/encryption")
      return { url: probeUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const names = [
  "OMNIROUTE_DATABASE_BACKEND",
  "STORAGE_ENCRYPTION_KEY",
  "JWT_SECRET",
  "API_KEY_SECRET",
  "DATA_DIR",
  "OMNIROUTE_PROXY_CORE_RELOAD_COMMAND",
];
const previous = new Map(names.map((name) => [name, process.env[name]]));
const directory = mkdtempSync(join(tmpdir(), "omniroute-control-secret-lifetime-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.STORAGE_ENCRYPTION_KEY = "4b".repeat(32);
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_PROXY_CORE_RELOAD_COMMAND = "external";
const runtime = (await import(
  runtimeUrl
)) as typeof import("../../../src/lib/db/persistenceRuntime.ts");
const service = (await import(
  reloadUrl
)) as typeof import("../../../src/lib/proxySubscription/reloadSecret.ts");
const core = await import("../../../src/lib/proxySubscription/coreConfig/reload.ts");
const renderers = await import("../../../src/lib/proxySubscription/coreConfig/renderers.ts");

async function waitUntil(predicate: () => boolean) {
  const deadline = performance.now() + 1500;
  while (!predicate() && performance.now() < deadline) await delay(5);
  assert.equal(predicate(), true, "bounded deferred secret observation did not arrive");
}
async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  proxySecretDatabaseFixture.reset();
  proxyControlSecretProbe.reset();
  core.__resetCoreReloadTestState();
  try {
    await body();
  } finally {
    core.clearCoreReloadState("lifetime");
    fixture.releaseBarriers();
    if (globalThis.__omniroutePersistenceRuntime) await runtime.closePersistenceRuntime();
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}
function clock(t: TestContext) {
  let now = 1_000_000;
  t.mock.method(Date, "now", () => now);
  return () => {
    now += 59_980;
  };
}
async function queue(t: TestContext) {
  const advance = clock(t);
  await runtime.runPersistenceRequest(() =>
    service.reloadAfterReplace("lifetime", null, "fixture.json")
  );
  advance();
  const resource = await runtime.runPersistenceRequest(async () => {
    const resource = new AsyncResource("ended-proxy-control-request");
    assert.equal(await service.reloadAfterReplace("lifetime", null, "fixture.json"), null);
    return resource;
  });
  assert.throws(() => resource.runInAsyncScope(getSelectedControlPlane), /scope ended/);
  return resource;
}

test.after(() => {
  hooks.deregister();
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  rmSync(directory, { recursive: true, force: true });
});

test(
  "a real deferred default-core callback rereads the rotated secret in a fresh installed application scope",
  { timeout: 10000 },
  (t) =>
    scenario(async () => {
      assert.equal(renderers.DEFAULT_CORE, "sing-box");
      assert.equal(renderers.RENDERERS[renderers.DEFAULT_CORE].apiReload, false);
      await runtime.ensurePersistenceReady();
      proxySecretDatabaseFixture.ciphertext = encrypt("before-deadline")!;
      const resource = await queue(t);
      try {
        proxySecretDatabaseFixture.ciphertext = encrypt("after-deadline")!;
        await waitUntil(() => proxyControlSecretProbe.values.length === 3);
        await Promise.allSettled([...globalThis.__omniroutePersistenceRuntime!.background]);
        assert.deepEqual(proxyControlSecretProbe.values, [
          "before-deadline",
          "before-deadline",
          "after-deadline",
        ]);
        const [first, second, fresh] = proxyControlSecretProbe.bindings;
        assert.notEqual(fresh, first);
        assert.notEqual(fresh, second);
        assert.equal(fresh.databaseIdentity, second.databaseIdentity);
        assert.throws(() => second.assertReady(), /scope ended/);
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
      } finally {
        resource.emitDestroy();
      }
    })
);

test(
  "a successful null secret is accepted without a native failure warning",
  { timeout: 10000 },
  (t) =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const warnings: string[] = [];
      t.mock.method(console, "warn", (message: unknown) => warnings.push(String(message)));
      const resource = await queue(t);
      try {
        await waitUntil(() => proxySecretDatabaseFixture.reads === 3);
        await Promise.allSettled([...globalThis.__omniroutePersistenceRuntime!.background]);
        assert.deepEqual(warnings, []);
        assert.deepEqual(proxyControlSecretProbe.values, []);
      } finally {
        resource.emitDestroy();
      }
    })
);

for (const failure of [
  "closing",
  "identity",
  "admission",
  "fresh-identity",
  "catalog",
  "read",
] as const) {
  test(
    `native ${failure} failure is observed safely without a wrong-authority secret read`,
    { timeout: 10000 },
    (t) =>
      scenario(async () => {
        await runtime.ensurePersistenceReady();
        proxySecretDatabaseFixture.ciphertext = encrypt("private-test-secret")!;
        const warnings: string[] = [];
        t.mock.method(console, "warn", (message: unknown) => warnings.push(String(message)));
        const resource = await queue(t);
        const state = globalThis.__omniroutePersistenceRuntime!;
        const slots = failure === "admission" ? Array.from({ length: 64 }, () => barrier()) : [];
        try {
          if (failure === "closing") state.phase = "closing";
          if (failure === "identity") state.applicationIdentity = Object.freeze({});
          for (const slot of slots) state.background.add(slot.promise);
          if (failure === "fresh-identity")
            proxySecretDatabaseFixture.afterCatalog = () => {
              state.applicationIdentity = Object.freeze({});
              proxySecretDatabaseFixture.afterCatalog = undefined;
            };
          if (failure === "catalog")
            fixture.catalogFailure = new Error("private-test-secret catalog error");
          if (failure === "read")
            proxySecretDatabaseFixture.failure = new Error("private-test-secret driver error");
          await waitUntil(() => warnings.length === 1);
          assert.deepEqual(warnings, ["[ProxySubscription] Deferred control secret read failed"]);
          assert.equal(proxySecretDatabaseFixture.reads, failure === "read" ? 3 : 2);
          assert.equal(proxyControlSecretProbe.values.length, 2);
          assert.equal(state.requests.size, 0);
        } finally {
          for (const slot of slots) {
            slot.resolve();
            state.background.delete(slot.promise);
          }
          resource.emitDestroy();
        }
      })
  );
}

test(
  "a standalone native plane keeps its explicit-await contract and never allocates an application runtime",
  { timeout: 10000 },
  (t) =>
    scenario(async () => {
      const standalone = await initializePostgresControlPlane();
      const advance = clock(t);
      proxySecretDatabaseFixture.ciphertext = encrypt("standalone")!;
      try {
        await standalone.run(async () => {
          await service.reloadAfterReplace("lifetime", null, "fixture.json");
          advance();
          await service.reloadAfterReplace("lifetime", null, "fixture.json");
          await waitUntil(() => proxyControlSecretProbe.values.length === 3);
          assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
          assert.equal(fixture.environmentReads, 0);
          assert.equal(fixture.planeCreates, 1);
          assert.equal(new Set(proxyControlSecretProbe.bindings).size, 1);
        });
      } finally {
        await standalone.close();
      }
    })
);

test(
  "a foreign plane is not rerouted into a saturated application's background owner",
  { timeout: 10000 },
  (t) =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      const foreign = await initializePostgresControlPlane();
      const advance = clock(t);
      const slots = Array.from({ length: 64 }, () => barrier());
      for (const slot of slots) state.background.add(slot.promise);
      proxySecretDatabaseFixture.ciphertext = encrypt("foreign")!;
      try {
        await foreign.run(async (binding) => {
          assert.notEqual(binding.databaseIdentity, state.applicationIdentity);
          await service.reloadAfterReplace("lifetime", null, "fixture.json");
          advance();
          await service.reloadAfterReplace("lifetime", null, "fixture.json");
          await waitUntil(() => proxyControlSecretProbe.values.length === 3);
          assert.equal(state.background.size, 64);
          assert.ok(proxyControlSecretProbe.bindings.every((value) => value === binding));
        });
      } finally {
        for (const slot of slots) {
          slot.resolve();
          state.background.delete(slot.promise);
        }
        await foreign.close();
      }
    })
);
