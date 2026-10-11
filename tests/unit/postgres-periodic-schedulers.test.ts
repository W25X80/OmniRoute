import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test, { after } from "node:test";
import { schedulerFixture } from "../helpers/persistence/schedulerAdmissionFixture.ts";

const modulePaths = [
  "src/lib/services/quotaAutoPing.ts",
  "src/lib/contextWindowResolver.ts",
  "src/lib/memory/typedDecay.ts",
  "src/lib/pricingSync.ts",
  "src/lib/modelsDevSync.ts",
  "src/lib/arenaEloSync.ts",
  "src/lib/catalog/openrouterProviderStats.ts",
];
const moduleUrls = new Set(
  modulePaths.map((path) => new URL("../../" + path, import.meta.url).href)
);
const fixtureUrl = new URL("../helpers/persistence/schedulerAdmissionFixture.ts", import.meta.url)
  .href;
const domainDependencies = new Set([
  "@omniroute/open-sse/utils/error.ts",
  "@omniroute/open-sse/services/usage/codex.ts",
  "@/lib/usage/providerLimits/credentialRefresh",
  "@/shared/utils/circuitBreaker",
  "@/lib/db/settings",
  "@/lib/db/providers",
  "@/lib/exclusiveLeaseIsolation",
  "@omniroute/open-sse/services/modelLifecycle.ts",
  "@omniroute/open-sse/utils/proxyFetch.ts",
  "./modelCapabilities",
  "./store",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      context.parentURL &&
      moduleUrls.has(context.parentURL.split("?")[0]) &&
      (specifier === "@/lib/db/persistenceRuntime" ||
        specifier === "@/lib/db/controlPlaneContext" ||
        domainDependencies.has(specifier) ||
        (context.parentURL.endsWith("/arenaEloSync.ts") &&
          specifier === "@/shared/utils/featureFlags"))
    ) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const quota = await import("../../src/lib/services/quotaAutoPing.ts");
const context = await import("../../src/lib/contextWindowResolver.ts");
const memory = await import("../../src/lib/memory/typedDecay.ts");
const pricing = await import("../../src/lib/pricingSync.ts");
const models = await import("../../src/lib/modelsDevSync.ts");
const arena = await import("../../src/lib/arenaEloSync.ts");
const openrouter = await import("../../src/lib/catalog/openrouterProviderStats.ts");
after(() => hooks.deregister());

test("all native periodic producers admit owned work, skip overlap and drain after closing timers", async () => {
  const saved = Object.fromEntries(
    [
      "OMNIROUTE_DATABASE_BACKEND",
      "MEMORY_TYPED_DECAY_ENABLED",
      "MEMORY_TYPED_DECAY_SWEEP_INTERVAL",
      "ARENA_ELO_SYNC_ENABLED",
    ].map((name) => [name, process.env[name]])
  );
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.MEMORY_TYPED_DECAY_ENABLED = "true";
  process.env.MEMORY_TYPED_DECAY_SWEEP_INTERVAL = "1";
  process.env.ARENA_ELO_SYNC_ENABLED = "true";
  let release!: () => void;
  schedulerFixture.barrier = new Promise<void>((resolve) => {
    release = resolve;
  });
  schedulerFixture.admissions.length = 0;
  const original = { setTimeout, setInterval, clearTimeout, clearInterval };
  type Handle = { unref(): void };
  const timers = new Map<Handle, () => void>();
  const arm = (callback: () => void, once: boolean) => {
    const handle = { unref() {} };
    timers.set(handle, () => {
      if (once) timers.delete(handle);
      callback();
    });
    return handle;
  };
  globalThis.setTimeout = ((callback: () => void) =>
    arm(callback, true)) as unknown as typeof setTimeout;
  globalThis.setInterval = ((callback: () => void) =>
    arm(callback, false)) as unknown as typeof setInterval;
  globalThis.clearTimeout = ((handle: Handle) => {
    timers.delete(handle);
  }) as unknown as typeof clearTimeout;
  globalThis.clearInterval = ((handle: Handle) => {
    timers.delete(handle);
  }) as unknown as typeof clearInterval;
  const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
  const stoppers = [
    quota.stopQuotaAutoPingAsync,
    context.stopContextWindowReconcileAsync,
    memory.stopMemoryDecaySweepAsync,
    pricing.stopPeriodicSyncAsync,
    models.stopPeriodicSyncAsync,
    arena.stopArenaEloSyncAsync,
    openrouter.stopOpenRouterProviderStatsSync,
  ];
  try {
    quota.startQuotaAutoPing();
    context.startContextWindowReconcile(1000);
    memory.startMemoryDecaySweep(1000);
    pricing.startPeriodicSync(1000);
    models.startPeriodicSync(1000);
    await arena.initArenaEloSync();
    openrouter.initOpenRouterProviderStatsSync();
    await turn();
    assert.equal(schedulerFixture.admissions.length, 7);
    const secondQuotaBundle = await import(
      new URL("../../src/lib/services/quotaAutoPing.ts?second-bundle", import.meta.url).href
    );
    secondQuotaBundle.startQuotaAutoPing();
    await turn();
    assert.equal(schedulerFixture.admissions.length, 7, "another bundle must reuse the same owner");
    const callbacks = [...timers.values()];
    callbacks.forEach((callback) => callback());
    await turn();
    assert.equal(
      schedulerFixture.admissions.length,
      7,
      "an active tick must not create an unbounded queue"
    );
    let stopped = false;
    const stopping = Promise.all([
      secondQuotaBundle.stopQuotaAutoPingAsync(),
      ...stoppers.map((stop) => stop()),
    ]).then(() => {
      stopped = true;
    });
    await turn();
    assert.equal(stopped, false);
    assert.equal(timers.size, 0);
    callbacks.forEach((callback) => callback());
    await turn();
    assert.equal(schedulerFixture.admissions.length, 7, "a stale timer cannot regain admission");
    release();
    await stopping;
    assert.equal(stopped, true);
  } finally {
    release();
    await Promise.all(stoppers.map((stop) => stop()));
    Object.assign(globalThis, original);
    schedulerFixture.barrier = undefined;
    for (const [name, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
});

test("quota auto-ping accepts only owner records with string identifiers and providers", async () => {
  const valid = { id: "connection-one", provider: "codex", lastPingAt: null };
  schedulerFixture.providers = [valid, { id: 42, provider: "codex" }, { id: "missing-provider" }];
  try {
    const rows = await quota.createDefaultQuotaAutoPingDeps().getProviderConnections({});
    assert.deepEqual(rows, [valid]);
  } finally {
    schedulerFixture.providers = [];
  }
});
