import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test, { after, beforeEach } from "node:test";
import {
  startupFixture,
  nativeStartupFixture,
  markServerStopping,
} from "../helpers/persistence/nativeStartupFixture.ts";

const fixtureUrl = new URL("../helpers/persistence/nativeStartupFixture.ts", import.meta.url).href;
const optionalGateUrl = new URL(
  "../helpers/persistence/startupOptionalImportFixture.ts",
  import.meta.url
).href;
const bootUrl = new URL("../../src/lib/startup/postgresBootstrap.ts", import.meta.url).href;
const instrumentationUrl = new URL("../../src/instrumentation-node.ts", import.meta.url).href;
const shutdownUrl = new URL("../../src/lib/startup/postgresShutdown.ts", import.meta.url).href;
const jobsUrl = new URL("../../src/lib/startup/postgresJobs.ts", import.meta.url).href;
const plainUrl = (url: string | undefined) => url?.split("?")[0];
const ownerSymbol = Symbol.for("omniroute.postgresStartupOwner");
const ownedGlobals = globalThis as typeof globalThis & { [ownerSymbol]?: unknown };
let importGeneration = 0;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = plainUrl(context.parentURL);
    if (parent === bootUrl) return { url: fixtureUrl, shortCircuit: true };
    if (parent === shutdownUrl) {
      if (specifier === "./postgresBootstrap") return { url: bootUrl, shortCircuit: true };
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (parent === jobsUrl) {
      if (specifier === "./postgresBootstrap") return { url: bootUrl, shortCircuit: true };
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (parent === instrumentationUrl) {
      if (specifier === "@/lib/startup/postgresBootstrap")
        return { url: bootUrl, shortCircuit: true };
      if (specifier === "@/lib/startup/postgresShutdown")
        return { url: shutdownUrl, shortCircuit: true };
      if (specifier === "@/lib/startup/postgresJobs") return { url: jobsUrl, shortCircuit: true };
      if (specifier === "@/lib/modelsDevSync")
        return { url: `${optionalGateUrl}?generation=${importGeneration}`, shortCircuit: true };
      if (
        specifier !== "@/lib/db/backendSelection" &&
        specifier !== "@/lib/instrumentationBootError" &&
        specifier !== "@/lib/system/headless"
      ) {
        return { url: fixtureUrl, shortCircuit: true };
      }
    }
    return nextResolve(specifier, context);
  },
});
const boot = await import("../../src/lib/startup/postgresBootstrap.ts");
const bundle: typeof boot = await import(`${bootUrl}?second-bundle`);
const instrumentation = await import("../../src/instrumentation-node.ts");
const shutdown = await import("../../src/lib/startup/postgresShutdown.ts");
const savedEnv = {
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  background: process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES,
  headless: process.env.OMNIROUTE_HEADLESS,
  marker: globalThis.__omniroutePersistenceTransportInstalled,
};
after(() => {
  hooks.deregister();
  delete ownedGlobals[ownerSymbol];
  for (const [key, value] of Object.entries({
    OMNIROUTE_DATABASE_BACKEND: savedEnv.backend,
    OMNIROUTE_DISABLE_BACKGROUND_SERVICES: savedEnv.background,
    OMNIROUTE_HEADLESS: savedEnv.headless,
  })) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  globalThis.__omniroutePersistenceTransportInstalled = savedEnv.marker;
});
beforeEach(() => {
  delete ownedGlobals[ownerSymbol];
  startupFixture.events.length = 0;
  startupFixture.identity = {};
  startupFixture.readyFailure = false;
  startupFixture.bootFailure = false;
  startupFixture.stopFailure = false;
  startupFixture.flushFailure = false;
  startupFixture.stopBarrier = undefined;
  nativeStartupFixture.readyBarrier = undefined;
  nativeStartupFixture.cloudBarrier = undefined;
  nativeStartupFixture.memoryBarrier = undefined;
  nativeStartupFixture.optionalImportBarrier = undefined;
  nativeStartupFixture.optionalImportEntered = undefined;
  importGeneration++;
  nativeStartupFixture.persistencePhase = "starting";
  nativeStartupFixture.serverPhase = "starting";
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES = "1";
  process.env.OMNIROUTE_HEADLESS = "1";
  globalThis.__omniroutePersistenceTransportInstalled = true;
});
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
const count = (event: string) => startupFixture.events.filter((entry) => entry === event).length;

// These exercise actual ownership and bootstrap control flow with controlled runtime/modules.
// They do not certify real PostgreSQL, S3, transport or external-provider behavior.
test("shutdown waits early factory work and blocks selected startup and Ready", async () => {
  const gate = deferred();
  nativeStartupFixture.readyBarrier = gate.promise;
  const registering = instrumentation.registerNodejs();
  const rejection = assert.rejects(registering, /startup is stopping/);
  await turn();
  let closed = false;
  const closing = shutdown.shutdownPostgresApplication().then(() => {
    closed = true;
  });
  try {
    await turn();
    assert.equal(closed, false);
    assert.equal(count("pool:close"), 0);
    assert.equal(count("server:starting"), 0);
    gate.resolve();
    await Promise.all([rejection, closing]);
    assert.equal(count("server:ready"), 0);
    assert.equal(
      startupFixture.events.some((entry) => entry.startsWith("start:")),
      false
    );
    assert.equal(count("pool:close"), 1);
    assert.ok(count("maintenance:resources:stop") > 0);
    assert.ok(count("maintenance:continuation:stop") > 0);
  } finally {
    gate.resolve();
    await Promise.allSettled([registering, closing]);
  }
});

test("shutdown drains an accepted producer before later jobs and pool close", async () => {
  delete process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES;
  const gate = deferred();
  nativeStartupFixture.cloudBarrier = gate.promise;
  const registering = instrumentation.registerNodejs();
  const rejection = assert.rejects(registering, /startup is stopping/);
  while (!startupFixture.events.includes("start:cloud")) await turn();
  const closing = shutdown.shutdownPostgresApplication();
  try {
    await turn();
    assert.equal(count("stop:cloud"), 0);
    assert.equal(count("pool:close"), 0);
    gate.resolve();
    await Promise.all([rejection, closing]);
    assert.equal(count("start:reasoning"), 0);
    assert.equal(count("start:batch"), 0);
    assert.equal(count("server:ready"), 0);
    assert.ok(
      startupFixture.events.indexOf("stop:cloud") < startupFixture.events.indexOf("pool:close")
    );
  } finally {
    gate.resolve();
    await Promise.allSettled([registering, closing]);
  }
});

test("optional import completing after cancellation cannot activate its producer", async () => {
  const gate = deferred();
  const entered = deferred();
  nativeStartupFixture.optionalImportBarrier = gate.promise;
  nativeStartupFixture.optionalImportEntered = entered.resolve;
  const subsystem = instrumentation.OPTIONAL_BOOT_SUBSYSTEMS.find(
    (entry) => entry.name === "models-dev-sync"
  )!;
  const starting = boot.runPostgresBootstrap(async () => {
    await subsystem.start();
    boot.assertPostgresBootstrapActive();
    startupFixture.events.push("ready");
  });
  const rejection = assert.rejects(starting, /startup is stopping/);
  await entered.promise;
  const closing = boot.runPostgresShutdown(async () => {
    startupFixture.events.push("cleanup");
  });
  try {
    await turn();
    assert.equal(count("cleanup"), 0);
    gate.resolve();
    await Promise.all([rejection, closing]);
    assert.equal(count("start:modelsDev"), 0);
    assert.equal(count("ready"), 0);
    assert.equal(count("cleanup"), 1);
  } finally {
    gate.resolve();
    await Promise.allSettled([starting, closing]);
  }
});

test("late parallel startup settling after shutdown cannot mark the server Ready", async () => {
  delete process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES;
  const gate = deferred();
  nativeStartupFixture.memoryBarrier = gate.promise;
  const registering = instrumentation.registerNodejs();
  const rejection = assert.rejects(registering, /startup is stopping/);
  while (!startupFixture.events.includes("start:memoryBackends")) await turn();
  const closing = shutdown.shutdownPostgresApplication();
  try {
    await turn();
    assert.equal(count("pool:close"), 0);
    assert.equal(count("server:ready"), 0);
    gate.resolve();
    await Promise.all([rejection, closing]);
    assert.equal(count("server:ready"), 0);
    assert.equal(count("pool:close"), 1);
  } finally {
    gate.resolve();
    await Promise.allSettled([registering, closing]);
  }
});

test(
  "outer fatal boot catch joins cleanup without waiting on itself",
  { timeout: 5000 },
  async () => {
    startupFixture.bootFailure = true;
    await assert.rejects(instrumentation.registerNodejs(), /Required registrar failed/);
    assert.equal(count("pool:close"), 1);
    assert.equal(count("server:ready"), 0);
    assert.equal(globalThis.__omniroutePersistenceRequest, undefined);
  }
);

test("factory failure closes without application imports or flushes", async () => {
  startupFixture.readyFailure = true;
  await assert.rejects(instrumentation.registerNodejs(), /Required bootstrap failed/);
  assert.equal(count("pool:close"), 1);
  assert.equal(
    startupFixture.events.some((entry) => entry.startsWith("stop:")),
    false
  );
  assert.equal(
    startupFixture.events.some((entry) => entry.startsWith("flush:")),
    false
  );
});

test("two failing registrations share producer cleanup across bundle-local flags", async () => {
  startupFixture.bootFailure = true;
  const otherInstrumentation = await import(`${instrumentationUrl}?second-bundle`);
  await Promise.all([
    assert.rejects(instrumentation.registerNodejs(), /Required registrar failed/),
    assert.rejects(otherInstrumentation.registerNodejs(), /Required registrar failed/),
  ]);
  assert.equal(count("factory:enter"), 1);
  assert.equal(count("stop:cloud"), 1);
  assert.equal(count("pool:close"), 1);
});

test("concurrent cross-bundle shutdown executes one cleanup and preserves failure", async () => {
  await boot.runPostgresBootstrap(async () => {});
  const gate = deferred();
  const failure = new Error("Producer drain failed");
  let calls = 0;
  const first = boot.runPostgresShutdown(async () => {
    calls++;
    await gate.promise;
    throw failure;
  });
  const second = bundle.runPostgresShutdown(async () => {
    calls++;
  });
  const firstRejection = assert.rejects(first, (error) => error === failure);
  const secondRejection = assert.rejects(second, (error) => error === failure);
  assert.equal(first, second);
  await turn();
  assert.equal(calls, 1);
  gate.resolve();
  await Promise.all([firstRejection, secondRejection]);
  assert.equal(calls, 1);
});

test("shutdown before first bootstrap remains sticky across bundles", async () => {
  let starts = 0;
  let cleanups = 0;
  await boot.runPostgresShutdown(async () => {
    cleanups++;
  });
  await assert.rejects(
    bundle.runPostgresBootstrap(async () => {
      starts++;
    }),
    /startup is stopping/
  );
  assert.equal(starts, 0);
  assert.equal(cleanups, 1);
  assert.equal(count("factory:enter"), 0);
});

test("process lifecycle stopping fences activation even before cleanup starts", async () => {
  await boot.runPostgresBootstrap(async () => {});
  markServerStopping();
  assert.throws(() => bundle.assertPostgresBootstrapActive(), /startup is stopping/);
  nativeStartupFixture.serverPhase = "starting";
  assert.throws(() => boot.assertPostgresBootstrapActive(), /startup is stopping/);
});

test("persistence identity cannot silently switch during native startup", async () => {
  await boot.runPostgresBootstrap(async () => {});
  startupFixture.identity = {};
  assert.throws(() => boot.assertPostgresBootstrapActive(), /persistence identity changed/);
});

test("a second successful registration reuses bootstrap without resetting Ready", async () => {
  process.env.OMNIROUTE_HEADLESS = "0";
  await instrumentation.registerNodejs();
  const otherInstrumentation = await import(`${instrumentationUrl}?ready-bundle`);
  await otherInstrumentation.registerNodejs();
  assert.equal(count("factory:enter"), 1);
  assert.equal(count("server:starting"), 1);
  assert.equal(count("server:ready"), 1);
  assert.equal(count("start:proxy"), 1);
  assert.equal(count("start:freeProxy"), 1);
  assert.equal(nativeStartupFixture.serverPhase, "ready");
});

test("SQLite optional startup preserves synchronous failure propagation", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  const failure = new Error("Optional startup failed synchronously");
  await assert.rejects(
    instrumentation.startOptionalBootSubsystems(
      [
        {
          name: "local",
          start: () => {
            throw failure;
          },
        },
      ],
      { env: {} }
    ),
    (error) => error === failure
  );
  assert.equal(count("factory:enter"), 0);
});
