import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";
import type { ServiceConfig } from "../../../src/lib/services/types.ts";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/runtimeOwnedWorkFixture.ts", import.meta.url)
  .href;
const servicesFixtureUrl = new URL(
  "../../helpers/persistence/nativeEmbeddedServicesFixture.ts",
  import.meta.url
).href;
const sourceUrl = (name: string) =>
  new URL(`../../../src/lib/services/${name}.ts`, import.meta.url).href;
const dependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const serviceOwners = new Set(
  ["ServiceSupervisor", "bootstrap", "modelSync", "ringBuffer"].map(sourceUrl)
);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (parent === sourceUrl("bootstrap") && specifier === "./nativeLifecycle")
      return { url: servicesFixtureUrl, shortCircuit: true };
    if (parent === runtimeUrl && specifier === "./postgres/catalogSnapshot")
      return { url: servicesFixtureUrl, shortCircuit: true };
    if (parent === runtimeUrl && dependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (parent && serviceOwners.has(parent)) {
      if (
        [
          "@/lib/db/versionManager",
          "@/lib/db/serviceModels",
          "@/lib/db/settings",
          "node:child_process",
          "node:fs",
          "node:fs/promises",
          "./apiKey",
          "./portProbe",
          "./providerPlugins/registry",
        ].includes(specifier) ||
        specifier.startsWith("./installers/")
      )
        return { url: servicesFixtureUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const previous = {
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  jwt: process.env.JWT_SECRET,
  key: process.env.API_KEY_SECRET,
  fetch: globalThis.fetch,
  lifecycle: globalThis.__omnirouteServerLifecycle,
};
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const { fixture, barrier } = await import("../../helpers/persistence/runtimeOwnedWorkFixture.ts");
const { serviceFixture, FakeServiceChild } =
  await import("../../helpers/persistence/nativeEmbeddedServicesFixture.ts");
const runtime = await import("../../../src/lib/db/persistenceRuntime.ts");
const { ServiceSupervisor } = await import("../../../src/lib/services/ServiceSupervisor.ts");
const { RingBuffer } = await import("../../../src/lib/services/ringBuffer.ts");
const { HealthChecker } = await import("../../../src/lib/services/healthCheck.ts");
const lifecycle = await import("../../../src/lib/services/nativeLifecycle.ts");
const signals = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
const registry = await import("../../../src/lib/services/registry.ts");
const duplicate = await import(`${sourceUrl("registry")}?second-native-bundle`);
const bootstrap = await import("../../../src/lib/services/bootstrap.ts");
const models = await import("../../../src/lib/services/modelSync.ts");
const ownerSymbol = Symbol.for("omniroute.nativeEmbeddedServices");
const turn = () => new Promise<void>((done) => setImmediate(done));
async function promises() {
  for (let i = 0; i < 50; i++) await Promise.resolve();
}
async function callbacks() {
  const owner = lifecycle.getNativeEmbeddedServices();
  while (owner?.callbacks.size) await Promise.allSettled(Array.from(owner.callbacks));
}
async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  delete globalThis.__omnirouteServerLifecycle;
  Reflect.deleteProperty(globalThis, ownerSymbol);
  fixture.reset();
  serviceFixture.reset();
  globalThis.fetch = async () => new Response("{}", { status: 200 });
  try {
    await runtime.ensurePersistenceReady();
    await body();
  } finally {
    fixture.releaseBarriers();
    serviceFixture.rowBarrier = undefined;
    serviceFixture.statusBarrier = undefined;
    serviceFixture.writeBarrier = undefined;
    for (const child of serviceFixture.children) {
      if (child instanceof FakeServiceChild && child.signalCode === null) child.ackExit("SIGKILL");
      else if (
        !(child instanceof FakeServiceChild) &&
        child.exitCode === null &&
        child.signalCode === null
      ) {
        await new Promise<void>((done) => {
          child.once("exit", done);
          child.kill("SIGKILL");
        });
      }
    }
    await runtime.runPersistenceShutdownWork(() => lifecycle.stopNativeEmbeddedServices());
    await callbacks();
    await runtime.drainPersistenceAcceptedWork();
    await runtime.runPersistenceShutdownWork(() => lifecycle.disposeNativeEmbeddedServiceBuffers());
    await runtime.closePersistenceRuntime();
    Reflect.deleteProperty(globalThis, ownerSymbol);
  }
}
const config = (overrides: Partial<ServiceConfig> = {}): ServiceConfig => ({
  tool: "fixture",
  port: 0,
  spawnArgs: () => ({ command: process.execPath, args: [], env: process.env, cwd: process.cwd() }),
  healthUrl: () => "http://fixture.invalid/health",
  healthIntervalMs: 500,
  stopTimeoutMs: 50,
  logsBufferBytes: 4096,
  ...overrides,
});
async function startFake(t: TestContext, supervisor: InstanceType<typeof ServiceSupervisor>) {
  const start = supervisor.start();
  await promises();
  t.mock.timers.tick(500);
  await promises();
  t.mock.timers.tick(500);
  await promises();
  return start;
}
test.after(() => {
  hooks.deregister();
  fixture.reset();
  globalThis.fetch = previous.fetch;
  delete globalThis.__omniroutePersistenceRuntime;
  globalThis.__omnirouteServerLifecycle = previous.lifecycle;
  for (const [key, value] of [
    ["OMNIROUTE_DATABASE_BACKEND", previous.backend],
    ["JWT_SECRET", previous.jwt],
    ["API_KEY_SECRET", previous.key],
  ] as const) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test("cold native embedded getters and close do not construct an owner or install signal handlers", async () => {
  Reflect.deleteProperty(globalThis, ownerSymbol);
  assert.equal(registry.getSupervisor("unused"), null);
  await lifecycle.stopNativeEmbeddedServices();
  await lifecycle.disposeNativeEmbeddedServiceBuffers();
  assert.equal(Reflect.has(globalThis, ownerSymbol), false);
  assert.deepEqual([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")], signals);
});
test("native registry shares actual supervisors across bundles and rejects foreign identity", () =>
  scenario(async () => {
    const supervisor = new ServiceSupervisor(config());
    registry.registerSupervisor(supervisor);
    assert.equal(duplicate.getSupervisor("fixture"), supervisor);
    const identity = globalThis.__omniroutePersistenceRuntime!.applicationIdentity;
    globalThis.__omniroutePersistenceRuntime!.applicationIdentity = Object.freeze({});
    try {
      assert.throws(() => duplicate.getSupervisor("fixture"), /identity changed/);
    } finally {
      globalThis.__omniroutePersistenceRuntime!.applicationIdentity = identity;
    }
  }));
test("native owned child escalates after the unchanged deadline and waits for actual exit ACK", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    serviceFixture.childFactory = () => {
      const child = new FakeServiceChild();
      child.ignoreTerm = true;
      child.delayExit = true;
      return child;
    };
    const supervisor = new ServiceSupervisor(config());
    registry.registerSupervisor(supervisor);
    assert.equal((await startFake(t, supervisor)).state, "running");
    const child = serviceFixture.children[0] as InstanceType<typeof FakeServiceChild>;
    let stopped = false;
    const stop = supervisor.stop().then((value) => {
      stopped = true;
      return value;
    });
    await promises();
    assert.deepEqual(child.signals, ["SIGTERM"]);
    assert.equal(child.killed, true);
    t.mock.timers.tick(50);
    await promises();
    assert.deepEqual(child.signals, ["SIGTERM", "SIGKILL"]);
    assert.equal(stopped, false);
    assert.equal(supervisor.getStatus().state, "stopping");
    assert.equal(supervisor.getStatus().pid, child.pid);
    child.ackExit("SIGKILL");
    assert.equal((await stop).state, "stopped");
  }));
test("native actual local child ignoring SIGTERM is reaped before stopped status", () =>
  scenario(async () => {
    serviceFixture.realChild = true;
    const supervisor = new ServiceSupervisor(
      config({
        healthIntervalMs: 5,
        spawnArgs: () => ({
          command: process.execPath,
          args: [
            "-e",
            'process.on("SIGTERM", () => {}); console.log("child-ready"); setInterval(() => {}, 1000);',
          ],
          env: process.env,
          cwd: process.cwd(),
        }),
      })
    );
    registry.registerSupervisor(supervisor);
    await supervisor.start();
    await serviceFixture.realReady.promise;
    const child = serviceFixture.children[0];
    assert.equal((await supervisor.stop()).state, "stopped");
    assert.equal(child.signalCode, "SIGKILL");
    assert.equal(supervisor.getStatus().pid, null);
  }));
test("native bootstrap child event and state-change effects use fresh catalog scopes and drain", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    serviceFixture.installed.add("9router");
    await runtime.runPersistenceRequest(() => bootstrap.bootstrapEmbeddedServices());
    const initializationGeneration = Math.max(
      ...serviceFixture.calls.map((call) => call.generation)
    );
    const supervisor = registry.getSupervisor("9router")!;
    await startFake(t, supervisor);
    await callbacks();
    const gate = barrier(),
      entered = barrier();
    serviceFixture.statusBarrier = gate.promise;
    serviceFixture.statusEntered = entered.resolve;
    const external = new AsyncResource("native-child-event");
    try {
      external.runInAsyncScope(() =>
        (serviceFixture.children[0] as InstanceType<typeof FakeServiceChild>).ackExit("SIGTERM")
      );
    } finally {
      external.emitDestroy();
    }
    await promises();
    const eventCall = serviceFixture.calls.find((call) => call.operation === "status:error");
    assert.ok(eventCall && eventCall.generation > initializationGeneration);
    await entered.promise;
    let drained = false;
    const drain = runtime.drainPersistenceAcceptedWork().then(() => {
      drained = true;
    });
    await turn();
    assert.equal(drained, false);
    gate.resolve();
    await drain;
    assert.ok(
      serviceFixture.calls.some(
        (call) => call.operation === "unavailable" && call.generation > initializationGeneration
      )
    );
  }));
test("native close fences a delayed start before spawn and memoizes stop across bundles", () =>
  scenario(async () => {
    const gate = barrier();
    serviceFixture.rowBarrier = gate.promise;
    const supervisor = new ServiceSupervisor(config());
    registry.registerSupervisor(supervisor);
    const start = supervisor.start();
    await turn();
    runtime.beginPersistenceAdmissionClose();
    const stop = runtime.runPersistenceShutdownWork(() => lifecycle.stopNativeEmbeddedServices());
    gate.resolve();
    await assert.rejects(start, /stopping/);
    await stop;
    assert.equal(serviceFixture.children.length, 0);
    const second = await import(`${sourceUrl("nativeLifecycle")}?second-native-bundle`);
    assert.equal(lifecycle.stopNativeEmbeddedServices(), second.stopNativeEmbeddedServices());
  }));
test("native health stop aborts transport, joins real settlement and suppresses stale callback", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    const gate = barrier(),
      entered = barrier();
    let signal: AbortSignal | undefined;
    let changes = 0;
    globalThis.fetch = async (_url, init) => {
      signal = init?.signal ?? undefined;
      entered.resolve();
      await gate.promise;
      return new Response("{}", { status: 200 });
    };
    const checker = new HealthChecker(
      () => "http://fixture.invalid",
      10,
      () => {
        changes++;
      }
    );
    checker.start();
    t.mock.timers.tick(10);
    await entered.promise;
    let closed = false;
    const close = checker.stopAsync().then(() => {
      closed = true;
    });
    await turn();
    assert.equal(signal?.aborted, true);
    assert.equal(closed, false);
    gate.resolve();
    await close;
    assert.equal(changes, 0);
  }));
test("native immediate model sync owns its delayed body and stop joins prefix persistence", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    const gate = barrier(),
      entered = barrier();
    let calls = 0;
    globalThis.fetch = async () => {
      calls++;
      entered.resolve();
      await gate.promise;
      return new Response(JSON.stringify({ data: [{ id: "model" }] }));
    };
    await runtime.runPersistenceRequest(async () =>
      models.scheduleServiceModelSync("fixture", "http://fixture.invalid", "key", 100)
    );
    await entered.promise;
    let stopped = false;
    const stop = models.stopServiceModelSyncAsync("fixture").then(() => {
      stopped = true;
    });
    t.mock.timers.tick(1000);
    await turn();
    assert.equal(calls, 1);
    assert.equal(stopped, false);
    gate.resolve();
    await stop;
    assert.equal(serviceFixture.models.get("fixture")?.[0].id, "fixture/model");
    assert.ok(serviceFixture.calls.every((call) => call.generation > 0));
  }));
test("native file flush yields, serializes writes, and final disposal joins the accepted write", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    const directory = await mkdtemp(path.join(tmpdir(), "native-service-buffer-"));
    const file = path.join(directory, "service.log"),
      buffer = new RingBuffer(100);
    buffer.setFlushPath(file);
    buffer.push({ ts: 1, stream: "stdout", line: "first" });
    const gate = barrier(),
      entered = barrier();
    serviceFixture.writeBarrier = gate.promise;
    serviceFixture.writeEntered = entered.resolve;
    t.mock.timers.tick(60000);
    await entered.promise;
    buffer.push({ ts: 2, stream: "stderr", line: "second" });
    t.mock.timers.tick(60000);
    await promises();
    assert.equal(serviceFixture.fileWrites, 1);
    let closed = false;
    const closing = buffer.disposeAsync().then(() => {
      closed = true;
    });
    try {
      await turn();
      assert.equal(closed, false);
      gate.resolve();
      await closing;
      assert.equal(await readFile(file, "utf8"), "1 [stdout] first\n2 [stderr] second");
      assert.equal(serviceFixture.syncWrites, 0);
      assert.equal(serviceFixture.fileWrites, 2);
      buffer.push({ ts: 3, stream: "stdout", line: "after" });
      t.mock.timers.tick(60000);
      await promises();
      assert.equal(serviceFixture.fileWrites, 2);
    } finally {
      gate.resolve();
      await closing;
      await rm(directory, { recursive: true, force: true });
    }
  }));

test("native stop leaves an adopted host process untouched", (t) =>
  scenario(async () => {
    serviceFixture.probe = { healthy: true, portInUse: true };
    serviceFixture.adoptEnabled = true;
    let kills = 0;
    t.mock.method(process, "kill", () => {
      kills++;
      return true;
    });
    const supervisor = new ServiceSupervisor(config({ probeBeforeSpawn: true }));
    registry.registerSupervisor(supervisor);
    const started = await supervisor.start();
    assert.equal(started.adopted, true);
    assert.equal(started.pid, 42424);
    assert.equal(serviceFixture.children.length, 0);
    await supervisor.stop();
    assert.equal(kills, 0);
  }));

test("concurrent native bootstrap bundles retain one supervisor and one transition listener", () =>
  scenario(async () => {
    serviceFixture.installed.add("9router");
    const duplicateBootstrap = await import(`${sourceUrl("bootstrap")}?second-native-bundle`);
    await Promise.all([
      runtime.runPersistenceRequest(() => bootstrap.bootstrapEmbeddedServices()),
      runtime.runPersistenceRequest(() => duplicateBootstrap.bootstrapEmbeddedServices()),
    ]);
    const supervisor = registry.getSupervisor("9router")!;
    assert.equal(lifecycle.getNativeEmbeddedServices()!.supervisors.size, 1);
    assert.equal(supervisor.listenerCount("stateChange"), 1);
    assert.throws(
      () => registry.registerSupervisor(new ServiceSupervisor(config({ tool: "9router" }))),
      /already registered/
    );
    assert.equal(registry.getSupervisor("9router"), supervisor);
  }));

test("native event refusal after admission close has no late status side effect", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    const supervisor = new ServiceSupervisor(config());
    registry.registerSupervisor(supervisor);
    await startFake(t, supervisor);
    const warnings: string[] = [];
    t.mock.method(console, "warn", (value: string) => warnings.push(value));
    runtime.beginPersistenceAdmissionClose();
    const external = new AsyncResource("native-late-child-event");
    try {
      external.runInAsyncScope(() =>
        (serviceFixture.children[0] as InstanceType<typeof FakeServiceChild>).ackExit("SIGTERM")
      );
    } finally {
      external.emitDestroy();
    }
    await callbacks();
    assert.equal(
      serviceFixture.calls.some((call) => call.operation === "status:error"),
      false
    );
    assert.ok(warnings.some((warning) => warning.includes("Background work failed")));
  }));

test("SQLite control retains synchronous buffer writes and its original supervisor signal path", (t) =>
  scenario(async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "sqlite-service-control-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    const before = { SIGINT: process.listeners("SIGINT"), SIGTERM: process.listeners("SIGTERM") };
    try {
      t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
      const controlRegistry = await import(`${sourceUrl("registry")}?sqlite-control-bundle`);
      for (const signal of ["SIGINT", "SIGTERM"] as const)
        assert.equal(process.listenerCount(signal), before[signal].length + 1);
      const supervisor = new ServiceSupervisor(config());
      controlRegistry.registerSupervisor(supervisor);
      assert.equal((await startFake(t, supervisor)).state, "running");
      assert.equal((await supervisor.stop()).state, "stopped");
      controlRegistry.unregisterSupervisor("fixture");
      const file = path.join(directory, "service.log"),
        buffer = new RingBuffer(100);
      buffer.setFlushPath(file);
      buffer.push({ ts: 1, stream: "stdout", line: "control" });
      t.mock.timers.tick(60000);
      buffer.dispose();
      assert.equal(await readFile(file, "utf8"), "1 [stdout] control");
      assert.equal(serviceFixture.syncWrites, 1);
      assert.equal(serviceFixture.fileWrites, 0);
    } finally {
      for (const signal of ["SIGINT", "SIGTERM"] as const)
        for (const listener of process.listeners(signal))
          if (!before[signal].includes(listener)) process.removeListener(signal, listener);
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      await rm(directory, { recursive: true, force: true });
    }
  }));

test("delayed old child exit cannot clear the child created by a later native restart", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    const supervisor = new ServiceSupervisor(config());
    registry.registerSupervisor(supervisor);
    await startFake(t, supervisor);
    await callbacks();
    const old = serviceFixture.children[0] as InstanceType<typeof FakeServiceChild>;
    const gate = barrier(),
      entered = barrier();
    serviceFixture.nextCatalogBarrier = gate.promise;
    serviceFixture.catalogEntered = entered.resolve;
    old.ackExit("SIGTERM");
    await entered.promise;
    await supervisor.stop();
    await startFake(t, supervisor);
    const current = serviceFixture.children[1];
    gate.resolve();
    await callbacks();
    assert.equal(supervisor.getStatus().state, "running");
    assert.equal(supervisor.getStatus().pid, current.pid);
    assert.equal(
      serviceFixture.calls.some((call) => call.operation === "status:error"),
      false
    );
  }));

test("queued native health-unknown cannot overwrite a restarted healthy generation", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    const changes: string[] = [];
    const restartedHealthy = barrier();
    const checker = new HealthChecker(
      () => "http://fixture.invalid",
      500,
      (health) => {
        changes.push(health);
        if (health === "healthy" && changes.filter((value) => value === "healthy").length === 2)
          restartedHealthy.resolve();
      }
    );
    checker.start();
    t.mock.timers.tick(500);
    await callbacks();
    const gate = barrier(),
      entered = barrier();
    serviceFixture.nextCatalogBarrier = gate.promise;
    serviceFixture.catalogEntered = entered.resolve;
    checker.stop();
    await entered.promise;
    checker.start();
    t.mock.timers.tick(500);
    await restartedHealthy.promise;
    gate.resolve();
    await callbacks();
    assert.equal(changes.at(-1), "healthy");
    assert.equal(checker.getHealth(), "healthy");
    await checker.stopAsync();
  }));

test("native model timer stops at the source transition and stale error effects cannot stop a restart", (t) =>
  scenario(async () => {
    t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
    serviceFixture.installed.add("9router");
    await runtime.runPersistenceRequest(() => bootstrap.bootstrapEmbeddedServices());
    const supervisor = registry.getSupervisor("9router")!;
    await startFake(t, supervisor);
    await callbacks();
    const gate = barrier(),
      entered = barrier();
    serviceFixture.stateEffectEntered = entered.resolve;
    supervisor.prependListener("stateChange", (status: { state: string }) => {
      if (status.state === "error") serviceFixture.nextStateEffectBarrier = gate.promise;
    });
    (serviceFixture.children[0] as InstanceType<typeof FakeServiceChild>).ackExit("SIGTERM");
    await entered.promise;
    assert.equal(lifecycle.getNativeEmbeddedServices()!.modelTimers.has("9router"), false);
    await startFake(t, supervisor);
    await promises();
    const unavailableBeforeRelease = serviceFixture.calls.filter(
      (call) => call.operation === "unavailable"
    ).length;
    gate.resolve();
    await callbacks();
    assert.equal(supervisor.getStatus().state, "running");
    assert.equal(lifecycle.getNativeEmbeddedServices()!.modelTimers.has("9router"), true);
    assert.equal(
      serviceFixture.calls.filter((call) => call.operation === "unavailable").length,
      unavailableBeforeRelease
    );
  }));
