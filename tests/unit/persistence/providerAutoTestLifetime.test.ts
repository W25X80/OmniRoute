import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { EventEmitter } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { registerHooks, syncBuiltinESMExports } from "node:module";
import { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { MockAgent, getGlobalDispatcher, setGlobalDispatcher } from "undici";
import type { NativeFileHttpTransport } from "../../../src/lib/db/nativeFileWork";

const directory = await mkdtemp(join(tmpdir(), "provider-auto-test-lifetime-"));
const saved = new Map(
  [
    "DATA_DIR",
    "OMNIROUTE_DATABASE_BACKEND",
    "APP_LOG_TO_FILE",
    "JWT_SECRET",
    "API_KEY_SECRET",
    "STORAGE_ENCRYPTION_KEY",
    "OMNIROUTE_PEER_STAMP_TOKEN",
  ].map((name) => [name, process.env[name]])
);
Object.assign(process.env, {
  DATA_DIR: directory,
  OMNIROUTE_DATABASE_BACKEND: "postgres",
  APP_LOG_TO_FILE: "false",
  JWT_SECRET: "provider-lifetime-jwt-secret",
  API_KEY_SECRET: "provider-lifetime-api-secret",
  STORAGE_ENCRYPTION_KEY: "provider-lifetime-encryption-secret",
});
delete process.env.OMNIROUTE_PEER_STAMP_TOKEN;
const networkAttempts: string[] = [];
const connect = Socket.prototype.connect;
Socket.prototype.connect = function () {
  networkAttempts.push("socket");
  throw new Error("Lifetime fixture forbids sockets");
};
const originalFetch = globalThis.fetch;
globalThis.fetch = async () => {
  networkAttempts.push("fetch");
  throw new Error("Lifetime fixture forbids fetch");
};
const dispatcher = getGlobalDispatcher();
const network = new MockAgent();
network.disableNetConnect();
const dispatch = network.dispatch.bind(network);
network.dispatch = (options, handler) => {
  networkAttempts.push("dispatcher");
  return dispatch(options, handler);
};
setGlobalDispatcher(network);
const spawnAttempts: string[] = [];
const processMethods = [
  "spawn",
  "spawnSync",
  "exec",
  "execSync",
  "execFile",
  "execFileSync",
  "fork",
] as const;
const processOriginals = processMethods.map((name) => [name, childProcess[name]] as const);
for (const name of processMethods) {
  Object.defineProperty(childProcess, name, {
    value: () => {
      spawnAttempts.push(name);
      throw new Error("Lifetime fixture forbids child processes");
    },
    configurable: true,
    writable: true,
  });
}
syncBuiltinESMExports();
const fixtureUrl = new URL(
  "../../helpers/persistence/providerAutoTestLifetimeFixture.ts",
  import.meta.url
).href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    const parent = decodeURIComponent(context.parentURL || "");
    const replace =
      (parent.endsWith("/db/persistenceRuntime.ts") &&
        [
          "./postgres/environment",
          "../artifacts/s3ArtifactConfiguration",
          "../artifacts/s3ArtifactStore",
          "@/shared/utils/circuitBreaker",
          "@/domain/quotaCache",
        ].includes(specifier)) ||
      (parent.endsWith("/postgres/controlPlane.ts") &&
        ["./connect", "./migrations"].includes(specifier)) ||
      (parent.endsWith("/api/providers/route.ts") && specifier === "@/lib/compliance/index") ||
      (parent.endsWith("/providers/[id]/test/route.ts") &&
        [
          "@/lib/providers/validation",
          "@/shared/services/cliRuntime",
          "@/lib/tokenHealthCheck",
          "@/lib/proxyLogger",
          "@/lib/exclusiveLeaseIsolation",
          "@/lib/db/settings",
        ].includes(specifier));
    return replace ? { url: fixtureUrl, shortCircuit: true } : next(specifier, context);
  },
});
const { barrier, fixture } =
  await import("../../helpers/persistence/providerAutoTestLifetimeFixture");
const runtime = await import("../../../src/lib/db/persistenceRuntime");
const { getSelectedControlPlane } = await import("../../../src/lib/db/controlPlaneContext");
const { POST, PATCH } = await import("../../../src/app/api/providers/route");
const { makeManagementSessionRequest } = await import("../../helpers/managementSession");
const { wrapRequestListenerWithPersistence } =
  await import("../../../scripts/dev/persistence-request-boundary.mjs");
type Binding = ReturnType<typeof getSelectedControlPlane>;
// Match the serving hook ABI without importing instrumentation startup into this fixture.
const transportGlobals = globalThis as typeof globalThis & {
  __omniroutePersistenceRequest:
    | ((
        run: () => unknown | Promise<unknown>,
        httpLifetime: Promise<void>,
        fileTransport?: NativeFileHttpTransport
      ) => Promise<void>)
    | undefined;
};
const previousHook = transportGlobals.__omniroutePersistenceRequest;
const originalLog = console.log;
console.log = (...args: unknown[]) => {
  if (String(args[0]).startsWith("[providers] Auto-test failed")) {
    fixture.failures.push(String(args[1]));
    fixture.events.push("auto-test-failed");
    fixture.failed.resolve();
    return;
  }
  originalLog(...args);
};

test.after(async () => {
  fixture.release.resolve();
  await runtime.closePersistenceRuntime();
  hooks.deregister();
  console.log = originalLog;
  transportGlobals.__omniroutePersistenceRequest = previousHook;
  delete globalThis.__omniroutePersistenceRuntime;
  globalThis.fetch = originalFetch;
  Socket.prototype.connect = connect;
  setGlobalDispatcher(dispatcher);
  await network.close();
  for (const [name, value] of processOriginals)
    Object.defineProperty(childProcess, name, { value });
  syncBuiltinESMExports();
  for (const [name, value] of saved) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  await rm(directory, { recursive: true, force: true });
  assert.deepEqual(networkAttempts, [], "no network attempt may escape the fixture ports");
  assert.deepEqual(spawnAttempts, [], "no host process may escape the CLI fixture port");
});

async function scenario(body: () => Promise<void>) {
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  try {
    await runtime.ensurePersistenceReady();
    await body();
  } finally {
    fixture.release.resolve();
    await runtime.closePersistenceRuntime();
    assert.deepEqual(
      fixture.unexpected,
      [],
      "unexpected SQL must not be hidden by best-effort catches"
    );
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}

/** Real transport lifetime + the production instrumentation hook's handler/lifetime join. */
async function createOverHttp(
  options: { provider?: string; local?: boolean; authorized?: boolean } = {}
) {
  const init = {
    method: "POST",
    body: {
      provider: options.provider || "openai",
      apiKey: "sk-fixture-credential",
      name: "Lifetime fixture",
    },
  };
  const request =
    options.authorized === false
      ? new Request("http://localhost/api/providers", {
          method: "POST",
          body: JSON.stringify(init.body),
          headers: { "content-type": "application/json" },
        })
      : await makeManagementSessionRequest("http://localhost/api/providers", init);
  Object.defineProperty(request, "ip", { value: options.local ? "127.0.0.1" : "8.8.8.8" });
  const responseReady = barrier();
  let response: Response;
  let requestBinding: Binding;
  let owned: Promise<void>;
  const req = Object.assign(new EventEmitter(), {
    url: "/api/providers",
    method: "POST",
    headers: {},
    complete: true,
    pause() {},
    resume() {},
  });
  const res = Object.assign(new EventEmitter(), { writableFinished: false, destroyed: false });
  transportGlobals.__omniroutePersistenceRequest = (run, lifetime) => {
    owned = runtime.runPersistenceRequest(async () => {
      requestBinding = getSelectedControlPlane();
      await run();
      await lifetime;
    });
    return owned;
  };
  const listener = wrapRequestListenerWithPersistence(
    async () => {
      response = await POST(request);
      responseReady.resolve();
    },
    { basePath: "", experimental: { caseSensitiveRoutes: false } }
  );
  listener(req, res);
  await responseReady.promise;
  fixture.events.push("http-response");
  res.writableFinished = true;
  res.emit("finish");
  await owned!;
  fixture.events.push("http-owner-ended");
  assert.throws(() => requestBinding!.assertReady(), /scope has ended/);
  assert.equal(globalThis.__omniroutePersistenceRuntime!.requests.size, 0);
  return { response: response!, requestBinding: requestBinding! };
}

async function created(options: Parameters<typeof createOverHttp>[0] = {}) {
  const result = await createOverHttp(options);
  assert.equal(result.response.status, 201, await result.response.clone().text());
  const body = (await result.response.json()) as { connection: { id: string; isActive: boolean } };
  assert.equal(body.connection.isActive, false);
  assert.equal(fixture.rows.get(body.connection.id)!.is_active, 0);
  return { ...result, id: body.connection.id };
}

async function releaseAndJoin(closing?: Promise<void>) {
  fixture.release.resolve();
  await Promise.race([fixture.resultAck.promise, fixture.failed.promise]);
  assert.deepEqual(fixture.failures, [], "probe must persist after the HTTP scope has ended");
  await runtime.drainPersistenceAcceptedWork();
  // A concurrent close may have admitted its own flush after this drain completed.
  await closing;
  assert.deepEqual(fixture.failures, [], "acknowledged result must complete its owned effects");
  assert.equal(globalThis.__omniroutePersistenceRuntime!.background.size, 0);
}

test(
  "actual POST returns 201 before a delayed probe, then persists under a fresh native owner",
  { timeout: 15000 },
  async () =>
    scenario(async () => {
      const { id, requestBinding } = await created();
      await Promise.race([fixture.entered.promise, fixture.failed.promise]);
      assert.deepEqual(fixture.failures, []);
      const pendingBackground = globalThis.__omniroutePersistenceRuntime!.background.size;
      await releaseAndJoin();
      assert.equal(pendingBackground, 1);
      assert.notEqual(fixture.validators[0].binding, requestBinding);
      assert.equal(fixture.rows.get(id)!.is_active, 1);
      assert.equal(fixture.rows.get(id)!.test_status, "active");
      assert.equal(fixture.resultWrites, 1);
      assert.deepEqual(fixture.resultBindings, [fixture.validators[0].binding]);
      assert.ok(
        fixture.reads.length >= 3,
        "initial, latest and update reads must reach the repository"
      );
      assert.ok(fixture.reads.every((binding) => binding === fixture.validators[0].binding));
      assert.equal(fixture.validators.length, 1);
      assert.ok(
        fixture.events.indexOf("http-owner-ended") < fixture.events.indexOf("probe-released")
      );
      assert.ok(fixture.events.indexOf("probe-released") < fixture.events.indexOf("result-ack"));
    })
);

test("shutdown joins the admitted probe before closing resources", { timeout: 15000 }, async () =>
  scenario(async () => {
    const { id } = await created();
    await fixture.entered.promise;
    runtime.beginPersistenceAdmissionClose();
    const closing = runtime.closePersistenceRuntime();
    await assert.rejects(
      runtime.runPersistenceRequest(async () => {}),
      /unavailable|draining/
    );
    assert.equal(globalThis.__omniroutePersistenceRuntime!.background.size, 1);
    assert.equal(globalThis.__omniroutePersistenceRuntime!.plane!.state, "ready");
    assert.equal(fixture.events.includes("pool-close"), false);
    assert.equal(fixture.events.includes("artifacts-close"), false);
    await releaseAndJoin(closing);
    assert.equal(fixture.rows.get(id)!.is_active, 1);
    assert.ok(fixture.events.indexOf("result-ack") < fixture.events.indexOf("artifacts-close"));
    assert.ok(fixture.events.indexOf("artifacts-close") < fixture.events.indexOf("pool-close"));
  })
);

test(
  "an operator disable during the delayed probe remains authoritative",
  { timeout: 15000 },
  async () =>
    scenario(async () => {
      const { id } = await created();
      await fixture.entered.promise;
      const request = await makeManagementSessionRequest("http://localhost/api/providers", {
        method: "PATCH",
        body: { ids: [id], isActive: false },
      });
      const response = await runtime.runPersistenceRequest(() => PATCH(request));
      assert.equal(response.status, 200, await response.text());
      await releaseAndJoin();
      assert.equal(fixture.rows.get(id)!.is_active, 0);
      assert.equal(fixture.rows.get(id)!.test_status, "active");
    })
);

for (const local of [false, true]) {
  test(
    `actual POST preserves ${local ? "local" : "remote"} runtime and validator spawn gates`,
    { timeout: 15000 },
    async () =>
      scenario(async () => {
        await created({ provider: "cline", local });
        await fixture.entered.promise;
        assert.deepEqual(fixture.runtimeProbes, local ? ["cline"] : []);
        assert.equal(fixture.validators[0].allowLocalSpawn, local);
        await releaseAndJoin();
      })
  );
}

test(
  "unauthenticated POST creates no connection or background probe",
  { timeout: 15000 },
  async () =>
    scenario(async () => {
      const { response } = await createOverHttp({ authorized: false });
      assert.equal(response.status, 401, await response.text());
      assert.equal(fixture.creates, 0);
      assert.equal(globalThis.__omniroutePersistenceRuntime!.background.size, 0);
      assert.deepEqual(fixture.validators, []);
      assert.deepEqual(fixture.runtimeProbes, []);
    })
);

for (const unsupported of [false, true]) {
  test(
    `delayed ${unsupported ? "unsupported" : "invalid"} probe preserves activation semantics`,
    { timeout: 15000 },
    async () =>
      scenario(async () => {
        fixture.result = {
          valid: false,
          unsupported,
          error: unsupported ? "Unsupported provider" : "Invalid API key",
        };
        const { id } = await created();
        await fixture.entered.promise;
        await releaseAndJoin();
        assert.equal(fixture.rows.get(id)!.is_active, unsupported ? 1 : 0);
        assert.equal(fixture.rows.get(id)!.test_status, unsupported ? "unknown" : "error");
      })
  );
}

test(
  "exclusive lease skips the probe without activating the new connection",
  { timeout: 15000 },
  async () =>
    scenario(async () => {
      fixture.leased = true;
      const { id } = await created();
      await runtime.drainPersistenceAcceptedWork();
      assert.deepEqual(fixture.validators, []);
      assert.equal(fixture.rows.get(id)!.is_active, 0);
      assert.equal(fixture.resultWrites, 0);
    })
);

test(
  "full background admission keeps the committed 201 without replaying the probe",
  { timeout: 15000 },
  async () =>
    scenario(async () => {
      const hold = barrier();
      const accepted = Array.from({ length: 64 }, () =>
        runtime.runPersistenceBackgroundWork(() => hold.promise)
      );
      try {
        const { id } = await created();
        await fixture.failed.promise;
        assert.deepEqual(fixture.failures, ["Persistence background admission is unavailable"]);
        assert.equal(globalThis.__omniroutePersistenceRuntime!.background.size, 64);
        assert.deepEqual(fixture.validators, []);
        assert.equal(fixture.rows.get(id)!.is_active, 0);
        assert.equal(fixture.creates, 1);
        assert.equal(fixture.resultWrites, 0);
      } finally {
        hold.resolve();
        await Promise.all(accepted);
      }
    })
);
