import assert from "node:assert/strict";
import { once } from "node:events";
import type { ChildProcess } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const sourceUrl = (name: string) => new URL(`../../../${name}`, import.meta.url).href;
const runtimeUrl = sourceUrl("src/lib/db/persistenceRuntime.ts");
const shutdownUrl = sourceUrl("src/lib/startup/postgresShutdown.ts");
const serviceUrl = (name: string) => sourceUrl(`src/lib/services/${name}.ts`);
const runtimeFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const serviceFixtureUrl = new URL(
  "../../helpers/persistence/nativeEmbeddedServicesFixture.ts",
  import.meta.url
).href;
const shutdownFixtureUrl = new URL(
  "../../helpers/persistence/embeddedShutdownFixture.ts",
  import.meta.url
).href;
const runtimeDependencies = new Set([
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
    const parent = context.parentURL?.split("?")[0];
    if (parent === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: runtimeFixtureUrl, shortCircuit: true };
    if (
      parent === shutdownUrl &&
      ![
        "@/lib/db/persistenceRuntime",
        "./postgresBootstrap",
        "@/lib/services/nativeLifecycle",
      ].includes(specifier)
    )
      return { url: shutdownFixtureUrl, shortCircuit: true };
    if (
      parent === serviceUrl("ServiceSupervisor") &&
      ["node:child_process", "@/lib/db/versionManager", "./portProbe"].includes(specifier)
    )
      return { url: serviceFixtureUrl, shortCircuit: true };
    if (
      parent === sourceUrl("open-sse/executors/ninerouter.ts") &&
      specifier === "@/lib/services/apiKey"
    )
      return { url: serviceFixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const previous = {
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  jwt: process.env.JWT_SECRET,
  key: process.env.API_KEY_SECRET,
  fetch: globalThis.fetch,
  request: globalThis.__omniroutePersistenceRequest,
  lifecycle: globalThis.__omnirouteServerLifecycle,
};
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const { fixture, barrier } = await import("../../helpers/persistence/runtimeOwnedWorkFixture.ts");
const { serviceFixture } =
  await import("../../helpers/persistence/nativeEmbeddedServicesFixture.ts");
const runtime = await import("../../../src/lib/db/persistenceRuntime.ts");
const shutdown = await import("../../../src/lib/startup/postgresShutdown.ts");
const lifecycle = await import("../../../src/lib/services/nativeLifecycle.ts");
const { ServiceSupervisor } = await import("../../../src/lib/services/ServiceSupervisor.ts");
const registry = await import("../../../src/lib/services/registry.ts");
const { NineRouterExecutor } = await import("../../../open-sse/executors/ninerouter.ts");
const { wrapRequestListenerWithPersistence } =
  await import("../../../scripts/dev/persistence-request-boundary.mjs");
const embeddedSymbol = Symbol.for("omniroute.nativeEmbeddedServices");
const startupSymbol = Symbol.for("omniroute.postgresStartupOwner");
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

async function listen(server: Server): Promise<number> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return address.port;
}

test.after(() => {
  hooks.deregister();
  delete globalThis.__omniroutePersistenceRuntime;
  globalThis.__omnirouteServerLifecycle = previous.lifecycle;
  globalThis.__omniroutePersistenceRequest = previous.request;
  globalThis.fetch = previous.fetch;
  Reflect.deleteProperty(globalThis, embeddedSymbol);
  Reflect.deleteProperty(globalThis, startupSymbol);
  for (const [name, value] of [
    ["OMNIROUTE_DATABASE_BACKEND", previous.backend],
    ["JWT_SECRET", previous.jwt],
    ["API_KEY_SECRET", previous.key],
  ] as const) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("cold embedded admission close is idempotent and allocates no owner", () => {
  Reflect.deleteProperty(globalThis, embeddedSymbol);
  lifecycle.beginNativeEmbeddedServicesClose();
  lifecycle.beginNativeEmbeddedServicesClose();
  assert.equal(Reflect.has(globalThis, embeddedSymbol), false);
});

// Real runtime, HTTP lifetime boundary, NineRouter executor, coordinator and harmless child.
// SQL/bootstrap and unrelated producer transports are the explicit controlled seams.
test(
  "accepted NineRouter HTTP fetch completes before child teardown and shared pool close",
  { timeout: 15000 },
  async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "omniroute-embedded-drain-"));
    const childFile = path.join(directory, "local-provider.cjs");
    await writeFile(
      childFile,
      `const http = require("node:http");
let held;
const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/release") {
    held.end(JSON.stringify({choices:[{message:{content:"accepted reply"}}]}));
    held = undefined; res.end("released"); return;
  }
  if (req.url === "/v1/chat/completions" && req.method === "POST") {
    let body = ""; req.on("data", chunk => body += chunk);
    req.on("end", () => {
      if (JSON.parse(body).model !== "fixture-model" || req.headers.authorization !== "Bearer fixture-service-key") {
        res.statusCode = 400; res.end("unexpected executor projection"); return;
      }
      held = res;
      res.setHeader("Content-Type", "application/json");
      console.log("request-held");
    }); return;
  }
  res.end("healthy");
});
process.on("SIGTERM", () => {
  console.log("child-terminated");
  server.closeAllConnections();
  server.close(() => process.exit(0));
});
server.listen(Number(process.argv[2]), "127.0.0.1", () => console.log("child-ready"));
`
    );
    const reservation = createServer();
    const childPort = await listen(reservation).catch(async (error) => {
      await rm(directory, { recursive: true, force: true });
      throw error;
    });
    await new Promise<void>((resolve) => reservation.close(() => resolve()));
    const frontend = createServer();
    let child: ChildProcess | undefined;
    let accepted: Promise<Response> | undefined;
    let closing: Promise<void> | undefined;
    delete globalThis.__omniroutePersistenceRuntime;
    delete globalThis.__omnirouteServerLifecycle;
    Reflect.deleteProperty(globalThis, embeddedSymbol);
    Reflect.deleteProperty(globalThis, startupSymbol);
    fixture.reset();
    serviceFixture.reset();
    serviceFixture.realChild = true;
    globalThis.fetch = ((input, init) =>
      String(input) === "http://fixture.invalid/health"
        ? Promise.resolve(new Response("healthy"))
        : previous.fetch(input, init)) as typeof fetch;
    const held = barrier();
    try {
      await runtime.ensurePersistenceReady();
      const supervisor = new ServiceSupervisor({
        tool: "9router",
        port: childPort,
        spawnArgs: () => ({
          command: process.execPath,
          args: [childFile, String(childPort)],
          env: process.env,
          cwd: directory,
        }),
        healthUrl: () => "http://fixture.invalid/health",
        healthIntervalMs: 5,
        stopTimeoutMs: 100,
        logsBufferBytes: 4096,
      });
      registry.registerSupervisor(supervisor);
      const buffer = supervisor.getRingBuffer();
      const dispose = buffer.disposeAsync.bind(buffer);
      buffer.disposeAsync = async () => {
        fixture.events.push("buffers:closing");
        await dispose();
        fixture.events.push("buffers:closed");
      };
      const owner = lifecycle.getNativeEmbeddedServices()!;
      let ticks = 0;
      const timer = setInterval(() => ticks++, 1000);
      owner.modelTimers.set("9router", timer);
      await supervisor.start();
      child = serviceFixture.children[0] as ChildProcess;
      child.stdout?.on("data", (data: Buffer) => {
        if (String(data).includes("request-held")) held.resolve();
      });
      child.once("exit", () => fixture.events.push("child:exit"));
      await serviceFixture.realReady.promise;
      globalThis.__omniroutePersistenceRequest = async (run, lifetime) => {
        await runtime.runPersistenceRequest(async () => {
          await run();
          await lifetime;
        });
      };
      const executor = new NineRouterExecutor();
      frontend.on(
        "request",
        wrapRequestListenerWithPersistence(async (request, response) => {
          const chunks: Buffer[] = [];
          for await (const chunk of request) chunks.push(Buffer.from(chunk));
          const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          const result = await executor.execute({
            model: "9router/fixture-model",
            body,
            stream: false,
            credentials: {},
          });
          const reply = await result.response.text();
          fixture.events.push("accepted:reply");
          response.statusCode = result.response.status;
          response.end(reply);
        })
      );
      const frontendPort = await listen(frontend);
      const url = `http://127.0.0.1:${frontendPort}/v1/chat/completions`;
      const body = JSON.stringify({
        model: "9router/fixture-model",
        messages: [{ role: "user", content: "hi" }],
      });
      accepted = previous.fetch(url, { method: "POST", body }).then((response) => {
        fixture.events.push(`accepted:http:${response.status}`);
        return response;
      });
      void accepted.catch(() => {});
      await held.promise;
      assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 1);
      closing = shutdown.shutdownPostgresApplication();
      void closing.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      const rejected = await previous.fetch(url, { method: "POST", body });
      assert.equal(rejected.status, 503);
      await rejected.text();
      for (let count = 0; count < 30 && !owner.closing; count++) await turn();
      assert.equal(owner.closing, true);
      assert.equal(owner.modelTimers.size, 0);
      const ticksAtClose = ticks;
      await new Promise<void>((resolve) => setTimeout(resolve, 1100));
      assert.equal(ticks, ticksAtClose);
      assert.equal(
        supervisor.getStatus().state,
        "running",
        `Accepted local-provider capability removed before drain: ${fixture.events.join(",")}`
      );
      assert.equal(child.exitCode, null);
      assert.equal(child.signalCode, null);
      assert.equal(child.killed, false);
      assert.equal(fixture.poolCloses, 0);
      assert.ok(fixture.events.includes("producer:stop"));
      await assert.rejects(supervisor.start(), /stopping/);
      const release = await previous.fetch(`http://127.0.0.1:${childPort}/release`);
      await release.text();
      const response = await accepted;
      assert.equal(response.status, 200);
      assert.equal((await response.json()).choices[0].message.content, "accepted reply");
      await closing;
      assert.equal(supervisor.getStatus().state, "stopped");
      assert.equal(child.signalCode, null);
      assert.equal(child.exitCode, 0);
      assert.ok(fixture.events.indexOf("accepted:reply") < fixture.events.indexOf("child:exit"));
      assert.ok(fixture.events.indexOf("child:exit") < fixture.events.indexOf("pool-close"));
      assert.ok(fixture.events.indexOf("child:exit") < fixture.events.indexOf("buffers:closing"));
      assert.ok(fixture.events.indexOf("buffers:closed") < fixture.events.indexOf("pool-close"));
      assert.equal(fixture.poolCloses, 1);
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
    } finally {
      fixture.releaseBarriers();
      if (child && child.exitCode === null && child.signalCode === null) {
        const exited = once(child, "exit");
        child.kill("SIGKILL");
        await exited;
      }
      frontend.closeAllConnections();
      await new Promise<void>((resolve) => frontend.close(() => resolve()));
      await Promise.allSettled([accepted, closing].filter(Boolean));
      if (runtime.getPersistenceRuntimeStatus().phase !== "closed") {
        await runtime.runPersistenceShutdownWork(() => lifecycle.stopNativeEmbeddedServices());
        await runtime.drainPersistenceAcceptedWork();
        await runtime.runPersistenceShutdownWork(() =>
          lifecycle.disposeNativeEmbeddedServiceBuffers()
        );
        await runtime.closePersistenceRuntime();
      }
      await rm(directory, { recursive: true, force: true });
      Reflect.deleteProperty(globalThis, embeddedSymbol);
      Reflect.deleteProperty(globalThis, startupSymbol);
    }
  }
);
