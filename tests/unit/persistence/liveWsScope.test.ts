import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { realpathSync } from "node:fs";
import type { IncomingMessage, Server } from "node:http";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { liveFixture, WebSocket } from "../../helpers/persistence/liveWsScopeFixture.ts";

const liveUrl = new URL("../../../src/server/ws/liveServer.ts", import.meta.url).href;
const runtimeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
).href;
const fixtureUrl = new URL("../../helpers/persistence/liveWsScopeFixture.ts", import.meta.url).href;
const runtimeDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const liveDependencies = new Set([
  "ws",
  "http",
  "@/shared/utils/dashboardSessionToken",
  "@/lib/events/eventBus",
  "@/shared/utils/testProcess",
  "@/server/authz/proxyHeaders",
  "@/lib/startup/nonLoopbackApiKeyGuard",
  "@/shared/utils/httpClientAbortGuard.mjs",
  "@/lib/db/settings",
  "../../sse/services/auth.ts",
  "@/lib/db/compressionAnalytics",
  "@omniroute/open-sse/utils/error",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (context.parentURL === liveUrl && liveDependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (context.parentURL === liveUrl && specifier === "@/lib/db/backendSelection")
      return nextResolve(
        new URL("../../../src/lib/db/backendSelection.ts", import.meta.url).href,
        context
      );
    if (context.parentURL === liveUrl && specifier === "@/lib/db/persistenceRuntime")
      return { url: runtimeUrl, shortCircuit: true };
    if (context.parentURL === liveUrl && specifier === "@/lib/events/types")
      return nextResolve(
        new URL("../../../src/lib/events/types.ts", import.meta.url).href,
        context
      );
    return nextResolve(specifier, context);
  },
});
const saved = new Map(
  ["OMNIROUTE_DATABASE_BACKEND", "OMNIROUTE_ENABLE_LIVE_WS", "JWT_SECRET", "API_KEY_SECRET"].map(
    (name) => [name, process.env[name]]
  )
);
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.OMNIROUTE_ENABLE_LIVE_WS = "0";
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const live = await import("../../../src/server/ws/liveServer.ts");
const request = {
  headers: { origin: "http://localhost:20128", authorization: "Bearer test-api-key" },
  socket: { remoteAddress: "127.0.0.1" },
} as unknown as IncomingMessage;
const observe = <T>(work: Promise<T>): Promise<T> => {
  void work.catch(() => {});
  return work;
};
let ownedServer: Server | undefined;
async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  liveFixture.reset();
  ownedServer = undefined;
  try {
    await body();
  } finally {
    fixture.releaseBarriers();
    if (ownedServer) await live.stopLiveDashboardServer(ownedServer).catch(() => {});
    await runtime.closePersistenceRuntime().catch(() => {});
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}
async function boot() {
  await runtime.ensurePersistenceReady();
  let resource!: AsyncResource;
  ownedServer = await runtime.runPersistenceRequest(async () => {
    resource = new AsyncResource("live-ws-ended-boot");
    return live.startLiveDashboardServer(20132, "127.0.0.1");
  });
  assert.throws(() => resource.runInAsyncScope(() => getSelectedControlPlane()), /scope ended/);
  return resource;
}
function emitConnection(resource: AsyncResource, socket: WebSocket) {
  resource.runInAsyncScope(() => liveFixture.wss!.emit("connection", socket, request));
}
test.after(() => {
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  for (const [name, value] of saved) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test(
  "LiveWS auth and frame callbacks obtain fresh scopes from an ended boot resource",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const resource = await boot();
      const welcome = barrier();
      liveFixture.sent = (message) => {
        if (message.type === "welcome") welcome.resolve();
      };
      const socket = new WebSocket();
      try {
        emitConnection(resource, socket);
        resource.runInAsyncScope(() =>
          socket.emit("message", Buffer.from('{"type":"subscribe","channels":["requests"]}'))
        );
        await welcome.promise;
        assert.equal(liveFixture.latestReads, 1);
        assert.deepEqual(liveFixture.authGenerations, [4]);
        assert.equal(fixture.generation, 5);
        assert.equal(socket.sent[0].type, "welcome");
        assert.equal(socket.sent[0].backlog, 0);
        const stopped = live.stopLiveDashboardServer(ownedServer!);
        assert.equal(live.stopLiveDashboardServer(ownedServer!), stopped);
        await stopped;
        assert.equal(liveFixture.httpCloses, 1);
        assert.equal(liveFixture.wsCloses, 1);
        assert.equal(liveFixture.unsubscribes, 1);
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
      } finally {
        resource.emitDestroy();
      }
    })
);

test(
  "stop closes admission and drains accepted auth plus its buffered frame before transports",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const resource = await boot();
      const authEntered = barrier(),
        authDone = barrier(),
        frameEntered = barrier(),
        frameDone = barrier();
      liveFixture.authEntered = authEntered.resolve;
      liveFixture.authBarrier = authDone.promise;
      const socket = new WebSocket();
      try {
        emitConnection(resource, socket);
        resource.runInAsyncScope(() =>
          socket.emit("message", Buffer.from('{"type":"subscribe","channels":["requests"]}'))
        );
        await authEntered.promise;
        const stopping = observe(live.stopLiveDashboardServer(ownedServer!));
        const rejected = new WebSocket();
        emitConnection(resource, rejected);
        assert.equal(rejected.closeCode, 1001);
        resource.runInAsyncScope(() => socket.emit("message", Buffer.from('{"type":"ping"}')));
        assert.equal(socket.sent.at(-1)?.code, "SERVER_DRAINING");
        liveFixture.frameBarrier = frameDone.promise;
        liveFixture.frameEntered = frameEntered.resolve;
        authDone.resolve();
        await frameEntered.promise;
        assert.equal(liveFixture.welcomes, 0);
        assert.equal(liveFixture.httpCloses, 0);
        assert.equal(liveFixture.wsCloses, 0);
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 1);
        frameDone.resolve();
        await stopping;
        assert.equal(liveFixture.welcomes, 1);
        assert.equal(liveFixture.httpCloses, 1);
        assert.equal(liveFixture.wsCloses, 1);
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
        assert.equal(
          fixture.poolCloses,
          0,
          "transport stop does not close the shared native runtime"
        );
        await runtime.closePersistenceRuntime();
        assert.equal(fixture.poolCloses, 1);
      } finally {
        resource.emitDestroy();
      }
    })
);

test(
  "native frame preparation failure returns a safe protocol error and still drains stop",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const resource = await boot();
      const welcome = barrier(),
        error = barrier();
      liveFixture.sent = (message) => {
        if (message.type === "welcome") welcome.resolve();
        if (message.code === "SERVER_ERROR") error.resolve();
      };
      const socket = new WebSocket();
      try {
        emitConnection(resource, socket);
        resource.runInAsyncScope(() =>
          socket.emit("message", Buffer.from('{"type":"subscribe","channels":["requests"]}'))
        );
        await welcome.promise;
        const frame = barrier();
        frame.resolve();
        liveFixture.frameBarrier = frame.promise;
        liveFixture.frameFailure = new Error("api_key=private-test-secret at /srv/private/store");
        resource.runInAsyncScope(() => socket.emit("message", Buffer.from('{"type":"ping"}')));
        await error.promise;
        assert.equal(socket.closeCode, 1013);
        assert.doesNotMatch(
          JSON.stringify(socket.sent),
          /private-test-secret|\/srv\/private|at \/|stack/
        );
        await live.stopLiveDashboardServer(ownedServer!);
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
      } finally {
        resource.emitDestroy();
      }
    })
);

test(
  "native startup snapshot failure releases transports before returning failure",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const failure = new Error("startup-catalog-unavailable");
      fixture.catalogFailure = failure;
      await assert.rejects(live.startLiveDashboardServer(), (error) => error === failure);
      assert.equal(liveFixture.httpCloses, 1);
      assert.equal(liveFixture.wsCloses, 1);
      assert.equal(liveFixture.unsubscribes, 1);
      assert.equal(fixture.poolCloses, 0);
    })
);

test(
  "native frame capacity includes its completing subscription and rejects extra frames without scopes",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const resource = await boot();
      const welcome = barrier();
      liveFixture.sent = (message) => {
        if (message.type === "welcome") welcome.resolve();
      };
      const socket = new WebSocket();
      try {
        emitConnection(resource, socket);
        resource.runInAsyncScope(() =>
          socket.emit("message", Buffer.from('{"type":"subscribe","channels":["requests"]}'))
        );
        await welcome.promise;
        const base = fixture.generation;
        const entered = barrier(),
          frame = barrier();
        liveFixture.frameEntered = entered.resolve;
        liveFixture.frameBarrier = frame.promise;
        for (let i = 0; i < 33; i++)
          resource.runInAsyncScope(() => socket.emit("message", Buffer.from('{"type":"ping"}')));
        await entered.promise;
        assert.equal(socket.closeCode, 4008);
        assert.equal(socket.sent.at(-1)?.code, "RATE_LIMITED");
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 1);
        const stopping = observe(live.stopLiveDashboardServer(ownedServer!));
        assert.equal(liveFixture.httpCloses, 0);
        frame.resolve();
        await stopping;
        assert.equal(fixture.generation, base + 31);
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
      } finally {
        resource.emitDestroy();
      }
    })
);

test(
  "SQLite accepted message callbacks keep their immediate protocol response",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
      liveFixture.requireLogin = false;
      const socket = new WebSocket(),
        external = new AsyncResource("sqlite-ws-input");
      try {
        ownedServer = await live.startLiveDashboardServer();
        const welcome = barrier();
        liveFixture.sent = (message) => {
          if (message.type === "welcome") welcome.resolve();
        };
        emitConnection(external, socket);
        external.runInAsyncScope(() =>
          socket.emit("message", Buffer.from('{"type":"subscribe","channels":["requests"]}'))
        );
        await welcome.promise;
        external.runInAsyncScope(() => socket.emit("message", Buffer.from('{"type":"ping"}')));
        assert.equal(socket.sent.at(-1)?.type, "pong");
        assert.equal(fixture.generation, 0);
        assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      } finally {
        socket.terminate();
        external.emitDestroy();
        await new Promise<void>((resolve, reject) =>
          liveFixture.wss!.close((error) => (error ? reject(error) : resolve()))
        );
        if (ownedServer) await live.stopLiveDashboardServer(ownedServer);
        ownedServer = undefined;
        process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      }
    })
);

test(
  "a synchronous native listen failure awaits complete transport cleanup",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const failure = new Error("invalid-listen-configuration");
      liveFixture.listenFailure = failure;
      await assert.rejects(live.startLiveDashboardServer(), (error) => error === failure);
      assert.equal(liveFixture.httpCloses, 1);
      assert.equal(liveFixture.wsCloses, 1);
      assert.equal(liveFixture.unsubscribes, 1);
      assert.equal(fixture.poolCloses, 0);
    })
);
