import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { IncomingMessage, Server } from "node:http";
import { registerHooks } from "node:module";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { liveFixture, WebSocket } from "../helpers/persistence/liveWsScopeFixture.ts";

test(
  "native LiveWS seeds real SQL analytics and authorizes a detached anonymous dashboard frame",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  async () => {
    const liveUrl = new URL("../../src/server/ws/liveServer.ts", import.meta.url).href;
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const nativeFixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const transportFixtureUrl = new URL(
      "../helpers/persistence/liveWsScopeFixture.ts",
      import.meta.url
    ).href;
    const transportDependencies = new Set([
      "ws",
      "http",
      "@/shared/utils/dashboardSessionToken",
      "@/lib/events/eventBus",
      "@/shared/utils/testProcess",
      "@/server/authz/proxyHeaders",
      "@/lib/startup/nonLoopbackApiKeyGuard",
      "@/shared/utils/httpClientAbortGuard.mjs",
      "../../sse/services/auth.ts",
      "@omniroute/open-sse/utils/error",
    ]);
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: nativeFixtureUrl, shortCircuit: true };
        if (context.parentURL === liveUrl && transportDependencies.has(specifier))
          return { url: transportFixtureUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const saved = new Map(
      [
        "OMNIROUTE_DATABASE_BACKEND",
        "OMNIROUTE_ENABLE_LIVE_WS",
        "STORAGE_ENCRYPTION_KEY",
        "JWT_SECRET",
        "API_KEY_SECRET",
      ].map((name) => [name, process.env[name]])
    );
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.OMNIROUTE_ENABLE_LIVE_WS = "0";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let live: typeof import("../../src/server/ws/liveServer.ts") | undefined;
    let server: Server | undefined;
    const external = new AsyncResource("native-live-ws-detached-frame");
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      liveFixture.reset();
      const preparation = await createTestPostgresDatabase();
      try {
        await preparation.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      } finally {
        await preparation.close();
      }
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const requestId = randomUUID();
      await runtime.runPersistenceRequest(async () => {
        await (
          await import("../../src/lib/db/settings.ts")
        ).updateSettings({ requireLogin: false });
        await (
          await import("../../src/lib/db/compressionAnalytics.ts")
        ).insertCompressionAnalyticsRowAsync({
          timestamp: new Date().toISOString(),
          mode: "native-live-ws-fixture",
          original_tokens: 100,
          compressed_tokens: 50,
          tokens_saved: 50,
          request_id: requestId,
        });
      });
      live = await import(liveUrl);
      server = await runtime.runPersistenceRequest(() => live!.startLiveDashboardServer());
      let received!: (message: Record<string, unknown>) => void;
      const welcome = new Promise<Record<string, unknown>>((resolve) => {
        received = resolve;
      });
      liveFixture.sent = (message) => {
        if (message.type === "welcome") received(message);
      };
      const socket = new WebSocket();
      const request = {
        headers: { origin: "http://localhost:20128" },
        socket: { remoteAddress: "127.0.0.1" },
      } as unknown as IncomingMessage;
      external.runInAsyncScope(() => liveFixture.wss!.emit("connection", socket, request));
      external.runInAsyncScope(() =>
        socket.emit("message", Buffer.from('{"type":"subscribe","channels":["compression"]}'))
      );
      const message = await welcome;
      const events = message.data as Array<{ event: string; data: { requestId: string } }>;
      assert.ok(
        events.some(
          (entry) => entry.event === "compression.completed" && entry.data.requestId === requestId
        )
      );
      await live.stopLiveDashboardServer(server);
      assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
      await runtime.closePersistenceRuntime();
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
    } finally {
      if (server && live) await live.stopLiveDashboardServer(server).catch(() => {});
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      external.emitDestroy();
      delete globalThis.__omniroutePersistenceRuntime;
      for (const [name, value] of saved) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
