import { EventEmitter } from "node:events";
import {
  getSelectedControlPlane,
  getSelectedPersistenceIdentity,
} from "../../../src/lib/db/controlPlaneContext.ts";
import {
  fixture,
  withPostgresCatalogSnapshot as prepareCatalog,
} from "./runtimeOwnedWorkFixture.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
export {
  readPostgresEnvironment,
  readS3ArtifactConfiguration,
  initializePostgresControlPlane,
  createS3ArtifactStore,
  initializeCircuitBreakerPersistence,
  initializeQuotaSnapshotCache,
  flushCircuitBreakerPersistence,
} from "./runtimeOwnedWorkFixture.ts";
export { sanitizeErrorMessage } from "../../../open-sse/utils/errorSanitization.ts";

export const liveFixture = {
  wss: undefined as WebSocketServer | undefined,
  authBarrier: undefined as Promise<void> | undefined,
  authEntered: undefined as (() => void) | undefined,
  frameBarrier: undefined as Promise<void> | undefined,
  frameEntered: undefined as (() => void) | undefined,
  frameFailure: undefined as Error | undefined,
  requireLogin: true,
  listenFailure: undefined as Error | undefined,
  authGenerations: [] as Array<number | undefined>,
  latestReads: 0,
  welcomes: 0,
  unsubscribes: 0,
  httpCloses: 0,
  wsCloses: 0,
  sent: undefined as ((message: Record<string, unknown>) => void) | undefined,
  reset() {
    liveFixture.wss = undefined;
    liveFixture.authBarrier = undefined;
    liveFixture.authEntered = undefined;
    liveFixture.frameBarrier = undefined;
    liveFixture.frameEntered = undefined;
    liveFixture.frameFailure = undefined;
    liveFixture.requireLogin = true;
    liveFixture.listenFailure = undefined;
    liveFixture.authGenerations = [];
    liveFixture.latestReads = 0;
    liveFixture.welcomes = 0;
    liveFixture.unsubscribes = 0;
    liveFixture.httpCloses = 0;
    liveFixture.wsCloses = 0;
    liveFixture.sent = undefined;
  },
};
export class WebSocket extends EventEmitter {
  static OPEN = 1;
  readyState = WebSocket.OPEN;
  sent: Array<Record<string, unknown>> = [];
  closeCode: number | undefined;
  send(bytes: string) {
    const message = JSON.parse(bytes) as Record<string, unknown>;
    this.sent.push(message);
    liveFixture.sent?.(message);
    if (message.type === "welcome") liveFixture.welcomes++;
  }
  close(code: number) {
    this.closeCode = code;
    this.terminate();
  }
  terminate() {
    if (this.readyState !== 3) {
      this.readyState = 3;
      this.emit("close");
    }
  }
  ping() {}
}
export class WebSocketServer extends EventEmitter {
  clients = new Set<WebSocket>();
  private stopped = false;
  private done: ((error?: Error) => void) | undefined;
  constructor(_options: unknown) {
    super();
    liveFixture.wss = this;
    this.on("connection", (socket: WebSocket) => {
      this.clients.add(socket);
      socket.once("close", () => {
        this.clients.delete(socket);
        this.complete();
      });
    });
  }
  private complete() {
    if (this.stopped && !this.clients.size && this.done) {
      const done = this.done;
      this.done = undefined;
      this.emit("close");
      done();
    }
  }
  close(done: (error?: Error) => void) {
    liveFixture.wsCloses++;
    this.stopped = true;
    this.done = done;
    this.complete();
  }
}
class HttpServer extends EventEmitter {
  listen(_port: number, _host: string, done: () => void) {
    if (liveFixture.listenFailure) throw liveFixture.listenFailure;
    done();
    return this;
  }
  close(done: (error?: Error) => void) {
    liveFixture.httpCloses++;
    this.emit("close");
    done();
    return this;
  }
  closeAllConnections() {}
}
export function createServer(_listener: unknown) {
  return new HttpServer();
}
export function onAny(_listener: unknown) {
  return () => {
    liveFixture.unsubscribes++;
  };
}
export function emit() {}
export function on() {}
export function getEventHistory() {
  return [];
}
export function installProcessCrashGuard() {}
export function attachRequestStreamGuards() {}
export function warnIfNonLoopbackWithoutApiKey() {}
export function hasProxyHopHeader() {
  return false;
}
export function isAutomatedTestProcess() {
  return true;
}
export function isBuildProcess() {
  return false;
}
export const DASHBOARD_SESSION_COOKIE = "auth_token";
export async function verifyDashboardSessionToken() {
  return null;
}
export async function getSettings() {
  getSelectedControlPlane()?.assertReady();
  liveFixture.authGenerations.push(fixture.selectedGeneration());
  return { requireLogin: liveFixture.requireLogin, password: "configured-test-password" };
}
export function extractApiKey() {
  return "test-api-key";
}
export async function isValidApiKey() {
  getSelectedControlPlane()?.assertReady();
  liveFixture.authEntered?.();
  if (liveFixture.authBarrier) await liveFixture.authBarrier;
  getSelectedControlPlane()?.assertReady();
  return true;
}
export async function getLatestCompressionAnalyticsRunAsync() {
  getSelectedControlPlane()?.assertReady();
  liveFixture.latestReads++;
  return undefined;
}
/** Catalog preparation is the only deferred frame dependency in this composition oracle. */
export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  const frame = liveFixture.frameBarrier;
  if (!frame) return prepareCatalog(database, body);
  liveFixture.frameBarrier = undefined;
  return prepareCatalog(database, async () => {
    assertFrameAuthority();
    liveFixture.frameEntered?.();
    await frame;
    if (liveFixture.frameFailure) throw liveFixture.frameFailure;
    return body();
  });
}
function assertFrameAuthority() {
  getSelectedControlPlane()?.assertReady();
  if (getSelectedPersistenceIdentity() !== fixture.authority)
    throw new Error("Wrong fixture authority");
}
