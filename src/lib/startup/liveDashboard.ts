import type { Server } from "node:http";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { warnIfNonLoopbackWithoutApiKey } from "./nonLoopbackApiKeyGuard";

interface LiveDashboardListener {
  readonly server: Server;
  stop(): Promise<void>;
}

interface LiveDashboardOwner {
  readonly identity: object;
  readonly starting: Promise<LiveDashboardListener | undefined>;
  closing: boolean;
  stopping?: Promise<void>;
}
const LIVE_DASHBOARD_OWNER = Symbol.for("omniroute.postgresLiveDashboardOwner");
const LIVE_DASHBOARD_CLOSED = Symbol.for("omniroute.postgresLiveDashboardClosed");
const liveGlobals = globalThis as typeof globalThis & {
  [LIVE_DASHBOARD_OWNER]?: LiveDashboardOwner;
  [LIVE_DASHBOARD_CLOSED]?: WeakSet<object>;
};

const closedIdentities = (liveGlobals[LIVE_DASHBOARD_CLOSED] ??= new WeakSet<object>());

/** Own the real listener's readiness and retain its transport-specific drain callback. */
export async function startPostgresLiveDashboard(): Promise<boolean> {
  if (parseDatabaseBackend() !== "postgres") {
    throw new Error("Managed live dashboard startup requires PostgreSQL persistence");
  }
  const identity = getSelectedPersistenceIdentity();
  if (!identity) throw new Error("Live dashboard requires selected PostgreSQL persistence");
  if (closedIdentities.has(identity)) return false;
  const existing = liveGlobals[LIVE_DASHBOARD_OWNER];
  if (existing) {
    if (existing.identity !== identity || existing.closing)
      throw new Error("Live dashboard persistence authority changed or is draining");
    return Boolean(await existing.starting);
  }

  let owner: LiveDashboardOwner;
  const starting = runPersistenceBackgroundWork(async () => {
    if (getSelectedPersistenceIdentity() !== identity)
      throw new Error("Live dashboard persistence identity changed");
    const liveServer = await import("@/server/ws/liveServer");
    if (owner.closing || !liveServer.isLiveWsEnabled()) return undefined;
    const port = parseInt(process.env.LIVE_WS_PORT || "20132", 10);
    const host = process.env.LIVE_WS_HOST || "127.0.0.1";
    warnIfNonLoopbackWithoutApiKey("Live dashboard WebSocket", host);
    const server = await liveServer.startLiveDashboardServer(port, host);
    return { server, stop: () => liveServer.stopLiveDashboardServer(server) };
  });
  owner = { identity, starting, closing: false };
  liveGlobals[LIVE_DASHBOARD_OWNER] = owner;
  void starting.catch(() => {
    if (liveGlobals[LIVE_DASHBOARD_OWNER] === owner && !owner.closing)
      liveGlobals[LIVE_DASHBOARD_OWNER] = undefined;
  });
  return Boolean(await starting);
}

/** Latch admission before awaiting a pending listen, then drain the exact returned server. */
export function stopPostgresLiveDashboard(): Promise<void> {
  const owner = liveGlobals[LIVE_DASHBOARD_OWNER];
  const identity = owner?.identity ?? getSelectedPersistenceIdentity();
  if (identity) closedIdentities.add(identity);
  if (!owner) return Promise.resolve();
  owner.closing = true;
  owner.stopping ??= (async () => {
    try {
      const listener = await owner.starting;
      if (listener) await listener.stop();
    } finally {
      if (liveGlobals[LIVE_DASHBOARD_OWNER] === owner)
        liveGlobals[LIVE_DASHBOARD_OWNER] = undefined;
    }
  })();
  return owner.stopping;
}
