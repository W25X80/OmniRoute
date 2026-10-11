import { createPersistencePoller, type PersistencePoller } from "../config/persistencePoller";
import { getSelectedPersistenceIdentity } from "./controlPlaneContext";
import { runPersistenceBackgroundWork } from "./persistenceRuntime";

type CleanupPhase = "startup" | "periodic";
interface CleanupSchedulerOwner {
  readonly identity: object;
  readonly poller: PersistencePoller;
  stopping?: Promise<void>;
}

declare global {
  var __omniroutePostgresCleanupScheduler: CleanupSchedulerOwner | undefined;
}

/** One native scheduler across server bundles; every pass enters fresh persistence authority. */
export function startPostgresCleanupScheduler(
  runPass: (phase: CleanupPhase) => Promise<void>,
  intervalMs: number
): void {
  const identity = getSelectedPersistenceIdentity();
  if (!identity) throw new Error("PostgreSQL cleanup requires selected persistence");
  const existing = globalThis.__omniroutePostgresCleanupScheduler;
  if (existing) {
    if (existing.identity !== identity) {
      throw new Error("PostgreSQL cleanup persistence identity changed");
    }
    if (existing.stopping) throw new Error("PostgreSQL cleanup is draining");
    return;
  }
  const poller = createPersistencePoller({
    intervalMs,
    initialDelayMs: 30_000,
    check: (source) =>
      runPersistenceBackgroundWork(async () => {
        if (getSelectedPersistenceIdentity() !== identity) {
          throw new Error("PostgreSQL cleanup persistence identity changed");
        }
        await runPass(source === "start" ? "startup" : "periodic");
      }),
    onFailure: () => console.error("[Cleanup] PostgreSQL scheduled cleanup failed"),
  });
  globalThis.__omniroutePostgresCleanupScheduler = { identity, poller };
  poller.start();
  console.log("[Cleanup] Background cleanup scheduler started (every 6 hours).");
}

/** Cancel pending startup and interval dispatch synchronously, retaining any admitted pass. */
export function stopPostgresCleanupScheduler(): void {
  const owner = globalThis.__omniroutePostgresCleanupScheduler;
  if (!owner || owner.stopping) return;
  owner.stopping = owner.poller.stop().finally(() => {
    if (globalThis.__omniroutePostgresCleanupScheduler === owner) {
      globalThis.__omniroutePostgresCleanupScheduler = undefined;
    }
  });
  // The synchronous compatibility caller cannot await; the async stopper retains rejection.
  void owner.stopping.catch(() => console.error("[Cleanup] PostgreSQL cleanup drain failed"));
}

/** Shutdown must await SQL work and all artifact deletion/acknowledgement I/O in the pass. */
export async function stopPostgresCleanupSchedulerAsync(): Promise<void> {
  const owner = globalThis.__omniroutePostgresCleanupScheduler;
  stopPostgresCleanupScheduler();
  await owner?.stopping;
}
