import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { ensurePersistenceReady, runPersistenceRequest } from "@/lib/db/persistenceRuntime";
import { getServerLifecyclePhase } from "@/lib/serverLifecycle";

interface PostgresStartupOwner {
  cancelled: boolean;
  identity?: object;
  bootstrap?: Promise<void>;
  shutdown?: Promise<void>;
}

const POSTGRES_STARTUP_OWNER = Symbol.for("omniroute.postgresStartupOwner");
const startupGlobals = globalThis as typeof globalThis & {
  [POSTGRES_STARTUP_OWNER]?: PostgresStartupOwner;
};

function getOwner(): PostgresStartupOwner {
  return (startupGlobals[POSTGRES_STARTUP_OWNER] ??= { cancelled: false });
}

/** Reject later activations once shutdown is admitted, including during request drain. */
export function assertPostgresBootstrapActive(): void {
  const owner = getOwner();
  if (getServerLifecyclePhase() === "stopping") owner.cancelled = true;
  if (owner.cancelled) throw new Error("PostgreSQL application startup is stopping");
  if (owner.identity && getSelectedPersistenceIdentity() !== owner.identity)
    throw new Error("PostgreSQL application startup persistence identity changed");
}

/** Publish only inner startup work; its caller may join shutdown after it rejects. */
export function runPostgresBootstrap(body: () => Promise<void>): Promise<void> {
  const owner = getOwner();
  try {
    assertPostgresBootstrapActive();
  } catch (error) {
    return Promise.reject(error);
  }
  if (owner.bootstrap) return owner.bootstrap;
  // Publish before invoking startup, so a reentrant shutdown also sees the barrier.
  const bootstrap = Promise.resolve().then(async () => {
    assertPostgresBootstrapActive();
    await ensurePersistenceReady();
    assertPostgresBootstrapActive();
    const identity = getSelectedPersistenceIdentity();
    if (!identity) throw new Error("PostgreSQL application startup requires persistence");
    owner.identity = identity;
    await runPersistenceRequest(async () => {
      assertPostgresBootstrapActive();
      await body();
    });
  });
  owner.bootstrap = bootstrap;
  return bootstrap;
}

/** Close admission synchronously, join startup, then execute cleanup once across bundles. */
export function runPostgresShutdown(cleanup: () => Promise<void>): Promise<void> {
  const owner = getOwner();
  owner.cancelled = true;
  owner.shutdown ??= (async () => {
    // A boot error remains fatal to its caller. Cleanup only absorbs it at this barrier.
    await owner.bootstrap?.catch(() => {});
    await cleanup();
  })();
  return owner.shutdown;
}
