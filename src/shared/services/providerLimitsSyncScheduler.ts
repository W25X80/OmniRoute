import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { createPersistencePoller, type PersistencePoller } from "@/lib/config/persistencePoller";
import {
  getLastProviderLimitsAutoSyncTime,
  getProviderLimitsSyncIntervalMinutes,
  getProviderLimitsSyncIntervalMs,
  syncAllProviderLimits,
} from "@/lib/usage/providerLimits";

const STARTUP_DELAY_MS = 5_000;

let schedulerTimer: NodeJS.Timeout | null = null;
let startupTimer: NodeJS.Timeout | null = null;
let isRunning = false;
let postgresPoller: PersistencePoller | null = null;

async function runProviderLimitsSyncCycle(): Promise<void> {
  if (isRunning) {
    console.log("[ProviderLimitsSync] Skipping cycle — previous run still in progress");
    return;
  }

  isRunning = true;
  const start = Date.now();

  try {
    const result = await syncAllProviderLimits({ source: "scheduled" });
    console.log(
      `[ProviderLimitsSync] Cycle complete: ${result.succeeded}/${result.total} synced in ${Date.now() - start}ms`
    );
  } catch (error) {
    console.warn("[ProviderLimitsSync] Cycle failed:", (error as Error).message);
  } finally {
    isRunning = false;
  }
}

function startPostgresProviderLimitsSyncScheduler(): void {
  if (postgresPoller) {
    console.log("[ProviderLimitsSync] Scheduler already running — skipping start");
    return;
  }
  const intervalMs = getProviderLimitsSyncIntervalMs();
  postgresPoller = createPersistencePoller({
    intervalMs,
    initialDelayMs: () =>
      runPersistenceBackgroundWork(async () => {
        const lastAutoSyncAt = await getLastProviderLimitsAutoSyncTime();
        const lastRunMs = lastAutoSyncAt ? Date.parse(lastAutoSyncAt) : NaN;
        const elapsedMs = Date.now() - lastRunMs;
        return Number.isFinite(lastRunMs) && elapsedMs < intervalMs
          ? Math.max(intervalMs - elapsedMs, STARTUP_DELAY_MS)
          : STARTUP_DELAY_MS;
      }),
    check: () =>
      runPersistenceBackgroundWork(async () => {
        const start = Date.now();
        const result = await syncAllProviderLimits({ source: "scheduled" });
        console.log(
          `[ProviderLimitsSync] Cycle complete: ${result.succeeded}/${result.total} synced in ${Date.now() - start}ms`
        );
      }),
    onBusy: () =>
      console.log("[ProviderLimitsSync] Skipping cycle — previous run still in progress"),
    onFailure: () => console.warn("[ProviderLimitsSync] PostgreSQL cycle failed"),
  });
  postgresPoller.start();
  console.log(
    `[ProviderLimitsSync] Scheduler started — interval: ${getProviderLimitsSyncIntervalMinutes()}m`
  );
}

export function startProviderLimitsSyncScheduler(): void {
  if (parseDatabaseBackend() === "postgres") {
    startPostgresProviderLimitsSyncScheduler();
    return;
  }
  if (schedulerTimer || startupTimer) {
    console.log("[ProviderLimitsSync] Scheduler already running — skipping start");
    return;
  }

  const intervalMs = getProviderLimitsSyncIntervalMs();
  const intervalMinutes = getProviderLimitsSyncIntervalMinutes();

  console.log(`[ProviderLimitsSync] Scheduler started — interval: ${intervalMinutes}m`);

  void (async () => {
    let initialDelayMs = STARTUP_DELAY_MS;
    const lastAutoSyncAt = await getLastProviderLimitsAutoSyncTime();

    if (lastAutoSyncAt) {
      const lastRunMs = Date.parse(lastAutoSyncAt);
      if (Number.isFinite(lastRunMs)) {
        const elapsedMs = Date.now() - lastRunMs;
        if (elapsedMs < intervalMs) {
          initialDelayMs = Math.max(intervalMs - elapsedMs, STARTUP_DELAY_MS);
        }
      }
    }

    startupTimer = setTimeout(() => {
      startupTimer = null;
      void runProviderLimitsSyncCycle();

      schedulerTimer = setInterval(() => {
        void runProviderLimitsSyncCycle();
      }, intervalMs);
      schedulerTimer.unref?.();
    }, initialDelayMs);

    startupTimer.unref?.();
  })();
}

/** Native scheduler drain. SQLite has no production stop contract in the source. */
export async function stopProviderLimitsSyncScheduler(): Promise<void> {
  if (!postgresPoller) return;
  const poller = postgresPoller;
  await poller.stop();
  if (postgresPoller === poller) postgresPoller = null;
}
