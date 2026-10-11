import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { getConsistentMachineId } from "@/shared/utils/machineId";
import { isCloudEnabled } from "@/lib/db/settings";
import { getRuntimePorts } from "@/lib/runtime/ports";

const { dashboardPort } = getRuntimePorts();

const INTERNAL_BASE_URL =
  process.env.BASE_URL ||
  process.env.NEXT_PUBLIC_BASE_URL ||
  process.env.NEXT_PUBLIC_APP_URL ||
  `http://localhost:${dashboardPort}`;

/**
 * Cloud sync scheduler
 */
export class CloudSyncScheduler {
  machineId: string | null;
  intervalMinutes: number;
  intervalId: ReturnType<typeof setInterval> | null;
  private startupTimer: ReturnType<typeof setTimeout> | null = null;
  private closing = false;
  private generation = 0;
  private pending = new Set<Promise<unknown>>();
  private closePromise: Promise<void> | undefined;

  constructor(machineId: string | null = null, intervalMinutes = 15) {
    this.machineId = machineId;
    this.intervalMinutes = intervalMinutes;
    this.intervalId = null;
  }

  /**
   * Initialize machine ID if not provided
   */
  async initializeMachineId() {
    if (!this.machineId) {
      this.machineId = await getConsistentMachineId();
    }
  }

  /**
   * Start periodic sync (delays first sync to allow server to be ready)
   */
  async start() {
    if (this.closing) throw new Error("Cloud sync scheduler is closing");
    if (this.intervalId) {
      return;
    }

    const generation = this.generation;
    await this.initializeMachineId();
    if (this.closing) throw new Error("Cloud sync scheduler is closing");
    if (this.intervalId || generation !== this.generation) return;

    // Delay first sync by 30 seconds to ensure server is ready
    this.startupTimer = setTimeout(() => {
      if (this.closing || generation !== this.generation) return;
      this.startupTimer = null;
      this.enqueueSync(generation);
    }, 30000);
    this.startupTimer.unref?.();

    // Then sync periodically
    this.intervalId = setInterval(
      () => {
        this.enqueueSync(generation);
      },
      this.intervalMinutes * 60 * 1000
    );
    this.intervalId.unref?.();
  }

  /**
   * Stop periodic sync
   */
  stop() {
    this.generation++;
    if (this.startupTimer) {
      clearTimeout(this.startupTimer);
      this.startupTimer = null;
    }
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  private enqueueSync(generation: number): void {
    if (this.closing || generation !== this.generation || this.pending.size) return;
    const work = runPersistenceBackgroundWork(() => this.syncWithRetry());
    this.pending.add(work);
    void work.then(
      () => this.pending.delete(work),
      () => this.pending.delete(work)
    );
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closing = true;
    this.stop();
    this.closePromise = (async () => {
      while (this.pending.size) await Promise.allSettled([...this.pending]);
    })();
    return this.closePromise;
  }

  /**
   * Sync with retry logic (exponential backoff)
   */
  async syncWithRetry(maxRetries = 1) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const result = await this.sync();
        return result;
      } catch (_error) {
        if (attempt === maxRetries) {
          return null;
        }

        const delay = Math.min(1000 * Math.pow(2, attempt), 10000); // Max 10s
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  /**
   * Perform sync via internal API route (handles token update to db.json)
   */
  async sync() {
    // Check if cloud is enabled
    const enabled = await isCloudEnabled();
    if (!enabled) {
      return null;
    }

    await this.initializeMachineId();

    // Call internal API route which handles both sync and token update
    const response = await fetch(`${INTERNAL_BASE_URL}/api/sync/cloud`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ machineId: this.machineId, action: "sync" }),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error || "Sync failed");
    }

    const result = await response.json();
    return result;
  }

  /**
   * Check if scheduler is running
   */
  isRunning() {
    return this.intervalId !== null;
  }
}

declare global {
  var __omnirouteCloudSyncScheduler: CloudSyncScheduler | undefined;
}

export async function getCloudSyncScheduler(machineId: string | null = null, intervalMinutes = 15) {
  return (globalThis.__omnirouteCloudSyncScheduler ??= new CloudSyncScheduler(
    machineId,
    intervalMinutes
  ));
}

/** Shutdown does not instantiate a scheduler that was never started. */
export function closeCloudSyncScheduler(): Promise<void> {
  return globalThis.__omnirouteCloudSyncScheduler?.close() ?? Promise.resolve();
}
