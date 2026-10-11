import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { createPersistencePoller, type PersistencePoller } from "@/lib/config/persistencePoller";
import { setModelContextOverrideAsync } from "./db/modelContextOverrides";
import { removeModelContextOverrideAsync } from "./db/modelContextOverrides";
import { getSelectedPostgresDatabase } from "./db/controlPlaneContext";
import { isPostgresCatalogMutation, mutatePostgresCatalog } from "./db/postgres/catalogSnapshot";
import { getAllSyncedAvailableModels } from "./db/models";
import { getResolvedModelCapabilities } from "./modelCapabilities";
import { getModelContextOverrideRecord } from "./db/modelContextOverrides";

/**
 * Feature 5004 — self-correcting context-window reconciler.
 *
 * Compares each model's provider-declared window (captured by `/models` discovery in
 * `syncedAvailableModels`) against the override-free catalog and, when they diverge,
 * pins the discovered value as an `auto:discovery` override so the real window wins in
 * `getModelContextLimit`. It never touches `manual` overrides, and it self-heals by
 * removing a now-redundant auto override when the catalog has caught up.
 *
 * No new network fetch: it reconciles data the managed-model import already persisted,
 * so it does not duplicate `modelsDevSync` / `modelDiscovery`.
 */

export interface DiscoveredWindow {
  provider: string;
  modelId: string;
  window: number | null;
}

export interface ReconcileDeps {
  getCatalogWindow: (provider: string, modelId: string) => number | null;
  getExistingSource: (provider: string, modelId: string) => string | null;
  writeAuto: (provider: string, modelId: string, window: number) => void;
  removeOverride: (provider: string, modelId: string) => void;
}

export interface ReconcileResult {
  scanned: number;
  written: number;
  removed: number;
  skippedManual: number;
}

/**
 * Pure reconcile: given the discovered windows and a set of injectable deps, decide
 * which auto overrides to write/remove. Deterministic and side-effect-free except
 * through the injected `writeAuto`/`removeOverride`.
 */
export function reconcileContextWindows(
  discovered: DiscoveredWindow[],
  deps: ReconcileDeps
): ReconcileResult {
  const result: ReconcileResult = {
    scanned: 0,
    written: 0,
    removed: 0,
    skippedManual: 0,
  };
  for (const { provider, modelId, window } of discovered) {
    result.scanned++;
    if (!provider || !modelId) continue;
    if (typeof window !== "number" || !Number.isInteger(window) || window <= 0) {
      // Discovery omitted a real window. Drop a stale auto:discovery pin
      // (e.g. grok-cli's old 256k fallback) so registry/models.dev can win.
      if (deps.getExistingSource(provider, modelId) === "auto:discovery") {
        deps.removeOverride(provider, modelId);
        result.removed++;
      }
      continue;
    }

    const existingSource = deps.getExistingSource(provider, modelId);
    if (existingSource === "manual") {
      result.skippedManual++;
      continue;
    }

    const catalog = deps.getCatalogWindow(provider, modelId);
    if (window !== catalog) {
      deps.writeAuto(provider, modelId, window);
      result.written++;
    } else if (existingSource) {
      // Discovered window now matches the catalog and a stale auto override exists → drop it.
      deps.removeOverride(provider, modelId);
      result.removed++;
    }
  }
  return result;
}

/** Flatten the per-provider discovery map into the reconcile input. */
function toDiscoveredWindows(
  byProvider: Record<
    string,
    Array<{ id: string; contextWindow?: number; inputTokenLimit?: number }>
  >
): DiscoveredWindow[] {
  const out: DiscoveredWindow[] = [];
  for (const [provider, models] of Object.entries(byProvider)) {
    for (const m of models) {
      out.push({
        provider,
        modelId: m.id,
        window: m.contextWindow ?? m.inputTokenLimit ?? null,
      });
    }
  }
  return out;
}

/** Run the reconcile against the live DB (discovery → overrides). */
export async function runContextWindowReconcile(): Promise<ReconcileResult> {
  const database = getSelectedPostgresDatabase();
  if (database && !isPostgresCatalogMutation(database))
    return mutatePostgresCatalog(database, runContextWindowReconcile);
  const byProvider = await getAllSyncedAvailableModels();
  const discovered = toDiscoveredWindows(byProvider);
  const total: ReconcileResult = {
    scanned: 0,
    written: 0,
    removed: 0,
    skippedManual: 0,
  };
  for (const entry of discovered) {
    const actions: Array<() => Promise<unknown>> = [];
    const current = reconcileContextWindows([entry], {
      getCatalogWindow: (provider, modelId) =>
        getResolvedModelCapabilities({ provider, model: modelId }, { persistedOverrides: false })
          .contextWindow,
      getExistingSource: (provider, modelId) =>
        getModelContextOverrideRecord(provider, modelId)?.source ?? null,
      writeAuto: (provider, modelId, window) => {
        actions.push(
          async () =>
            await setModelContextOverrideAsync(provider, modelId, window, "auto:discovery")
        );
      },
      removeOverride: (provider, modelId) => {
        actions.push(async () => await removeModelContextOverrideAsync(provider, modelId));
      },
    });
    for (const action of actions) await action();
    total.scanned += current.scanned;
    total.written += current.written;
    total.removed += current.removed;
    total.skippedManual += current.skippedManual;
  }
  return total;
}

// --- Periodic job (mirrors modelsDevSync.startPeriodicSync) ---

const DEFAULT_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24h
let reconcileTimer: ReturnType<typeof setInterval> | null = null;

function resolveIntervalMs(): number {
  const raw = process.env.CONTEXT_WINDOW_RECONCILE_INTERVAL;
  if (raw === undefined) return DEFAULT_INTERVAL_MS;
  const seconds = Number(raw);
  if (!Number.isFinite(seconds) || seconds <= 0) return 0; // 0/invalid → disabled
  return Math.floor(seconds * 1000);
}

/**
 * Start the periodic reconcile. Idempotent. Disabled when
 * `CONTEXT_WINDOW_RECONCILE_INTERVAL=0`. Best-effort: failures are swallowed (the
 * static catalog remains the source of truth).
 */
interface NativeSchedulerOwner {
  readonly identity: object;
  readonly poller: PersistencePoller;
  stopping?: Promise<void>;
}
const NATIVE_SCHEDULER = Symbol.for("omniroute.contextWindowReconcile.postgresScheduler");
const nativeGlobals = globalThis as typeof globalThis & {
  [NATIVE_SCHEDULER]?: NativeSchedulerOwner;
};

function stopNativePoller(): Promise<void> {
  const owner = nativeGlobals[NATIVE_SCHEDULER];
  if (!owner) return Promise.resolve();

  owner.stopping ??= owner.poller.stop().finally(() => {
    if (nativeGlobals[NATIVE_SCHEDULER] === owner) nativeGlobals[NATIVE_SCHEDULER] = undefined;
  });
  return owner.stopping;
}

export function startContextWindowReconcile(intervalMs?: number): void {
  if (parseDatabaseBackend() === "postgres") {
    const identity = getSelectedPersistenceIdentity();
    if (!identity) throw new Error("Native scheduler requires selected PostgreSQL persistence");
    const existing = nativeGlobals[NATIVE_SCHEDULER];
    if (existing) {
      if (existing.identity !== identity || existing.stopping)
        throw new Error("Native scheduler authority changed or is draining");
      return;
    }
    const interval = intervalMs ?? resolveIntervalMs();
    if (!interval || interval <= 0) return;
    const poller = createPersistencePoller({
      intervalMs: interval,
      initialDelayMs: 0,
      check: async () => {
        await runPersistenceBackgroundWork(async () => {
          if (getSelectedPersistenceIdentity() !== identity)
            throw new Error("Native scheduler persistence identity changed");
          await runContextWindowReconcile();
        });
      },
      onFailure: () => {},
    });
    nativeGlobals[NATIVE_SCHEDULER] = { identity, poller };
    poller.start();
    return;
  }
  if (reconcileTimer) return;
  const interval = intervalMs ?? resolveIntervalMs();
  if (!interval || interval <= 0) return;

  const tick = async () => {
    void (await runContextWindowReconcile().catch(() => {
      // Swallow — reconcile is advisory; the catalog still resolves windows.
    }));
  };

  // Initial non-blocking pass, then on the interval.
  setTimeout(tick, 0);
  reconcileTimer = setInterval(tick, interval);
  reconcileTimer.unref?.();
}

export function stopContextWindowReconcile(): void {
  if (parseDatabaseBackend() === "postgres") {
    void stopNativePoller().catch(() => {});
    return;
  }
  if (reconcileTimer) {
    clearInterval(reconcileTimer);
    reconcileTimer = null;
  }
}

/** Stop admission and wait for every accepted native tick. */
export async function stopContextWindowReconcileAsync(): Promise<void> {
  if (parseDatabaseBackend() !== "postgres") return stopContextWindowReconcile();
  await stopNativePoller();
}
