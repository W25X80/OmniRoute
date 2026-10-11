import { saveServiceModelsAsync } from "@/lib/db/serviceModels";
/**
 * Model sync job for embedded services.
 *
 * Fetches /v1/models from a running service instance and persists the list
 * in the key_value table so the model catalog can expose them without calling
 * the service on every request. Also stamps last_sync_at in version_manager.
 *
 * Schedule: runs once when the service reaches "running" state, then every
 * SYNC_INTERVAL_MS. Stops automatically when the service stops.
 */

import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { getServiceModels, type ServiceModel } from "@/lib/db/serviceModels";
import { updateVersionManagerTool } from "@/lib/db/versionManager";
import {
  captureNativeEmbeddedServices,
  getNativeEmbeddedServices,
  assertNativeServiceActivation,
  runNativeServiceWork,
} from "./nativeLifecycle";

const SYNC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const FETCH_TIMEOUT_MS = 10_000;

const activeTimers = new Map<string, ReturnType<typeof setInterval>>();

/**
 * Fetch the model list from a running service and persist it.
 * Returns the number of models synced, or -1 on failure.
 */
export async function syncServiceModels(
  tool: string,
  baseUrl: string,
  apiKey: string
): Promise<number> {
  try {
    const res = await fetch(`${baseUrl}/v1/models`, {
      headers: { Authorization: `Bearer ${apiKey}` },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });

    if (!res.ok) {
      console.warn(`[ModelSync:${tool}] /v1/models returned HTTP ${res.status}`);
      return -1;
    }

    const json = await res.json();
    const data: unknown = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
    const models = (data as unknown[])
      .filter(
        (m): m is ServiceModel =>
          typeof m === "object" &&
          m !== null &&
          typeof (m as Record<string, unknown>).id === "string"
      )
      .map((m) => ({
        ...m,
        // Prefix the model id with the tool name so downstream routing and the
        // executor strip it back off (e.g. "9router/cx/gpt-5-mini").
        id: m.id.startsWith(`${tool}/`) ? m.id : `${tool}/${m.id}`,
      }));

    await saveServiceModelsAsync(tool, models);
    await updateVersionManagerTool(tool, { lastSyncAt: new Date().toISOString() });

    console.log(`[ModelSync:${tool}] synced ${models.length} model(s)`);
    return models.length;
  } catch (err) {
    const msg = sanitizeErrorMessage(err instanceof Error ? err.message : String(err));
    console.warn(`[ModelSync:${tool}] fetch failed: ${msg}`);
    return -1;
  }
}

/**
 * Start periodic model sync for a service.
 * Idempotent — calling again while already running is a no-op.
 */
export function scheduleServiceModelSync(
  tool: string,
  baseUrl: string,
  apiKey: string,
  intervalMs = SYNC_INTERVAL_MS
): void {
  if (parseDatabaseBackend() === "postgres") {
    const owner = captureNativeEmbeddedServices()!;
    assertNativeServiceActivation(owner);
    if (owner.modelTimers.has(tool)) return;
    const dispatch = () => {
      if (owner.closing) return;
      const work = runNativeServiceWork(owner, async () => {
        await syncServiceModels(tool, baseUrl, apiKey);
      });
      const runs = owner.modelRuns.get(tool) ?? new Set<Promise<void>>();
      owner.modelRuns.set(tool, runs);
      runs.add(work);
      void work.then(
        () => runs.delete(work),
        () => runs.delete(work)
      );
      void work.catch((error: unknown) =>
        console.warn(
          `[ModelSync:${tool}] background work failed: ${sanitizeErrorMessage(error instanceof Error ? error.message : String(error))}`
        )
      );
    };
    dispatch();
    const timer = setInterval(dispatch, intervalMs);
    timer.unref?.();
    owner.modelTimers.set(tool, timer);
    console.log(`[ModelSync:${tool}] scheduler started (interval ${intervalMs / 1000}s)`);
    return;
  }
  if (activeTimers.has(tool)) return;

  // First sync immediately (non-blocking)
  syncServiceModels(tool, baseUrl, apiKey).catch(() => {});

  const timer = setInterval(() => {
    syncServiceModels(tool, baseUrl, apiKey).catch(() => {});
  }, intervalMs);
  timer.unref?.();

  activeTimers.set(tool, timer);
  console.log(`[ModelSync:${tool}] scheduler started (interval ${intervalMs / 1000}s)`);
}

/**
 * Stop the periodic sync for a service.
 */
export function stopServiceModelSync(tool: string): void {
  if (parseDatabaseBackend() === "postgres") {
    const owner = getNativeEmbeddedServices();
    const timer = owner?.modelTimers.get(tool);
    if (!timer) return;
    clearInterval(timer);
    owner!.modelTimers.delete(tool);
    console.log(`[ModelSync:${tool}] scheduler stopped`);
    return;
  }
  const timer = activeTimers.get(tool);
  if (!timer) return;
  clearInterval(timer);
  activeTimers.delete(tool);
  console.log(`[ModelSync:${tool}] scheduler stopped`);
}

/** Re-export read path so consumers don't need to import two modules. */
export { getServiceModels };

/** Wait for model writes already accepted by this service's scheduler. */
export async function stopServiceModelSyncAsync(tool: string): Promise<void> {
  stopServiceModelSync(tool);
  if (parseDatabaseBackend() !== "postgres") return;
  const runs = getNativeEmbeddedServices()?.modelRuns.get(tool);
  while (runs?.size) await Promise.allSettled(Array.from(runs));
}
