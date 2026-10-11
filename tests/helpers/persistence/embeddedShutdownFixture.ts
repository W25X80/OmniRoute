import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";
import { fixture } from "./runtimeOwnedWorkFixture.ts";

export function assertShutdownScope(): void {
  if (
    !getSelectedPersistenceIdentity() ||
    globalThis.__omniroutePersistenceRuntime?.owners.getStore()?.active !== true
  ) {
    throw new Error("Shutdown transport seam lacks its actual owned scope");
  }
}

async function stop(): Promise<void> {
  assertShutdownScope();
  fixture.events.push("producer:stop");
}
export const stopPostgresLiveDashboard = stop;
export const shutdownMcpHttp = stop;
export const stopRuntimeConfigHotReload = stop;
export const getJobRegistry = () => ({ close: stop });
export const stopRadarSyncSchedulerAsync = stop;
export const stopModelSyncSchedulerAsync = stop;
export const stopCodexCatalogRevalidation = stop;
export const closeCloudSyncScheduler = stop;
export const stopProviderLimitsSyncScheduler = stop;
export const stopConnectionRecoveryScheduler = stop;
export const stopBackgroundRefresh = stop;
export const stopSessionAccountAffinityCleanup = stop;
export const stopCleanupSchedulerAsync = stop;
export const stopCredentialHealthCheckAsync = stop;
export const stopQuotaAutoPingAsync = stop;
export const stopReasoningCacheCleanupJob = stop;
export const stopContextWindowReconcileAsync = stop;
export const stopMemoryDecaySweepAsync = stop;
export const stopFreeProxyAutoSyncAsync = stop;
export const stopProxyHealthCheckAsync = stop;
export const stopPeriodicSyncAsync = stop;
export const stopOpenRouterProviderStatsSync = stop;
export const stopArenaEloSyncAsync = stop;
export const stopConductorBridge = stop;
export const autoRefreshDaemon = { stop };
export const stopBatchProcessorAsync = stop;
export const waitForAllBatches = async () => {};
export const flushCcrDurableWrites = async () => {};
export const flushGeminiThoughtSignatureWrites = async () => {};
export const shutdownA2ATaskManagerRuntime = async () => {};
export const shutdownPluginRuntime = async () => {};
export const closeNativeNotionThreadSessions = async () => {};
export const closeBlockedHistoryAsync = async () => {};
export const stopChatGptWebCodexRuntime = async () => {};
export const closeNativeCompressionWorkerPool = async () => {};
export const closeNativeLlmlinguaWorker = async () => {};
export const stopNativeHostProbes = async () => {};
export const closeNativeCodeGraphQueries = async () => {};
export const stopRtkRawOutputPurgeScheduler = stop;
export const closeRtkRawOutputPurgeScheduler = async () => {};
export async function flushSpendBatchWriter(): Promise<void> {
  assertShutdownScope();
  fixture.events.push("cost:flush");
}
export async function closeCallLogSaves(): Promise<void> {
  assertShutdownScope();
  fixture.events.push("logs:close");
}
