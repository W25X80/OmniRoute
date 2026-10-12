import { SpendBatchWriter } from "../../../src/lib/spend/batchWriter.ts";
import { runPersistenceBackgroundWork } from "../../../src/lib/db/persistenceRuntime.ts";
import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";

export const shutdownFixture = {
  events: [] as string[],
  persistedCost: 0,
  logRows: [] as string[],
  logClosing: false,
  stopFailure: false,
  onPhase: undefined as ((phase: string) => void) | undefined,
  reset() {
    writer.resetForTests();
    shutdownFixture.events = [];
    shutdownFixture.persistedCost = 0;
    shutdownFixture.logRows = [];
    shutdownFixture.logClosing = false;
    shutdownFixture.stopFailure = false;
    shutdownFixture.onPhase = undefined;
  },
};
const writer = new SpendBatchWriter({
  flushIntervalMs: 3_600_000,
  maxBufferSize: 1000,
  persistEntries: (entries) =>
    runPersistenceBackgroundWork(async () => {
      if (!getSelectedPersistenceIdentity()) throw new Error("Native shutdown authority absent");
      shutdownFixture.persistedCost += entries.reduce((sum, entry) => sum + entry.cost, 0);
      shutdownFixture.events.push("cost:persisted");
    }),
  logger: { log() {}, error() {} },
});
export function recordTailCost(amount: number): void {
  writer.increment("shutdown-tail", amount);
  shutdownFixture.events.push("tail:cost-buffered");
}
export async function saveTailLog(id: string): Promise<void> {
  if (!shutdownFixture.logClosing) shutdownFixture.logRows.push(id);
  shutdownFixture.events.push("tail:log-returned");
}
export async function flushSpendBatchWriter(): Promise<void> {
  shutdownFixture.events.push("cost:flush");
  await writer.flush();
}
export async function closeCallLogSaves(): Promise<void> {
  shutdownFixture.logClosing = true;
  shutdownFixture.events.push("logs:close");
}
function recordShutdownPhase(phase: string): void {
  if (globalThis.__omniroutePersistenceRuntime?.owners.getStore()?.active !== true)
    throw new Error("Producer stop must own its shutdown scope");
  if (!getSelectedPersistenceIdentity()) throw new Error("Producer stop authority absent");
  shutdownFixture.events.push(phase);
  shutdownFixture.onPhase?.(phase);
  if (shutdownFixture.stopFailure) throw new Error("Producer stop failed");
}
async function stop(): Promise<void> {
  recordShutdownPhase("producer:stop");
}
export function beginNativeEmbeddedServicesClose(): void {
  recordShutdownPhase("embedded:admission-close");
}
export async function stopRtkRawOutputPurgeScheduler(): Promise<void> {
  recordShutdownPhase("rtk:admission-close");
}
export async function closeBlockedHistoryAsync(): Promise<void> {
  recordShutdownPhase("history:close");
}
export async function closeRtkRawOutputPurgeScheduler(): Promise<void> {
  recordShutdownPhase("rtk:close");
}
export async function stopChatGptWebCodexRuntime(): Promise<void> {
  recordShutdownPhase("codex:close");
}
export async function closeNativeCompressionWorkerPool(): Promise<void> {
  recordShutdownPhase("compression:close");
}
export async function closeNativeLlmlinguaWorker(): Promise<void> {
  recordShutdownPhase("llmlingua:close");
}
export async function stopNativeEmbeddedServices(): Promise<void> {
  recordShutdownPhase("embedded:stop");
}
export async function disposeNativeEmbeddedServiceBuffers(): Promise<void> {
  recordShutdownPhase("embedded:buffers-close");
}
export async function stopNativeHostProbes(): Promise<void> {
  recordShutdownPhase("host:close");
}
export async function closeNativeCodeGraphQueries(): Promise<void> {
  recordShutdownPhase("codegraph:close");
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

export async function closeNativeNotionThreadSessions(): Promise<void> {
  shutdownFixture.events.push("notion:close");
}

export function beginNativeHeadroomClose(): void {
  recordShutdownPhase("headroom:admission-close");
}
export async function stopNativeHeadroomProcesses(): Promise<void> {
  recordShutdownPhase("headroom:close");
}
export async function closeNativeOmpHostStorage(): Promise<void> {
  recordShutdownPhase("omp:close");
}
