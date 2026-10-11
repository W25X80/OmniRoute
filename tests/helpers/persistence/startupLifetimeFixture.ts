import { AsyncLocalStorage } from "node:async_hooks";
import { sanitizeErrorMessage } from "../../../open-sse/utils/errorSanitization.ts";

export { sanitizeErrorMessage };
const scopes = new AsyncLocalStorage<number>();
export const startupFixture = {
  events: [] as string[],
  generation: 0,
  identity: {},
  readyFailure: false,
  bootFailure: false,
  stopFailure: false,
  flushFailure: false,
  importFailure: false,
  stopBarrier: undefined as Promise<void> | undefined,
  codexBarrier: undefined as Promise<void> | undefined,
  modelBarrier: undefined as Promise<void> | undefined,
  modelGenerations: [] as number[],
};
function record(name: string): void {
  startupFixture.events.push(name);
}
function assertScope(): number {
  const generation = scopes.getStore();
  if (generation === undefined) throw new Error("Persistence scope is absent");
  return generation;
}
export async function ensurePersistenceReady(): Promise<void> {
  record("ready");
  if (startupFixture.readyFailure) throw new Error("Required bootstrap failed");
}
export async function closePersistenceRuntime(): Promise<void> {
  record("pool:close");
}
export async function runPersistenceRequest<T>(body: () => T | Promise<T>): Promise<T> {
  record("request:enter");
  return scopes.run(++startupFixture.generation, body);
}
export async function runPersistenceBackgroundWork<T>(body: () => Promise<T>): Promise<T> {
  record("background:enter");
  return scopes.run(++startupFixture.generation, body);
}
export async function runPersistenceShutdownWork<T>(body: () => Promise<T>): Promise<T> {
  record("shutdown:enter");
  return scopes.run(++startupFixture.generation, body);
}
export function markServerStarting(): void {
  record("server:starting");
}
export function markServerReady(): void {
  record("server:ready");
}
export function markServerStopping(): void {
  record("server:stopping");
}
export function warnIfInferenceServerExposed(): void {
  assertScope();
  if (startupFixture.bootFailure) throw new Error("Required registrar failed");
}
export async function getSettings(): Promise<Record<string, unknown>> {
  assertScope();
  record("codex:read");
  await startupFixture.codexBarrier;
  assertScope();
  return {};
}
export async function updateSettings(): Promise<void> {
  assertScope();
}
export async function getProviderConnections(): Promise<[]> {
  startupFixture.modelGenerations.push(assertScope());
  await startupFixture.modelBarrier;
  assertScope();
  return [];
}
export async function isConnectionUnavailableToAuxiliaryActivity(): Promise<boolean> {
  assertScope();
  return false;
}
export function getRuntimePorts() {
  return { dashboardPort: 20128 };
}
export function timingSafeCompare(left: string, right: string) {
  return left === right;
}
export async function scheduleCodexCatalogRevalidation(): Promise<void> {
  assertScope();
  record("codex:registration");
}
async function stop(name: string): Promise<void> {
  record("stop:" + name);
  if (name === "hotReload") {
    await startupFixture.stopBarrier;
    if (startupFixture.stopFailure) throw new Error("Stopped producer failed");
  }
}
export async function shutdownMcpHttp(): Promise<void> {
  await stop("mcp");
}
export async function stopRuntimeConfigHotReload(): Promise<void> {
  await stop("hotReload");
}
export async function stopRadarSyncSchedulerAsync(): Promise<void> {
  await stop("radar");
}
export async function stopModelSyncSchedulerAsync(): Promise<void> {
  await stop("model");
}
export async function closeCloudSyncScheduler(): Promise<void> {
  await stop("cloud");
}
export async function stopProviderLimitsSyncScheduler(): Promise<void> {
  await stop("providerLimits");
}
export async function stopConnectionRecoveryScheduler(): Promise<void> {
  await stop("connectionRecovery");
}
export async function stopBackgroundRefresh(): Promise<void> {
  await stop("quota");
}
export async function stopSessionAccountAffinityCleanup(): Promise<void> {
  await stop("affinity");
}
export async function stopCleanupSchedulerAsync(): Promise<void> {
  await stop("cleanup");
}
export async function stopCredentialHealthCheckAsync(): Promise<void> {
  await stop("credential");
}
export async function stopQuotaAutoPingAsync(): Promise<void> {
  await stop("quotaPing");
}
export async function stopReasoningCacheCleanupJob(): Promise<void> {
  await stop("reasoning");
}
export async function stopContextWindowReconcileAsync(): Promise<void> {
  await stop("context");
}
export async function stopMemoryDecaySweepAsync(): Promise<void> {
  await stop("memory");
}
export async function stopFreeProxyAutoSyncAsync(): Promise<void> {
  await stop("freeProxy");
}
export async function stopProxyHealthCheckAsync(): Promise<void> {
  await stop("proxy");
}
export async function stopPeriodicSyncAsync(): Promise<void> {
  await stop("periodic");
}
export async function stopArenaEloSyncAsync(): Promise<void> {
  await stop("arena");
}
export async function stopConductorBridge(): Promise<void> {
  await stop("conductor");
}
export async function stopBatchProcessorAsync(): Promise<void> {
  await stop("batch");
}
export function getJobRegistry() {
  return { close: () => stop("jobs") };
}
export const autoRefreshDaemon = { stop: () => stop("autoRefresh") };
export async function waitForAllBatches(): Promise<void> {
  record("batch:drain");
}
async function flush(name: string): Promise<void> {
  assertScope();
  record("flush:" + name);
  if (name === "spend" && startupFixture.flushFailure) throw new Error("Flush failed");
}
export async function flushSpendBatchWriter(): Promise<void> {
  await flush("spend");
}
export async function closeCallLogSaves(): Promise<void> {
  await flush("callLogs");
}
export async function flushCcrDurableWrites(): Promise<void> {
  await flush("ccr");
}
export async function flushGeminiThoughtSignatureWrites(): Promise<void> {
  await flush("gemini");
}
export async function shutdownA2ATaskManagerRuntime(): Promise<void> {
  await flush("a2a");
}
export async function shutdownPluginRuntime(): Promise<void> {
  await flush("plugins");
}

export async function stopOpenRouterProviderStatsSync(): Promise<void> {
  await stop("openrouter");
}

export function getSelectedPersistenceIdentity(): object {
  return startupFixture.identity;
}

export async function stopCodexCatalogRevalidation(): Promise<void> {
  await stop("codex");
}
export function resolveModelSyncInternalBaseUrl(): string {
  return "http://127.0.0.1:20128";
}
export async function fetchModelSyncInternal(): Promise<Response> {
  assertScope();
  return new Response("{}", { status: 200 });
}
export async function getSyncedAvailableModelsForConnection(): Promise<[]> {
  assertScope();
  return [];
}
export async function replaceSyncedAvailableModelsForConnection(): Promise<void> {
  assertScope();
}

export function getPersistenceRuntimeStatus() {
  return { phase: startupFixture.readyFailure ? "failed" : "ready" };
}
export function getServerLifecyclePhase(): "starting" {
  return "starting";
}
export async function stopPostgresLiveDashboard(): Promise<void> {
  await stop("liveDashboard");
}

export function beginPersistenceAdmissionClose(): void {
  record("admission:close");
}
export async function drainPersistenceAcceptedWork(): Promise<void> {
  record("accepted:drain");
}

export async function closeNativeNotionThreadSessions(): Promise<void> {
  record("notion:close");
}
