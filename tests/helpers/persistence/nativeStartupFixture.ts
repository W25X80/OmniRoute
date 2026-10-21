import {
  startupFixture,
  runPersistenceBackgroundWork as scopedBackgroundWork,
} from "./startupLifetimeFixture.ts";

export * from "./startupLifetimeFixture.ts";
export const nativeStartupFixture = {
  readyBarrier: undefined as Promise<void> | undefined,
  cloudBarrier: undefined as Promise<void> | undefined,
  memoryBarrier: undefined as Promise<void> | undefined,
  optionalImportBarrier: undefined as Promise<void> | undefined,
  persistencePhase: "starting" as "starting" | "ready" | "failed" | "closing" | "closed",
  serverPhase: "starting" as "starting" | "ready" | "stopping",
};
const record = (event: string) => startupFixture.events.push(event);
export async function ensurePersistenceReady(): Promise<void> {
  record("factory:enter");
  await nativeStartupFixture.readyBarrier;
  if (startupFixture.readyFailure) {
    nativeStartupFixture.persistencePhase = "failed";
    throw new Error("Required bootstrap failed");
  }
  nativeStartupFixture.persistencePhase = "ready";
}
const background = new Set<Promise<unknown>>();
export function runPersistenceBackgroundWork<T>(body: () => Promise<T>): Promise<T> {
  const work = scopedBackgroundWork(body);
  background.add(work);
  void work.then(
    () => background.delete(work),
    () => background.delete(work)
  );
  return work;
}
export async function closePersistenceRuntime(): Promise<void> {
  while (background.size) await Promise.allSettled([...background]);
  record("pool:close");
  nativeStartupFixture.persistencePhase = "closed";
}
export function getPersistenceRuntimeStatus() {
  return { phase: nativeStartupFixture.persistencePhase };
}
export function getServerLifecyclePhase() {
  return nativeStartupFixture.serverPhase;
}
export function markServerStarting(): void {
  nativeStartupFixture.serverPhase = "starting";
  record("server:starting");
}
export function markServerReady(): void {
  nativeStartupFixture.serverPhase = "ready";
  record("server:ready");
}
export function markServerStopping(): void {
  nativeStartupFixture.serverPhase = "stopping";
  record("server:stopping");
}
export async function ensureCloudSyncInitialized(): Promise<boolean> {
  record("start:cloud");
  await nativeStartupFixture.cloudBarrier;
  return true;
}
export function startReasoningCacheCleanupJob(): void {
  record("start:reasoning");
}
export function startNativeArtifactRecovery(): void {
  record("start:artifactRecovery");
}
export function initProxyHealthCheck(): void {
  record("start:proxy");
}
export function initFreeProxyAutoSync(): void {
  record("start:freeProxy");
}
export function initGracefulShutdown(): void {
  record("start:graceful");
}
export function initApiBridgeServer(): void {
  record("start:apiBridge");
}
export function startSpendBatchWriter(): void {
  record("start:spend");
}
export function startBackgroundRefresh(): void {
  record("start:quota");
}
export function startProviderLimitsSyncScheduler(): void {
  record("start:providerLimits");
}
export function startQuotaAutoPing(): void {
  record("start:quotaPing");
}
export function initBatchProcessor(): void {
  record("start:batch");
}
export function startSessionAccountAffinityCleanup(): void {
  record("sqlite:affinity");
}
export async function startSessionAccountAffinityCleanupAsync(): Promise<void> {
  record("start:affinity");
}
export function startRuntimeConfigHotReload(): void {
  record("start:hotReload");
}
export function startCleanupScheduler(): void {
  record("start:cleanup");
}
export function initCredentialHealthCheck(): boolean {
  record("start:credential");
  return true;
}
export function initModelsDevSync(): boolean {
  record("start:modelsDev");
  return true;
}
export async function startPostgresLiveDashboard(): Promise<boolean> {
  record("start:liveDashboard");
  return false;
}
export async function stopPostgresLiveDashboard(): Promise<void> {
  record("stop:liveDashboard");
}
export const registerDefaultGuardrails = () => {};
export const registerBuiltinSkills = () => {};
export const skillExecutor = {};
export const registerSelectorTransitionSubscriber = () => {};
export const registerQuotaTrackersBatch = () => {};
export const registerCodexQuotaFetcher = () => {};
export const registerBailianCodingPlanQuotaFetcher = () => {};
export const registerQwenTokenPlanQuotaFetcher = () => {};
export const registerCrofUsageFetcher = () => {};
export const registerDeepseekQuotaFetcher = () => {};
export const registerMoonshotQuotaFetcher = () => {};
export const registerMoonshotFetchersForNodes = () => {};
export const registerOpenrouterQuotaFetcher = () => {};
export const registerOpencodeQuotaFetcher = () => {};
export const registerGrokWebQuotaFetcher = () => {};
export const registerGenericQuotaFetchers = () => {};
export const getProviderNodes = async () => [];
export const enforceWebRuntimeEnv = () => {};
export const assertEncryptionKeyConfiguredForProduction = () => {};
export const initConsoleInterceptor = () => {};
export const clearStaleCrashCooldownsAsync = async () => ({ cleared: 0 });
export const getCombos = async () => [];
export const scanCombosForModelCollisions = () => [];
export const warmAdaptiveVirtualLanesIntoRuntime = async () => false;
export const ensurePersistentManagementPasswordHash = async ({
  settings,
}: {
  settings: Record<string, unknown>;
}) => ({ settings });
export const applyRuntimeSettings = async () => [];
export const setSystemPromptConfig = () => {};
export const hydrateThinkingBudgetConfig = () => false;
export const hydrateTaskRoutingConfig = () => false;
export const seedDefaultModelAliases = async () => ({
  applied: [],
  skipped: [],
  removed: [],
  failed: [],
});
export const migrateCodexConnectionDefaultsFromLegacySettings = async () => ({
  migrated: false,
});
export const initAuditLog = () => {};
export const cleanupExpiredLogs = async () => ({});

export const autoRefreshDaemon = {
  start: async () => {
    record("start:autoRefresh");
  },
  stop: async () => {
    record("stop:autoRefresh");
  },
};
export const initConnectionRecoveryScheduler = () => {
  record("start:connectionRecovery");
};
export const startContextWindowReconcile = () => {
  record("start:context");
};
export const startMemoryDecaySweep = () => {
  record("start:memoryDecay");
};
export async function initMemoryBackends(): Promise<void> {
  record("start:memoryBackends");
  await nativeStartupFixture.memoryBarrier;
}
export const getUnifiedModelsResponse = async () => new Response("{}");
export const getOpenRouterCatalog = async () => [];

export function beginPersistenceAdmissionClose(): void {
  if (nativeStartupFixture.persistencePhase === "ready")
    nativeStartupFixture.persistencePhase = "closing";
}
export async function drainPersistenceAcceptedWork(): Promise<void> {
  while (background.size) await Promise.allSettled([...background]);
  record("accepted:drain");
}

export async function closeNativeNotionThreadSessions(): Promise<void> {
  record("notion:close");
}

export function initializePendingContinuationMaintenance(): void {
  record("maintenance:continuation:start");
}
export function startRuntimeResourceMonitor(): void {
  record("maintenance:resources:start");
}
export async function stopRuntimeResourceMonitor(): Promise<void> {
  record("maintenance:resources:stop");
}
export function stopPendingContinuationMaintenance(): void {
  record("maintenance:continuation:stop");
}
