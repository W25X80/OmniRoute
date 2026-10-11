import {
  runPersistenceShutdownWork,
  closePersistenceRuntime,
  getPersistenceRuntimeStatus,
  beginPersistenceAdmissionClose,
  drainPersistenceAcceptedWork,
} from "@/lib/db/persistenceRuntime";
import { runPostgresShutdown } from "./postgresBootstrap";

/** Stop every producer before draining owned durable work and closing the shared pool. */
export function shutdownPostgresApplication(): Promise<void> {
  beginPersistenceAdmissionClose();
  return runPostgresShutdown(stopPostgresApplication);
}

async function stopPostgresApplication(): Promise<void> {
  const { phase } = getPersistenceRuntimeStatus();
  if (phase !== "ready" && phase !== "closing") {
    // A failed factory never entered application startup or activated its producers.
    await closePersistenceRuntime();
    return;
  }
  beginPersistenceAdmissionClose();
  const failures: unknown[] = [];
  const attempt = async (operation: () => void | Promise<void>) => {
    try {
      await operation();
    } catch (error) {
      failures.push(error);
    }
  };
  try {
    await attempt(() =>
      runPersistenceShutdownWork(async () => {
        const loaded = await Promise.allSettled([
          import("@/lib/services/nativeLifecycle").then(
            (module) => () => module.beginNativeEmbeddedServicesClose()
          ),
          import("@/lib/startup/liveDashboard").then(
            (module) => () => module.stopPostgresLiveDashboard()
          ),
          import("@omniroute/open-sse/mcp-server/httpTransport.ts").then(
            (module) => () => module.shutdownMcpHttp()
          ),
          import("@/lib/config/hotReload").then(
            (module) => () => module.stopRuntimeConfigHotReload()
          ),
          import("@/lib/jobRegistry").then((module) => () => module.getJobRegistry().close()),
          import("@/lib/radar/scheduler").then(
            (module) => () => module.stopRadarSyncSchedulerAsync()
          ),
          import("@/shared/services/modelSyncScheduler").then(
            (module) => () => module.stopModelSyncSchedulerAsync()
          ),
          import("@/shared/services/codexCatalogRevalidation").then(
            (module) => () => module.stopCodexCatalogRevalidation()
          ),
          import("@/shared/services/cloudSyncScheduler").then(
            (module) => () => module.closeCloudSyncScheduler()
          ),
          import("@/shared/services/providerLimitsSyncScheduler").then(
            (module) => () => module.stopProviderLimitsSyncScheduler()
          ),
          import("@/lib/quota/connectionRecovery").then(
            (module) => () => module.stopConnectionRecoveryScheduler()
          ),
          import("@/domain/quotaCache").then((module) => () => module.stopBackgroundRefresh()),
          import("@/lib/db/sessionAccountAffinity").then(
            (module) => () => module.stopSessionAccountAffinityCleanup()
          ),
          import("@/lib/db/cleanup").then((module) => () => module.stopCleanupSchedulerAsync()),
          import("@/lib/credentialHealth/scheduler").then(
            (module) => () => module.stopCredentialHealthCheckAsync()
          ),
          import("@/lib/services/quotaAutoPing").then(
            (module) => () => module.stopQuotaAutoPingAsync()
          ),
          import("@/lib/jobs/reasoningCacheCleanupJob").then(
            (module) => () => module.stopReasoningCacheCleanupJob()
          ),
          import("@/lib/contextWindowResolver").then(
            (module) => () => module.stopContextWindowReconcileAsync()
          ),
          import("@/lib/memory/typedDecay").then(
            (module) => () => module.stopMemoryDecaySweepAsync()
          ),
          import("@/lib/freeProxyProviders/scheduler").then(
            (module) => () => module.stopFreeProxyAutoSyncAsync()
          ),
          import("@/lib/proxyHealth/scheduler").then(
            (module) => () => module.stopProxyHealthCheckAsync()
          ),
          import("@/lib/pricingSync").then((module) => () => module.stopPeriodicSyncAsync()),
          import("@/lib/modelsDevSync").then((module) => () => module.stopPeriodicSyncAsync()),
          import("@/lib/catalog/openrouterProviderStats").then(
            (module) => () => module.stopOpenRouterProviderStatsSync()
          ),
          import("@/lib/arenaEloSync").then((module) => () => module.stopArenaEloSyncAsync()),
          import("@/lib/conductor/boot").then((module) => () => module.stopConductorBridge()),
          import("@omniroute/open-sse/services/autoRefreshDaemon").then(
            (module) => () => module.autoRefreshDaemon.stop()
          ),
          import("@omniroute/open-sse/services/compression/engines/rtk/rawOutput.ts").then(
            (module) => () => module.stopRtkRawOutputPurgeScheduler()
          ),
          import("@omniroute/open-sse/services/batchProcessor").then(
            (module) => () => module.stopBatchProcessorAsync()
          ),
        ]);
        // Every stop closes its producer's admission synchronously, before awaiting a drain.
        const stopping = loaded.flatMap((result) => {
          if (result.status === "rejected") {
            failures.push(result.reason);
            return [];
          }
          return [attempt(result.value)];
        });
        await Promise.all(stopping);
        await attempt(async () =>
          (await import("@omniroute/open-sse/services/batchProcessor")).waitForAllBatches()
        );
      })
    );
    // This join is outside the shutdown job it observes; the plane stays ready for tails.
    await attempt(drainPersistenceAcceptedWork);
    await attempt(() =>
      runPersistenceShutdownWork(async () => {
        await attempt(async () => {
          await (await import("@/lib/spend/batchWriter")).flushSpendBatchWriter();
        });
        await attempt(async () =>
          (await import("@/lib/proxyHealth/blockedHistory")).closeBlockedHistoryAsync()
        );
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/services/compression/engines/rtk/rawOutput.ts")
          ).closeRtkRawOutputPurgeScheduler()
        );
        await attempt(async () => (await import("@/lib/usage/callLogs")).closeCallLogSaves());
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/services/compression/engines/ccr/index.ts")
          ).flushCcrDurableWrites()
        );
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/services/geminiThoughtSignatureStore.ts")
          ).flushGeminiThoughtSignatureWrites()
        );
        await attempt(async () =>
          (await import("@/lib/a2a/taskManager")).shutdownA2ATaskManagerRuntime()
        );
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/services/notionThreadSessions.ts")
          ).closeNativeNotionThreadSessions()
        );
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/executors/chatgpt-web-codex/runtime.ts")
          ).stopChatGptWebCodexRuntime()
        );
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/services/compression/compressionWorkerPool.ts")
          ).closeNativeCompressionWorkerPool()
        );
        await attempt(async () =>
          (
            await import("@omniroute/open-sse/services/compression/engines/llmlingua/worker.ts")
          ).closeNativeLlmlinguaWorker()
        );
        await attempt(async () =>
          (await import("@/lib/services/nativeLifecycle")).stopNativeEmbeddedServices()
        );
        await attempt(async () =>
          (await import("@/lib/services/nativeLifecycle")).disposeNativeEmbeddedServiceBuffers()
        );
        await attempt(async () => (await import("@/lib/plugins/manager")).shutdownPluginRuntime());
        // Accepted callers retain these request capabilities until their owned work settles.
        await attempt(async () => (await import("@/lib/nativeHostProbes")).stopNativeHostProbes());
        await attempt(async () =>
          (await import("@/lib/copilot/codegraphQueryClient")).closeNativeCodeGraphQueries()
        );
      })
    );
  } finally {
    await attempt(closePersistenceRuntime);
  }
  if (failures.length)
    throw new AggregateError(failures, "PostgreSQL application shutdown did not finish cleanly");
}
