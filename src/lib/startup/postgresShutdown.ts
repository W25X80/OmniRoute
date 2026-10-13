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
  const attempt = async (
    operation: () => void | Promise<void>,
    owner = operation.name || "cleanup"
  ) => {
    const started = Date.now();
    console.info("[Shutdown] Owner started", { owner, ...getPersistenceRuntimeStatus() });
    try {
      await operation();
    } catch (error) {
      failures.push(error);
      console.error("[Shutdown] Owner failed", { owner });
    } finally {
      console.info("[Shutdown] Owner settled", {
        owner,
        elapsedMs: Date.now() - started,
        ...getPersistenceRuntimeStatus(),
      });
    }
  };
  try {
    await attempt(
      () =>
        runPersistenceShutdownWork(async () => {
          const loaded = await Promise.allSettled([
            import("@/lib/spend/batchWriter").then(
              (module) =>
                function beginNativeSpendBatchWriterClose() {
                  return module.beginNativeSpendBatchWriterClose();
                }
            ),
            import("@/lib/usage/nativeArtifactRecovery").then(
              (module) =>
                function stopNativeArtifactRecovery() {
                  return module.stopNativeArtifactRecovery();
                }
            ),
            import("@omniroute/open-sse/executors/antigravity.ts").then(
              (module) =>
                function stopAntigravityCreditSweep() {
                  return module.stopAntigravityCreditSweep();
                }
            ),
            import("@/lib/proxyLogger").then(
              (module) =>
                function beginNativeProxyLogClose() {
                  return module.beginNativeProxyLogClose();
                }
            ),
            import("@/lib/services/nativeLifecycle").then(
              (module) =>
                function beginNativeEmbeddedServicesClose() {
                  return module.beginNativeEmbeddedServicesClose();
                }
            ),
            import("@/lib/headroom/nativeProcess").then(
              (module) =>
                function beginNativeHeadroomClose() {
                  return module.beginNativeHeadroomClose();
                }
            ),
            import("@/lib/startup/liveDashboard").then(
              (module) =>
                function stopPostgresLiveDashboard() {
                  return module.stopPostgresLiveDashboard();
                }
            ),
            import("@omniroute/open-sse/mcp-server/httpTransport.ts").then(
              (module) =>
                function shutdownMcpHttp() {
                  return module.shutdownMcpHttp();
                }
            ),
            import("@/lib/config/hotReload").then(
              (module) =>
                function stopRuntimeConfigHotReload() {
                  return module.stopRuntimeConfigHotReload();
                }
            ),
            import("@/lib/jobRegistry").then(
              (module) =>
                function closeJobRegistry() {
                  return module.getJobRegistry().close();
                }
            ),
            import("@/lib/radar/scheduler").then(
              (module) =>
                function stopRadarSyncSchedulerAsync() {
                  return module.stopRadarSyncSchedulerAsync();
                }
            ),
            import("@/shared/services/modelSyncScheduler").then(
              (module) =>
                function stopModelSyncSchedulerAsync() {
                  return module.stopModelSyncSchedulerAsync();
                }
            ),
            import("@/shared/services/codexCatalogRevalidation").then(
              (module) =>
                function stopCodexCatalogRevalidation() {
                  return module.stopCodexCatalogRevalidation();
                }
            ),
            import("@/shared/services/cloudSyncScheduler").then(
              (module) =>
                function closeCloudSyncScheduler() {
                  return module.closeCloudSyncScheduler();
                }
            ),
            import("@/shared/services/providerLimitsSyncScheduler").then(
              (module) =>
                function stopProviderLimitsSyncScheduler() {
                  return module.stopProviderLimitsSyncScheduler();
                }
            ),
            import("@/lib/quota/connectionRecovery").then(
              (module) =>
                function stopConnectionRecoveryScheduler() {
                  return module.stopConnectionRecoveryScheduler();
                }
            ),
            import("@/domain/quotaCache").then(
              (module) =>
                function stopBackgroundRefresh() {
                  return module.stopBackgroundRefresh();
                }
            ),
            import("@/lib/db/sessionAccountAffinity").then(
              (module) =>
                function stopSessionAccountAffinityCleanup() {
                  return module.stopSessionAccountAffinityCleanup();
                }
            ),
            import("@/lib/db/cleanup").then(
              (module) =>
                function stopCleanupSchedulerAsync() {
                  return module.stopCleanupSchedulerAsync();
                }
            ),
            import("@/lib/credentialHealth/scheduler").then(
              (module) =>
                function stopCredentialHealthCheckAsync() {
                  return module.stopCredentialHealthCheckAsync();
                }
            ),
            import("@/lib/services/quotaAutoPing").then(
              (module) =>
                function stopQuotaAutoPingAsync() {
                  return module.stopQuotaAutoPingAsync();
                }
            ),
            import("@/lib/jobs/reasoningCacheCleanupJob").then(
              (module) =>
                function stopReasoningCacheCleanupJob() {
                  return module.stopReasoningCacheCleanupJob();
                }
            ),
            import("@/lib/contextWindowResolver").then(
              (module) =>
                function stopContextWindowReconcileAsync() {
                  return module.stopContextWindowReconcileAsync();
                }
            ),
            import("@/lib/memory/typedDecay").then(
              (module) =>
                function stopMemoryDecaySweepAsync() {
                  return module.stopMemoryDecaySweepAsync();
                }
            ),
            import("@/lib/freeProxyProviders/scheduler").then(
              (module) =>
                function stopFreeProxyAutoSyncAsync() {
                  return module.stopFreeProxyAutoSyncAsync();
                }
            ),
            import("@/lib/proxySubscription/subscriptionService").then(
              (module) =>
                function stopSubscriptionSchedulerAsync() {
                  return module.stopSubscriptionSchedulerAsync();
                }
            ),
            import("@/lib/proxyHealth/scheduler").then(
              (module) =>
                function stopProxyHealthCheckAsync() {
                  return module.stopProxyHealthCheckAsync();
                }
            ),
            import("@/lib/pricingSync").then(
              (module) =>
                function stopPeriodicSyncAsync() {
                  return module.stopPeriodicSyncAsync();
                }
            ),
            import("@/lib/modelsDevSync").then(
              (module) =>
                function stopPeriodicSyncAsync() {
                  return module.stopPeriodicSyncAsync();
                }
            ),
            import("@/lib/catalog/openrouterProviderStats").then(
              (module) =>
                function stopOpenRouterProviderStatsSync() {
                  return module.stopOpenRouterProviderStatsSync();
                }
            ),
            import("@/lib/arenaEloSync").then(
              (module) =>
                function stopArenaEloSyncAsync() {
                  return module.stopArenaEloSyncAsync();
                }
            ),
            import("@/lib/conductor/boot").then(
              (module) =>
                function stopConductorBridge() {
                  return module.stopConductorBridge();
                }
            ),
            import("@omniroute/open-sse/services/autoRefreshDaemon").then(
              (module) => () => module.autoRefreshDaemon.stop()
            ),
            import("@omniroute/open-sse/services/compression/engines/rtk/rawOutput.ts").then(
              (module) =>
                function stopRtkRawOutputPurgeScheduler() {
                  return module.stopRtkRawOutputPurgeScheduler();
                }
            ),
            import("@omniroute/open-sse/services/batchProcessor").then(
              (module) =>
                function stopBatchProcessorAsync() {
                  return module.stopBatchProcessorAsync();
                }
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
        }),
      "producer-stop"
    );
    // This join is outside the shutdown job it observes; the plane stays ready for tails.
    await attempt(drainPersistenceAcceptedWork, "accepted-work-drain");
    await attempt(
      () =>
        runPersistenceShutdownWork(async () => {
          await attempt(async () => {
            await (await import("@/lib/spend/batchWriter")).closeNativeSpendBatchWriter();
          });
          await attempt(async () =>
            (await import("@/lib/proxyHealth/blockedHistory")).closeBlockedHistoryAsync()
          );
          await attempt(async () =>
            (
              await import("@omniroute/open-sse/services/compression/engines/rtk/rawOutput.ts")
            ).closeRtkRawOutputPurgeScheduler()
          );
          await attempt(async () => (await import("@/lib/proxyLogger")).closeProxyLogSavesAsync());
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
          await attempt(async () =>
            (await import("@/lib/plugins/manager")).shutdownPluginRuntime()
          );
          // Accepted callers retain these request capabilities until their owned work settles.
          await attempt(async () =>
            (await import("@/lib/headroom/nativeProcess")).stopNativeHeadroomProcesses()
          );
          await attempt(async () =>
            (await import("@/lib/nativeHostProbes")).stopNativeHostProbes()
          );
          await attempt(async () =>
            (await import("@/lib/copilot/codegraphQueryClient")).closeNativeCodeGraphQueries()
          );
          await attempt(async () =>
            (await import("@/lib/db/ompHostClient")).closeNativeOmpHostStorage()
          );
        }),
      "durable-cleanup"
    );
  } finally {
    await attempt(closePersistenceRuntime, "persistence-close");
  }
  if (failures.length)
    throw new AggregateError(failures, "PostgreSQL application shutdown did not finish cleanly");
}
