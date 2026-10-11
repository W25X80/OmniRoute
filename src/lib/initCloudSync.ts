import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import initializeCloudSync from "@/shared/services/initializeCloudSync";
import { startModelSyncScheduler } from "@/shared/services/modelSyncScheduler";
import { isAutomatedTestProcess } from "@/shared/utils/testProcess";
import { getJobRegistry } from "@/lib/jobRegistry";
import { registerBudgetResetJob, registerBudgetResetJobAsync } from "@/lib/jobs/budgetResetJob";
import {
  registerTokenHealthCheck,
  registerTokenHealthCheckAsync,
} from "@/lib/jobs/tokenHealthCheckJob";
import { registerLogExportJob, registerLogExportJobAsync } from "@/lib/jobs/logExportJob";
import { backfillVolcPlanAutoSync } from "@/lib/providers/volcPlanAutoSyncBackfill";

// Initialize runtime background sync services once per server process.
declare global {
  var __omnirouteBackgroundSyncInitialization:
    { initialized: boolean; pending?: Promise<boolean> } | undefined;
}
const initialization = (globalThis.__omnirouteBackgroundSyncInitialization ??= {
  initialized: false,
});

export function shouldSkipCloudSyncInitialization(
  env: NodeJS.ProcessEnv = process.env,
  argv: string[] = process.argv
): boolean {
  if (env.NEXT_PHASE === "phase-production-build") {
    return true;
  }

  const raw = env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES;
  if (raw && new Set(["1", "true", "yes", "on"]).has(raw.trim().toLowerCase())) {
    return true;
  }

  return isAutomatedTestProcess(argv, env) && env.OMNIROUTE_ENABLE_RUNTIME_BACKGROUND_TASKS !== "1";
}

export async function ensureCloudSyncInitialized(): Promise<boolean> {
  if (shouldSkipCloudSyncInitialization()) return false;
  if (initialization.initialized) return true;
  if (initialization.pending) return initialization.pending;
  const native = parseDatabaseBackend() === "postgres";
  initialization.pending = (async () => {
    try {
      await initializeCloudSync();
      await backfillVolcPlanAutoSync();
      startModelSyncScheduler();
      const registry = getJobRegistry();
      if (native) {
        await registerBudgetResetJobAsync(registry);
        await registerTokenHealthCheckAsync(registry);
        await registerLogExportJobAsync(registry);
      } else {
        registerBudgetResetJob(registry);
        registerTokenHealthCheck(registry);
        registerLogExportJob(registry);
      }
      await registry.startAll();
      initialization.initialized = true;
      return true;
    } catch (error) {
      console.error("[ServerInit] Error initializing background sync services:", error);
      if (native) throw error;
      return false;
    }
  })();
  try {
    return await initialization.pending;
  } finally {
    initialization.pending = undefined;
  }
}

export default ensureCloudSyncInitialized;
