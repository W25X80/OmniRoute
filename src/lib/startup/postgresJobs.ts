import { ensureCloudSyncInitialized } from "@/lib/initCloudSync";
import { startReasoningCacheCleanupJob } from "@/lib/jobs/reasoningCacheCleanupJob";
import { assertPostgresBootstrapActive } from "./postgresBootstrap";

/** Use the shared selected-backend bootstrap so each runtime producer has one owner. */
export async function startPostgresRuntimeJobs(): Promise<void> {
  assertPostgresBootstrapActive();
  const { startNativeArtifactRecovery } = await import("@/lib/usage/nativeArtifactRecovery");
  assertPostgresBootstrapActive();
  startNativeArtifactRecovery();
  await ensureCloudSyncInitialized();
  assertPostgresBootstrapActive();
  startReasoningCacheCleanupJob();
}
