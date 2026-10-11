import { parseDatabaseBackend } from "../db/backendSelection";

/** Essential native serving inputs; this barrier is independent of optional background warmup. */
export async function preparePostgresServingFileCaches(): Promise<void> {
  if (parseDatabaseBackend() !== "postgres") return;
  const [providers, lifecycle, fitness, alibaba] = await Promise.all([
    import("../../../open-sse/config/constants.ts"),
    import("../../../open-sse/services/modelLifecycle.ts"),
    import("../../../open-sse/services/autoCombo/taskFitness.ts"),
    import("../../../open-sse/services/alibabaFreeTierAllowlist.ts"),
  ]);
  await Promise.all([
    providers.prepareNativeProviders(),
    lifecycle.prepareNativeModelLifecycleSnapshot(),
    fitness.prepareNativeFitnessRetiredModels(),
    alibaba.prepareNativeAlibabaFreeTierAllowlist(),
  ]);
}
