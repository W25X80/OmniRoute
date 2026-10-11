import { shutdownFixture } from "./shutdownOrderingFixture.ts";
import { stopNativeHostProbes as stopProbes } from "../../../src/lib/nativeHostProbes.ts";
import { closeNativeCodeGraphQueries as closeGraph } from "../../../src/lib/copilot/codegraphQueryClient.ts";

export * from "./shutdownOrderingFixture.ts";

// Unrelated runtime transports are outside this no-SQL composition oracle.
export async function closeBlockedHistoryAsync(): Promise<void> {}
export async function stopChatGptWebCodexRuntime(): Promise<void> {}

export async function stopNativeHostProbes(): Promise<void> {
  shutdownFixture.events.push("probe:closing");
  await stopProbes();
  shutdownFixture.events.push("probe:closed");
}
export async function closeNativeCodeGraphQueries(): Promise<void> {
  shutdownFixture.events.push("graph:closing");
  await closeGraph();
  shutdownFixture.events.push("graph:closed");
}
