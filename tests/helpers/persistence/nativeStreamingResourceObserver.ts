import { createHook } from "node:async_hooks";
import { getUpstreamTimeoutConfig } from "../../../src/shared/utils/runtimeTimeouts.ts";

interface ResourceOrigin {
  asyncId: number;
  type: string;
  triggerAsyncId: number;
  creationStack: string;
}

interface DiagnosticReporter {
  diagnostic(message: string): void;
}

const MAX_RESOURCE_ORIGINS = 1024;
const MAX_CREATION_STACK_CHARS = 8192;
const OBSERVED_TYPES = new Set([
  "MESSAGEPORT",
  "WORKER",
  "TCPWRAP",
  "TCPSERVERWRAP",
  "TLSWRAP",
  "PIPEWRAP",
  "Timeout",
  "FSEVENTWRAP",
]);

/** Read-only fixture diagnostics: never hold or inspect the native resource object. */
export function observeNativeStreamingResources() {
  const initialResourceTypes = process.getActiveResourcesInfo();
  const configuredTimeouts = getUpstreamTimeoutConfig();
  const origins = new Map<number, ResourceOrigin>();
  let omittedOrigins = 0;
  let truncatedStacks = 0;
  const hook = createHook({
    init(asyncId, type, triggerAsyncId) {
      if (!OBSERVED_TYPES.has(type)) return;
      if (origins.size >= MAX_RESOURCE_ORIGINS) {
        omittedOrigins++;
        return;
      }
      // Stack formatting can trigger source-map work; do not observe the observer itself.
      hook.disable();
      let creationStack = "Stack unavailable";
      try {
        const stack = new Error("Fixture resource initialized").stack;
        if (typeof stack === "string") {
          if (stack.length > MAX_CREATION_STACK_CHARS) truncatedStacks++;
          creationStack = stack.slice(0, MAX_CREATION_STACK_CHARS);
        }
      } catch {
        creationStack = "Stack formatting unavailable";
      } finally {
        hook.enable();
      }
      origins.set(asyncId, { asyncId, type, triggerAsyncId, creationStack });
    },
    destroy(asyncId) {
      origins.delete(asyncId);
    },
  });
  hook.enable();
  return {
    report(reporter: DiagnosticReporter): void {
      hook.disable();
      const report = {
        initialResourceTypes,
        activeResourceTypesAfterCleanup: process.getActiveResourcesInfo(),
        configuredTimeouts,
        observedOriginsAfterCleanup: [...origins.values()],
        observedResourceTypes: [...OBSERVED_TYPES],
        maxResourceOrigins: MAX_RESOURCE_ORIGINS,
        omittedOrigins,
        truncatedStacks,
        maxCreationStackChars: MAX_CREATION_STACK_CHARS,
        truncated: omittedOrigins > 0 || truncatedStacks > 0,
        metadataMayIncludeUnreferencedOrClosingResources: true,
        creationStackCaptureTemporarilyDisablesObserver: true,
      };
      origins.clear();
      reporter.diagnostic(
        `Native stream resources after fixture cleanup: ${JSON.stringify(report)}`
      );
    },
  };
}
