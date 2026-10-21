import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { sampleResourceSignals } from "@omniroute/open-sse/utils/resourcePressureSampler.ts";
import { getPersistenceRuntimeStatus } from "@/lib/db/persistenceRuntime";
import { getPendingContinuationStats } from "@/lib/db/responsesContinuationStore";
import { getCallLogArtifactQueueStats } from "@/lib/usage/callLogArtifactWriter";
import { logger } from "@/shared/utils/logger";

type Sample = Awaited<ReturnType<typeof sampleResourceSignals>>;
type MonitorOptions = {
  sample?: () => Promise<Sample>;
  emit?: (sample: Sample, loop: { maximumMs: number; utilization: number }) => void;
  schedule?: (tick: () => void) => ReturnType<typeof setInterval>;
};

/** Startup owns the timer; it must never inherit a request's payload or persistence scope. */
export function createRuntimeResourceMonitor(options: MonitorOptions = {}) {
  const histogram = monitorEventLoopDelay({ resolution: 20 });
  const sample = options.sample ?? sampleResourceSignals;
  const emit =
    options.emit ??
    ((signals, eventLoop) => {
      const { retainedBytes, byteRefusals } = getCallLogArtifactQueueStats();
      logger.info(
        {
          module: "runtime.resources",
          memory: signals,
          eventLoop,
          persistence: getPersistenceRuntimeStatus(),
          pendingContinuation: getPendingContinuationStats(),
          artifactSerialization: { retainedBytes, byteRefusals },
        },
        "Runtime resource observation"
      );
    });
  let interval: ReturnType<typeof setInterval> | undefined;
  let pending: Promise<void> | undefined;
  let active = false;
  let generation = 0;
  let previous = performance.eventLoopUtilization();
  const tick = () => {
    if (!active || pending) return;
    const observedGeneration = generation;
    const current = performance.eventLoopUtilization();
    const utilization = performance.eventLoopUtilization(current, previous).utilization;
    previous = current;
    const maximumMs = histogram.max / 1e6;
    histogram.reset();
    pending = Promise.resolve()
      .then(sample)
      .then((signals) => {
        if (active && generation === observedGeneration) emit(signals, { maximumMs, utilization });
      })
      .catch(() => {
        // Diagnostics cannot fail serving or create an unhandled rejection.
      })
      .finally(() => {
        pending = undefined;
      });
  };
  return {
    start() {
      if (active) return;
      active = true;
      generation++;
      previous = performance.eventLoopUtilization();
      histogram.reset();
      histogram.enable();
      interval = options.schedule ? options.schedule(tick) : setInterval(tick, 30_000);
      interval.unref?.();
    },
    async stop() {
      active = false;
      generation++;
      if (interval) clearInterval(interval);
      interval = undefined;
      histogram.disable();
      await pending;
    },
  };
}

const key = Symbol.for("omniroute.runtime-resource-monitor.v1");
const globals = globalThis as typeof globalThis & {
  [key]?: ReturnType<typeof createRuntimeResourceMonitor>;
};

export function startRuntimeResourceMonitor(): void {
  (globals[key] ??= createRuntimeResourceMonitor()).start();
}

export async function stopRuntimeResourceMonitor(): Promise<void> {
  await globals[key]?.stop();
}
