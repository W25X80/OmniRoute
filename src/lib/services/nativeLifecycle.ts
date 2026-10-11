import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import {
  getPersistenceRuntimeStatus,
  isApplicationPersistenceIdentity,
  runPersistenceBackgroundWork,
} from "@/lib/db/persistenceRuntime";
import { getServerLifecyclePhase } from "@/lib/serverLifecycle";
import type { ServiceSupervisor } from "./ServiceSupervisor";

export interface NativeEmbeddedServices {
  readonly identity: object;
  readonly supervisors: Map<string, ServiceSupervisor>;
  readonly callbacks: Set<Promise<unknown>>;
  readonly modelTimers: Map<string, ReturnType<typeof setInterval>>;
  readonly modelRuns: Map<string, Set<Promise<void>>>;
  closing: boolean;
  stopping?: Promise<void>;
  disposing?: Promise<void>;
}

const EMBEDDED_SERVICES = Symbol.for("omniroute.nativeEmbeddedServices");
const serviceGlobals = globalThis as typeof globalThis & {
  [EMBEDDED_SERVICES]?: NativeEmbeddedServices;
};

/** Lookup does not create resources, initialize persistence, or load a supervisor. */
export function getNativeEmbeddedServices(): NativeEmbeddedServices | undefined {
  const owner = serviceGlobals[EMBEDDED_SERVICES];
  if (owner && !isApplicationPersistenceIdentity(owner.identity)) {
    throw new Error("Embedded service persistence identity changed");
  }
  return owner;
}

/** Only resources constructed under the held application authority can be registered. */
export function captureNativeEmbeddedServices(): NativeEmbeddedServices | undefined {
  if (parseDatabaseBackend() !== "postgres") return undefined;
  const identity = getSelectedPersistenceIdentity();
  if (!identity || !isApplicationPersistenceIdentity(identity)) {
    throw new Error("Embedded services require application persistence");
  }
  const existing = getNativeEmbeddedServices();
  if (existing) {
    if (existing.identity !== identity)
      throw new Error("Embedded service persistence identity changed");
    return existing;
  }
  return (serviceGlobals[EMBEDDED_SERVICES] = {
    identity,
    supervisors: new Map(),
    callbacks: new Set(),
    modelTimers: new Map(),
    modelRuns: new Map(),
    closing: false,
  });
}

export function assertNativeServiceActivation(owner: NativeEmbeddedServices): void {
  if (
    owner.closing ||
    getServerLifecyclePhase() === "stopping" ||
    getPersistenceRuntimeStatus().phase !== "ready"
  ) {
    throw new Error("Embedded services are stopping");
  }
  if (!isApplicationPersistenceIdentity(owner.identity)) {
    throw new Error("Embedded service persistence identity changed");
  }
}

/** A child or timer callback enters a fresh catalog scope; it never reuses startup ALS. */
export function runNativeServiceWork<T>(
  owner: NativeEmbeddedServices,
  body: () => Promise<T>
): Promise<T> {
  const work = runPersistenceBackgroundWork(async () => {
    if (
      !isApplicationPersistenceIdentity(owner.identity) ||
      getSelectedPersistenceIdentity() !== owner.identity
    ) {
      throw new Error("Embedded service persistence identity changed");
    }
    return body();
  });
  owner.callbacks.add(work);
  void work.then(
    () => owner.callbacks.delete(work),
    () => owner.callbacks.delete(work)
  );
  return work;
}

export function observeNativeServiceWork(
  owner: NativeEmbeddedServices,
  body: () => Promise<void>
): void {
  void runNativeServiceWork(owner, body).catch((error: unknown) => {
    console.warn(
      `[Services] Background work failed: ${sanitizeErrorMessage(error instanceof Error ? error.message : String(error))}`
    );
  });
}

/** Stop producer admission without removing capabilities still held by accepted requests. */
export function beginNativeEmbeddedServicesClose(): void {
  const owner = getNativeEmbeddedServices();
  if (!owner) return;
  owner.closing = true;
  for (const timer of owner.modelTimers.values()) clearInterval(timer);
  owner.modelTimers.clear();
  for (const supervisor of owner.supervisors.values()) supervisor.stopBackgroundProducers();
}

/** Stop owned children after accepted callers settle; adopted host processes remain untouched. */
export function stopNativeEmbeddedServices(): Promise<void> {
  beginNativeEmbeddedServicesClose();
  const owner = getNativeEmbeddedServices();
  if (!owner) return Promise.resolve();
  owner.stopping ??= (async () => {
    const stopped = await Promise.allSettled(
      Array.from(owner.supervisors.values()).map((supervisor) => supervisor.stop())
    );
    while (owner.callbacks.size) await Promise.allSettled(Array.from(owner.callbacks));
    const failures = stopped.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : []
    );
    if (failures.length)
      throw new AggregateError(failures, "Embedded services did not stop cleanly");
  })();
  return owner.stopping;
}

/** Final sink stage, after accepted work and child callbacks have drained. */
export function disposeNativeEmbeddedServiceBuffers(): Promise<void> {
  const owner = getNativeEmbeddedServices();
  if (!owner) return Promise.resolve();
  owner.disposing ??= (async () => {
    const disposed = await Promise.allSettled(
      Array.from(owner.supervisors.values()).map((supervisor) =>
        supervisor.getRingBuffer().disposeAsync()
      )
    );
    const failures = disposed.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : []
    );
    if (failures.length)
      throw new AggregateError(failures, "Embedded service log buffers did not close cleanly");
  })();
  return owner.disposing;
}
