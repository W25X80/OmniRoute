import { AsyncLocalStorage } from "node:async_hooks";
import type { NativeArtifactStore } from "@/domain/persistence/artifactStore";
import {
  parseDatabaseBackend,
  readNativeScratchDataDir,
  type DatabaseBackend,
} from "./backendSelection";
import {
  getSelectedPersistenceIdentity,
  installApplicationPersistence,
  UnsupportedPersistenceDomainError,
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "./controlPlaneContext";
import type { PostgresControlPlane } from "./postgres/controlPlane";

type Phase = "starting" | "ready" | "closing" | "closed" | "failed";
type Work = Promise<unknown>;
interface WorkOwner {
  active: boolean;
}
interface Runtime {
  readonly backend: DatabaseBackend;
  phase: Phase;
  startup?: Promise<void>;
  plane?: PostgresControlPlane;
  artifacts?: NativeArtifactStore;
  applicationHold?: Promise<void>;
  applicationIdentity?: object;
  releaseApplication?: () => void;
  requests: Set<Work>;
  background: Set<Work>;
  tails: Set<Work>;
  owners: AsyncLocalStorage<WorkOwner>;
  closing?: Promise<void>;
}

declare global {
  var __omniroutePersistenceRuntime: Runtime | undefined;
}

/** No allocation or startup: identify only the application authority already held by this runtime. */
export function isApplicationPersistenceIdentity(identity: object | undefined): boolean {
  const state = globalThis.__omniroutePersistenceRuntime;
  return Boolean(
    identity && state?.backend === "postgres" && state.applicationIdentity === identity
  );
}

export const PERSISTENCE_STREAM_CAPACITY_CODE = "PERSISTENCE_STREAM_CAPACITY";

// Next can bundle this owner more than once; only locally constructed refusals are branded.
const admissionErrorKey = Symbol.for("omniroute.persistence.stream-admission-errors.v1");
const admissionErrorState = globalThis as typeof globalThis & {
  [admissionErrorKey]?: WeakSet<object>;
};
const admissionErrors = (admissionErrorState[admissionErrorKey] ??= new WeakSet<object>());

export class PersistenceStreamCapacityError extends Error {
  constructor() {
    super("Persistence stream admission is unavailable");
    this.name = "PersistenceStreamCapacityError";
    admissionErrors.add(this);
  }
}

export function isPersistenceStreamCapacityError(error: unknown): boolean {
  return error !== null && typeof error === "object" && admissionErrors.has(error);
}

const BACKGROUND_LIMIT = 64;
const TAIL_LIMIT = 64;

function runtime(): Runtime {
  const backend = parseDatabaseBackend();
  const existing = globalThis.__omniroutePersistenceRuntime;
  if (existing) {
    if (existing.backend !== backend) {
      throw new Error("Changing the database backend requires a restart");
    }
    return existing;
  }
  return (globalThis.__omniroutePersistenceRuntime = {
    backend,
    phase: "starting",
    requests: new Set(),
    background: new Set(),
    tails: new Set(),
    owners: new AsyncLocalStorage<WorkOwner>(),
  });
}

async function initializePostgres(state: Runtime): Promise<void> {
  readNativeScratchDataDir();
  const [connectionModule, planeModule, artifactConfig, artifactFactory] = await Promise.all([
    import("./postgres/environment"),
    import("./postgres/controlPlane"),
    import("../artifacts/s3ArtifactConfiguration"),
    import("../artifacts/s3ArtifactStore"),
  ]);
  const connection = await connectionModule.readPostgresEnvironment();
  const configuration = artifactConfig.readS3ArtifactConfiguration();
  const plane = await planeModule.initializePostgresControlPlane({
    connection,
    logger: console,
  });
  state.plane = plane;
  try {
    state.artifacts = artifactFactory.createS3ArtifactStore(configuration);
    process.env.JWT_SECRET = plane.bootstrapSecrets.jwtSecret;
    process.env.API_KEY_SECRET = plane.bootstrapSecrets.apiKeySecret;
    let enter!: () => void;
    let fail!: (error: unknown) => void;
    const entered = new Promise<void>((resolve, reject) => {
      enter = resolve;
      fail = reject;
    });
    const stopped = new Promise<void>((resolve) => {
      state.releaseApplication = resolve;
    });
    state.applicationHold = plane.run(async (binding) => {
      const uninstall = installApplicationPersistence(binding);
      state.applicationIdentity = binding.databaseIdentity;
      try {
        const [breakerModule, quotaModule] = await Promise.all([
          import("@/shared/utils/circuitBreaker"),
          import("@/domain/quotaCache"),
        ]);
        await breakerModule.initializeCircuitBreakerPersistence(binding.database!);
        await quotaModule.initializeQuotaSnapshotCache();
        enter();
        await stopped;
      } finally {
        state.applicationIdentity = undefined;
        uninstall();
      }
    });
    void state.applicationHold.catch(fail);
    await entered;
  } catch (error) {
    state.releaseApplication?.();
    await state.applicationHold?.catch(() => {});
    await Promise.allSettled([state.artifacts?.close(), plane.close()]);
    throw error;
  }
}

/** Idempotent startup. Failure never selects another persistence authority. */
export async function ensurePersistenceReady(): Promise<void> {
  const state = runtime();
  if (state.phase === "ready") return;
  if (state.phase !== "starting") throw new Error("Application persistence is unavailable");
  state.startup ??= (async () => {
    try {
      if (state.backend === "postgres") await initializePostgres(state);
      else await (await import("./core")).ensureDbInitialized();
      if (state.phase !== "starting") throw new Error("Application persistence is draining");
      state.phase = "ready";
    } catch (error) {
      if (state.phase === "starting") state.phase = "failed";
      throw error;
    }
  })();
  return state.startup;
}

async function ownedScope<T>(state: Runtime, body: () => T | Promise<T>): Promise<T> {
  const owner: WorkOwner = { active: true };
  try {
    return await state.owners.run(owner, body);
  } finally {
    owner.active = false;
  }
}

async function catalogScope<T>(
  state: Runtime,
  binding: PostgresControlPlaneBinding,
  body: () => T | Promise<T>
): Promise<T> {
  const { withPostgresCatalogSnapshot } = await import("./postgres/catalogSnapshot");
  return withPostgresControlPlane(binding, () =>
    ownedScope(state, () => withPostgresCatalogSnapshot(binding.database!, body))
  );
}

async function freshScope<T>(state: Runtime, body: () => T | Promise<T>): Promise<T> {
  if (!state.plane) throw new Error("PostgreSQL persistence is unavailable");
  return state.plane.run((binding) => catalogScope(state, binding, body));
}

async function tracked<T>(set: Set<Work>, work: Promise<T>): Promise<T> {
  set.add(work);
  try {
    return await work;
  } finally {
    set.delete(work);
  }
}

/** HTTP/MCP entry; a fresh scope supersedes inherited ended scopes. */
export async function runPersistenceRequest<T>(body: () => T | Promise<T>): Promise<T> {
  if (parseDatabaseBackend() === "sqlite") return body();
  const state = runtime();
  await ensurePersistenceReady();
  if (state.phase !== "ready") throw new Error("Application persistence is draining");
  return tracked(state.requests, freshScope(state, body));
}

/** Admit at enqueue time. During drain, only live admitted work can create descendants. */
export async function runPersistenceBackgroundWork<T>(body: () => Promise<T>): Promise<T> {
  if (parseDatabaseBackend() === "sqlite") return body();
  const state = runtime();
  const descendant = state.owners.getStore()?.active === true;
  if (
    (state.phase !== "ready" && !(state.phase === "closing" && descendant)) ||
    state.background.size >= BACKGROUND_LIMIT
  ) {
    throw new Error("Persistence background admission is unavailable");
  }
  return tracked(state.background, freshScope(state, body));
}

/** Internal shutdown owners can flush after request admission closes. */
export async function runPersistenceShutdownWork<T>(body: () => Promise<T>): Promise<T> {
  if (parseDatabaseBackend() === "sqlite") return body();
  const state = runtime();
  if (state.phase !== "ready" && state.phase !== "closing") {
    throw new Error("Persistence shutdown authority is unavailable");
  }
  return tracked(state.background, freshScope(state, body));
}

export interface PersistenceTailPermit {
  /** Re-enter the admitted authority for a callback emitted outside its original async context. */
  run<T>(body: () => Promise<T>): Promise<T>;
  /** Call outside run's body, after all completion/failure callbacks have settled. */
  release(): Promise<void>;
}

/** Hold an operation scope, not a SQL connection, across an SSE accounting tail. */
export async function acquirePersistenceTail(): Promise<PersistenceTailPermit> {
  if (parseDatabaseBackend() === "sqlite") {
    return { run: (body) => body(), release: async () => {} };
  }
  const state = runtime();
  const descendant = state.owners.getStore()?.active === true;
  if (
    !state.plane ||
    (state.phase !== "ready" && !(state.phase === "closing" && descendant)) ||
    state.tails.size >= TAIL_LIMIT
  ) {
    throw new PersistenceStreamCapacityError();
  }
  let entered!: (binding: PostgresControlPlaneBinding) => void;
  let failed!: (error: unknown) => void;
  const bindingReady = new Promise<PostgresControlPlaneBinding>((resolve, reject) => {
    entered = resolve;
    failed = reject;
  });
  let stop!: () => void;
  const stopped = new Promise<void>((resolve) => {
    stop = resolve;
  });
  const hold = state.plane.run(async (binding) => {
    entered(binding);
    await stopped;
  });
  state.tails.add(hold);
  void hold.catch(failed);
  void hold.then(
    () => state.tails.delete(hold),
    () => state.tails.delete(hold)
  );
  const binding = await bindingReady;
  const pending = new Set<Work>();
  let released = false;
  let release: Promise<void> | undefined;
  return {
    run<T>(body: () => Promise<T>): Promise<T> {
      if (released) return Promise.reject(new Error("Persistence stream authority has ended"));
      return tracked(pending, catalogScope(state, binding, body));
    },
    release(): Promise<void> {
      release ??= (async () => {
        released = true;
        while (pending.size) await Promise.allSettled([...pending]);
        stop();
        await hold;
      })();
      return release;
    },
  };
}

/** The application store is available only to its held native database authority. */
export function getPersistenceArtifactStore(): NativeArtifactStore {
  const state = globalThis.__omniroutePersistenceRuntime;
  if (!state || state.backend !== "postgres" || !state.applicationIdentity) {
    throw new UnsupportedPersistenceDomainError();
  }
  if (getSelectedPersistenceIdentity() !== state.applicationIdentity) {
    throw new UnsupportedPersistenceDomainError();
  }
  if (!state.artifacts || (state.phase !== "ready" && state.phase !== "closing")) {
    throw new Error("Persistence artifact store is unavailable");
  }
  return state.artifacts;
}

async function drain(state: Runtime): Promise<void> {
  while (state.requests.size || state.background.size || state.tails.size) {
    await Promise.allSettled([...state.requests, ...state.background, ...state.tails]);
  }
}

function assertDrainOutsideOwnedWork(state: Runtime): void {
  if (state.owners.getStore()?.active === true)
    throw new Error("Persistence drain must run outside admitted work");
}

/** Close root admission without allocating a runtime or releasing its application binding. */
export function beginPersistenceAdmissionClose(): void {
  const state = globalThis.__omniroutePersistenceRuntime;
  if (!state || state.backend === "sqlite") return;
  assertDrainOutsideOwnedWork(state);
  if (state.phase === "ready") state.phase = "closing";
}

/** Join dynamically admitted descendants before final buffering sinks are closed. */
export async function drainPersistenceAcceptedWork(): Promise<void> {
  const state = globalThis.__omniroutePersistenceRuntime;
  if (!state || state.backend === "sqlite") return;
  assertDrainOutsideOwnedWork(state);
  await drain(state);
}

/** Flush owners before releasing the held binding and closing the native pool. */
export function closePersistenceRuntime(): Promise<void> {
  const state = globalThis.__omniroutePersistenceRuntime;
  if (!state || state.backend === "sqlite") return Promise.resolve();
  if (state.closing) return state.closing;
  if (state.phase === "closed") return Promise.resolve();
  // Close admission synchronously, including a runtime whose startup has not begun.
  state.phase = "closing";
  state.closing = (async () => {
    await state.startup?.catch(() => {});
    if (state.phase === "closed") return;
    state.phase = "closing";
    const failures: unknown[] = [];
    try {
      await drain(state);
      if (state.plane?.state === "ready") {
        await runPersistenceShutdownWork(async () => {
          const { flushCircuitBreakerPersistence } = await import("@/shared/utils/circuitBreaker");
          const binding = (await import("./controlPlaneContext")).getSelectedControlPlane()!;
          await flushCircuitBreakerPersistence(binding.database!);
        });
      }
      await drain(state);
    } catch (error) {
      failures.push(error);
    } finally {
      // A failed SQL flush must not bypass teardown of the independent object-store client.
      try {
        await state.artifacts?.close();
      } catch (error) {
        failures.push(error);
      }
      state.releaseApplication?.();
      await state.applicationHold?.catch(() => {});
      try {
        await state.plane?.close();
      } catch (error) {
        failures.push(error);
      } finally {
        state.phase = failures.length ? "failed" : "closed";
      }
    }
    if (failures.length) throw new AggregateError(failures, "Persistence cleanup failed");
  })();
  return state.closing;
}

export function getPersistenceRuntimeStatus(): {
  backend: DatabaseBackend;
  phase: Phase;
  activeRequests: number;
  backgroundOperations: number;
  streamTails: number;
} {
  const state = runtime();
  return {
    backend: state.backend,
    phase: state.phase,
    activeRequests: state.requests.size,
    backgroundOperations: state.background.size,
    streamTails: state.tails.size,
  };
}
