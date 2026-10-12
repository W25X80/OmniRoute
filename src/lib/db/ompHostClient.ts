import { fork, type ChildProcess } from "node:child_process";
import { access } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getSelectedPersistenceIdentity,
  UnsupportedPersistenceDomainError,
} from "./controlPlaneContext";
import {
  OMP_HOST_MAX_JOBS,
  OMP_HOST_MAX_FRAME_BYTES,
  OMP_HOST_MAX_PENDING_BYTES,
  OMP_HOST_OPERATION_TIMEOUT_MS,
  isOmpHostOperation,
  ompHostFrameBytes,
  type OmpHostOperation,
  type OmpHostCredentials,
  type OmpHostRequest,
} from "./ompHostProtocol";

export class OmpHostUnavailableError extends Error {
  constructor() {
    super("OMP host storage is unavailable");
    this.name = "OmpHostUnavailableError";
  }
}
export class OmpHostMutationUnknownError extends Error {
  constructor() {
    super("OMP host storage mutation could not be confirmed");
    this.name = "OmpHostMutationUnknownError";
  }
}
export class OmpHostOperationError extends Error {
  constructor() {
    super("OMP host storage operation failed");
    this.name = "OmpHostOperationError";
  }
}

type Job = {
  id: number;
  operation: OmpHostOperation;
  bytes: number;
  dispatched: boolean;
  timer: ReturnType<typeof setTimeout>;
  cleanupAbort: () => void;
  resolve: (value: OmpHostCredentials | undefined) => void;
  reject: (error: Error) => void;
};
export type OmpHostChildFactory = (
  databasePath: string,
  signal: AbortSignal
) => Promise<ChildProcess>;

/** Same CJS sidecar/async source-resolution pattern as the existing CodeGraph child. */
export async function resolveOmpHostWorker(): Promise<{ workerFile: string; execArgv: string[] }> {
  const moduleDir = path.dirname(fileURLToPath(import.meta.url));
  const entry = process.argv[1];
  for (const workerFile of [
    path.join(moduleDir, "ompHostWorker.cjs"),
    ...(entry
      ? [path.join(path.dirname(path.resolve(entry)), "src/lib/db/ompHostWorker.cjs")]
      : []),
    path.resolve(process.cwd(), "src/lib/db/ompHostWorker.cjs"),
  ]) {
    try {
      await access(workerFile);
      return { workerFile, execArgv: [] };
    } catch {
      /* next packaged location */
    }
  }
  for (const workerFile of [
    path.join(moduleDir, "ompHostWorker.ts"),
    path.resolve(process.cwd(), "src/lib/db/ompHostWorker.ts"),
  ]) {
    try {
      await access(workerFile);
      return { workerFile, execArgv: ["--import", "tsx/esm"] };
    } catch {
      /* no source entry */
    }
  }
  throw new OmpHostUnavailableError();
}
async function spawnOmpHostWorker(
  databasePath: string,
  signal: AbortSignal
): Promise<ChildProcess> {
  const entry = await resolveOmpHostWorker();
  if (signal.aborted) throw new OmpHostUnavailableError();
  return fork(entry.workerFile, [databasePath], {
    execArgv: entry.execArgv,
    stdio: ["ignore", "ignore", "ignore", "ipc"],
  });
}

/** Fixed get/save/delete owner; one in-flight operation, finite admission and no replay. */
export class OmpHostActor {
  private child: ChildProcess | null = null;
  private starting: Promise<void> | null = null;
  private startupAbort: AbortController | null = null;
  private retiring: ChildProcess | null = null;
  private closing = false;
  private terminal = false;
  private nextId = 1;
  private bytes = 0;
  private queue: Job[] = [];
  private active: Job | null = null;
  private closeWork: Promise<void> | null = null;
  private closeDone: (() => void) | null = null;
  private closeTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly databasePath: string;
  private readonly timeoutMs: number;
  private readonly createChild: OmpHostChildFactory;

  constructor(
    databasePath: string,
    options: { createChild?: OmpHostChildFactory; timeoutMs?: number } = {}
  ) {
    const timeout = options.timeoutMs ?? OMP_HOST_OPERATION_TIMEOUT_MS;
    if (
      !path.isAbsolute(databasePath) ||
      Buffer.byteLength(databasePath) > OMP_HOST_MAX_FRAME_BYTES ||
      !Number.isSafeInteger(timeout) ||
      timeout < 1 ||
      timeout > OMP_HOST_OPERATION_TIMEOUT_MS
    )
      throw new OmpHostUnavailableError();
    this.databasePath = databasePath;
    this.timeoutMs = timeout;
    this.createChild = options.createChild ?? spawnOmpHostWorker;
  }
  status() {
    return {
      queued: this.queue.length,
      active: this.active !== null,
      bytes: this.bytes,
      starting: this.starting !== null,
      retiring: this.retiring !== null,
      closing: this.closing,
    };
  }
  beginClose(): void {
    this.closing = true;
    if (!this.queue.length && !this.active) this.startupAbort?.abort();
  }
  execute(
    operation: OmpHostOperation,
    signal?: AbortSignal
  ): Promise<OmpHostCredentials | undefined> {
    if (
      this.closing ||
      this.retiring ||
      this.startupAbort?.signal.aborted ||
      signal?.aborted ||
      !isOmpHostOperation(operation) ||
      !Number.isSafeInteger(this.nextId)
    )
      return Promise.reject(new OmpHostUnavailableError());
    const id = this.nextId;
    const bytes = ompHostFrameBytes({ kind: "operation", id, operation });
    if (
      bytes > OMP_HOST_MAX_FRAME_BYTES ||
      this.queue.length + Number(this.active !== null) >= OMP_HOST_MAX_JOBS ||
      this.bytes + bytes > OMP_HOST_MAX_PENDING_BYTES
    )
      return Promise.reject(new OmpHostUnavailableError());
    this.nextId++;
    return new Promise((resolve, reject) => {
      const job: Job = {
        id,
        operation: { ...operation },
        bytes,
        dispatched: false,
        timer: setTimeout(() => this.expire(job), this.timeoutMs),
        cleanupAbort: () => {},
        resolve,
        reject,
      };
      if (signal) {
        const abort = () => this.expire(job);
        signal.addEventListener("abort", abort, { once: true });
        job.cleanupAbort = () => signal.removeEventListener("abort", abort);
      }
      this.bytes += bytes;
      this.queue.push(job);
      this.pump();
    });
  }
  private settle(job: Job, error?: Error, result?: OmpHostCredentials): void {
    if (this.active === job) this.active = null;
    else {
      const index = this.queue.indexOf(job);
      if (index < 0) return;
      this.queue.splice(index, 1);
    }
    clearTimeout(job.timer);
    job.cleanupAbort();
    this.bytes -= job.bytes;
    if (!this.queue.length && !this.active) this.startupAbort?.abort();
    if (error) job.reject(error);
    else job.resolve(result);
    this.pump();
    this.finishClose();
  }
  private unknown(job: Job): Error {
    return job.dispatched && job.operation.kind !== "get"
      ? new OmpHostMutationUnknownError()
      : new OmpHostUnavailableError();
  }
  private retire(): void {
    const child = this.child;
    if (!child) return;
    this.child = null;
    this.retiring = child;
    try {
      child.kill("SIGKILL");
    } catch {
      /* Actual exit remains the only retirement acknowledgement. */
    }
  }
  private expire(job: Job): void {
    if (this.active !== job && !this.queue.includes(job)) return;
    if (job.dispatched) this.retire();
    this.settle(job, this.unknown(job));
  }
  private attach(child: ChildProcess): void {
    this.child = child;
    child.on("message", (message: unknown) => {
      const job = this.active;
      if (
        !job ||
        !job.dispatched ||
        this.child !== child ||
        !message ||
        typeof message !== "object"
      )
        return;
      const reply = message as Record<string, unknown>;
      if (reply.kind !== "result" || reply.id !== job.id) return;
      try {
        if (ompHostFrameBytes(reply) > OMP_HOST_MAX_FRAME_BYTES)
          throw new OmpHostUnavailableError();
        if (reply.ok !== true) {
          this.settle(job, new OmpHostOperationError());
          return;
        }
        if (job.operation.kind === "get") {
          const value = reply.credentials;
          if (
            !value ||
            typeof value !== "object" ||
            typeof (value as OmpHostCredentials).hasOmniRoute !== "boolean" ||
            !Object.hasOwn(value, "apiKey") ||
            !Object.hasOwn(value, "baseUrl")
          )
            throw new OmpHostUnavailableError();
          this.settle(job, undefined, value as OmpHostCredentials);
        } else this.settle(job);
      } catch {
        this.retire();
        this.settle(job, this.unknown(job));
      }
    });
    child.once("error", () => {
      if (this.child !== child) return;
      const job = this.active;
      this.retire();
      if (job) this.settle(job, this.unknown(job));
    });
    child.once("close", () => {
      if (this.child === child) {
        this.child = null;
        const job = this.active;
        if (job) this.settle(job, this.unknown(job));
      }
      if (this.retiring === child) this.retiring = null;
      this.pump();
      this.finishClose();
    });
  }
  private pump(): void {
    if (this.terminal || this.active || this.starting || this.retiring || !this.queue.length)
      return;
    if (!this.child) {
      const startupAbort = new AbortController();
      this.startupAbort = startupAbort;
      const work = Promise.resolve()
        .then(() => this.createChild(this.databasePath, startupAbort.signal))
        .then((child) => {
          this.attach(child);
          if (startupAbort.signal.aborted) this.retire();
        })
        .catch(() => {
          // Startup was never dispatched. Reject these accepted operations without a retry.
          for (const job of [...this.queue]) this.settle(job, new OmpHostUnavailableError());
        })
        .finally(() => {
          this.starting = null;
          this.startupAbort = null;
          this.pump();
          this.finishClose();
        });
      this.starting = work;
      return;
    }
    const job = this.queue.shift()!;
    this.active = job;
    job.dispatched = true;
    try {
      this.child.send(
        { kind: "operation", id: job.id, operation: job.operation } satisfies OmpHostRequest,
        (error) => {
          if (!error || this.active !== job) return;
          this.retire();
          this.settle(job, this.unknown(job));
        }
      );
    } catch {
      this.retire();
      this.settle(job, this.unknown(job));
    }
  }
  private finishClose(): void {
    if (!this.closeDone || this.active || this.queue.length || this.starting || this.retiring)
      return;
    if (this.child) {
      const child = this.child;
      this.child = null;
      this.retiring = child;
      try {
        child.send({ kind: "close" } satisfies OmpHostRequest, (error) => {
          if (error) {
            try {
              child.kill("SIGKILL");
            } catch {
              /* Await verified exit. */
            }
          }
        });
      } catch {
        try {
          child.kill("SIGKILL");
        } catch {
          /* Await verified exit. */
        }
      }
      return;
    }
    const done = this.closeDone;
    this.closeDone = null;
    this.terminal = true;
    if (this.closeTimer) clearTimeout(this.closeTimer);
    this.closeTimer = null;
    done();
  }
  close(): Promise<void> {
    this.beginClose();
    this.closeWork ??= new Promise<void>((resolve, reject) => {
      this.closeDone = resolve;
      this.closeTimer = setTimeout(
        () => {
          this.closeDone = null;
          this.terminal = true;
          this.startupAbort?.abort();
          if (this.child) this.retire();
          if (this.active) this.settle(this.active, this.unknown(this.active));
          for (const job of [...this.queue]) this.settle(job, new OmpHostUnavailableError());
          if (this.retiring) {
            try {
              this.retiring.kill("SIGKILL");
            } catch {
              /* No termination claim. */
            }
          }
          reject(new OmpHostUnavailableError());
        },
        Math.min(2000, this.timeoutMs)
      );
      this.finishClose();
    });
    return this.closeWork;
  }
}

type HostOwner = { identity: object; closing: boolean; actor?: OmpHostActor };
const OWNER_SYMBOL = Symbol.for("omniroute.nativeOmpHostActor");
const owners = globalThis as typeof globalThis & { [OWNER_SYMBOL]?: HostOwner };
function currentIdentity(): object {
  const identity = getSelectedPersistenceIdentity();
  if (!identity || identity !== globalThis.__omniroutePersistenceRuntime?.applicationIdentity)
    throw new UnsupportedPersistenceDomainError();
  return identity;
}
export function getNativeOmpHostActor(): OmpHostActor {
  const identity = currentIdentity();
  const phase = globalThis.__omniroutePersistenceRuntime?.phase;
  if (phase !== "ready" && phase !== "closing") throw new OmpHostUnavailableError();
  const existing = owners[OWNER_SYMBOL];
  if (existing) {
    if (existing.identity !== identity) throw new UnsupportedPersistenceDomainError();
    if (existing.closing) throw new OmpHostUnavailableError();
    return existing.actor!;
  }
  const actor = new OmpHostActor(path.join(os.homedir(), ".omp", "agent", "agent.db"));
  owners[OWNER_SYMBOL] = { identity, closing: false, actor };
  return actor;
}
/** This request capability retires only after the accepted application work has drained. */
export function beginNativeOmpHostClose(): void {
  const state = globalThis.__omniroutePersistenceRuntime;
  if (!state || state.backend !== "postgres" || !state.applicationIdentity) return;
  const identity = currentIdentity();
  const existing = owners[OWNER_SYMBOL];
  if (existing && existing.identity !== identity) throw new UnsupportedPersistenceDomainError();
  const owner: HostOwner = existing ?? (owners[OWNER_SYMBOL] = { identity, closing: true });
  owner.closing = true;
  owner.actor?.beginClose();
}
export async function closeNativeOmpHostStorage(): Promise<void> {
  beginNativeOmpHostClose();
  const owner = owners[OWNER_SYMBOL];
  if (!owner) return;
  const state = globalThis.__omniroutePersistenceRuntime;
  if (!state || state.backend !== "postgres" || !state.applicationIdentity) return;
  if (owner.identity !== currentIdentity()) throw new UnsupportedPersistenceDomainError();
  await owner.actor?.close();
  // Keep the closing latch even if no child ever existed. The same owner cannot reopen it.
}
