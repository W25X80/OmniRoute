import fs from "node:fs";
import fsPromises from "node:fs/promises";
import { parseDatabaseBackend } from "../db/backendSelection";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";

import type { ArtifactPutOptions } from "@/domain/persistence/artifactStore";
import { getSelectedControlPlane } from "../db/controlPlaneContext";
import { getPersistenceArtifactStore } from "../db/persistenceRuntime";
import type { CallLogArtifact, CallLogArtifactWriteResult } from "./callLogArtifacts.ts";
import { MAX_CALL_LOG_DECODE_BYTES } from "./callLogArtifactProtocol.ts";
import type {
  CallLogArtifactDecodeResult,
  CallLogArtifactEnvironment,
  CallLogArtifactWorkerReply,
} from "./callLogArtifactProtocol.ts";

const MAX_QUEUED_JOBS = 128;
const IDLE_TIMEOUT_MS = 30_000;
const CLOSE_TIMEOUT_MS = 2_000;
const WARNING_INTERVAL_MS = 30_000;

type QueueBase = {
  id: number;
  artifact: CallLogArtifact;
  environment: CallLogArtifactEnvironment;
};
type QueueItem =
  | (QueueBase &
      (
        | { kind: "write"; resolve: (result: CallLogArtifactWriteResult | null) => void }
        | {
            kind: "serialize";
            resolve: (bytes: Uint8Array) => void;
            reject: (error: Error) => void;
          }
      ))
  | {
      id: number;
      kind: "deserialize";
      bytes: Uint8Array;
      resolve: (result: CallLogArtifactDecodeResult) => void;
      reject: (error: Error) => void;
    };

export class CallLogArtifactSerializationError extends Error {
  constructor() {
    super("Call-log artifact serializer is unavailable");
    this.name = "CallLogArtifactSerializationError";
  }
}

export class CallLogArtifactDeserializationError extends Error {
  constructor() {
    super("Call-log artifact decoder is unavailable");
    this.name = "CallLogArtifactDeserializationError";
  }
}

let worker: Worker | null = null;
let pendingSpawn: Promise<Worker> | null = null;
let workerGeneration = 0;
let active: QueueItem | null = null;
const queue: QueueItem[] = [];
const nativeWrites = new Set<Promise<CallLogArtifactWriteResult>>();
let nextId = 1;
let idleTimer: NodeJS.Timeout | null = null;
let closing = false;
let closeWaiters: Array<() => void> = [];
let lastWarningAt = 0;

function fileExistsAtRuntime(candidate: string): boolean {
  // The worker is copied explicitly by the standalone/npm packaging policies.
  // Keep this runtime probe opaque to Next's build tracer so it does not expand
  // a variable filesystem expression into a repository-wide glob.
  return Reflect.apply(fs.existsSync, fs, [candidate]) as boolean;
}

type WorkerResolutionContext = {
  moduleDir?: string;
  cwd?: string;
  entryFile?: string | null;
  fileExists?: (candidate: string) => boolean;
};

export function resolveCallLogArtifactWorker(context: WorkerResolutionContext = {}): {
  workerFile: string;
  execArgv: string[];
} {
  const moduleDir = context.moduleDir ?? path.dirname(fileURLToPath(import.meta.url));
  const cwd = context.cwd ?? process.cwd();
  const entryFile = context.entryFile === undefined ? process.argv[1] : context.entryFile;
  const exists = context.fileExists ?? fileExistsAtRuntime;

  const moduleJs = path.join(moduleDir, "callLogArtifactWorker.js");
  if (exists(moduleJs)) return { workerFile: moduleJs, execArgv: [] };

  const entryJs = entryFile
    ? path.join(path.dirname(path.resolve(entryFile)), "src/lib/usage/callLogArtifactWorker.js")
    : null;
  if (entryJs && exists(entryJs)) return { workerFile: entryJs, execArgv: [] };

  const cwdJs = path.resolve(cwd, "src/lib/usage/callLogArtifactWorker.js");
  if (exists(cwdJs)) return { workerFile: cwdJs, execArgv: [] };

  const moduleTs = path.join(moduleDir, "callLogArtifactWorker.ts");
  if (exists(moduleTs)) {
    return { workerFile: moduleTs, execArgv: ["--import", "tsx/esm"] };
  }

  const cwdTs = path.resolve(cwd, "src/lib/usage/callLogArtifactWorker.ts");
  if (exists(cwdTs)) {
    return { workerFile: cwdTs, execArgv: ["--import", "tsx/esm"] };
  }

  return { workerFile: entryJs ?? cwdJs, execArgv: [] };
}

async function fileExistsAtRuntimeAsync(candidate: string): Promise<boolean> {
  try {
    // Preserve the build-tracer boundary of the existing opaque runtime probe.
    await Reflect.apply(fsPromises.access, fsPromises, [candidate]);
    return true;
  } catch {
    return false;
  }
}

export async function resolveCallLogArtifactWorkerAsync(
  context: Omit<WorkerResolutionContext, "fileExists"> & {
    fileExists?: (candidate: string) => Promise<boolean>;
  } = {}
): Promise<{ workerFile: string; execArgv: string[] }> {
  const moduleDir = context.moduleDir ?? path.dirname(fileURLToPath(import.meta.url));
  const cwd = context.cwd ?? process.cwd();
  const entryFile = context.entryFile === undefined ? process.argv[1] : context.entryFile;
  const exists = context.fileExists ?? fileExistsAtRuntimeAsync;
  const moduleJs = path.join(moduleDir, "callLogArtifactWorker.js");
  if (await exists(moduleJs)) return { workerFile: moduleJs, execArgv: [] };
  const entryJs = entryFile
    ? path.join(path.dirname(path.resolve(entryFile)), "src/lib/usage/callLogArtifactWorker.js")
    : null;
  if (entryJs && (await exists(entryJs))) return { workerFile: entryJs, execArgv: [] };
  const cwdJs = path.resolve(cwd, "src/lib/usage/callLogArtifactWorker.js");
  if (await exists(cwdJs)) return { workerFile: cwdJs, execArgv: [] };
  const moduleTs = path.join(moduleDir, "callLogArtifactWorker.ts");
  if (await exists(moduleTs)) return { workerFile: moduleTs, execArgv: ["--import", "tsx/esm"] };
  const cwdTs = path.resolve(cwd, "src/lib/usage/callLogArtifactWorker.ts");
  if (await exists(cwdTs)) return { workerFile: cwdTs, execArgv: ["--import", "tsx/esm"] };
  return { workerFile: entryJs ?? cwdJs, execArgv: [] };
}

function clearIdleTimer(): void {
  if (!idleTimer) return;
  clearTimeout(idleTimer);
  idleTimer = null;
}

function terminateWorker(): void {
  workerGeneration++;
  const current = worker;
  worker = null;
  clearIdleTimer();
  if (current) void current.terminate().catch(() => {});
}

function notifyCloseWaiters(): void {
  if (active || queue.length > 0 || nativeWrites.size > 0 || pendingSpawn) return;
  const waiters = closeWaiters;
  closeWaiters = [];
  for (const resolve of waiters) resolve();
}

function scheduleIdleTermination(): void {
  clearIdleTimer();
  if (!worker || active || queue.length > 0) return;
  idleTimer = setTimeout(terminateWorker, IDLE_TIMEOUT_MS);
  idleTimer.unref?.();
}

function warnRateLimited(message: string): void {
  const now = Date.now();
  if (now - lastWarningAt < WARNING_INTERVAL_MS) return;
  lastWarningAt = now;
  console.warn(message);
}

function describeFailureDetail(detail: unknown): string {
  if (detail instanceof Error) return `${detail.name}: ${detail.message}`;
  return String(detail ?? "unknown error");
}

function failOpen(warn = false, detail?: unknown): void {
  const failed = active ? [active, ...queue] : [...queue];
  if (warn) {
    warnRateLimited(
      failed.some((item) => item.kind === "deserialize")
        ? "[callLogs] Call-log artifact decoder worker failed; detail unavailable."
        : failed.some((item) => item.kind === "serialize")
          ? "[callLogs] Call-log artifact serializer worker failed; detail unavailable."
          : `[callLogs] Call-log artifact worker failed: ${describeFailureDetail(detail)}`
    );
  }
  active = null;
  queue.length = 0;
  terminateWorker();
  for (const item of failed) {
    if (item.kind === "deserialize") item.reject(new CallLogArtifactDeserializationError());
    else if (item.kind === "serialize") item.reject(new CallLogArtifactSerializationError());
    else item.resolve(null);
  }
  notifyCloseWaiters();
}

let workerFileOverride: { workerFile: string; execArgv: string[] } | null = null;

/**
 * Test-only hook: force the next ensureWorker() call to spawn an arbitrary worker
 * script/execArgv instead of the real callLogArtifactWorker file. Lets regression tests
 * trigger a genuine worker_threads `error`/`exit` event without editing the production
 * worker script. Never called from production code paths.
 */
export function __setCallLogWorkerOverrideForTests(
  override: { workerFile: string; execArgv: string[] } | null
): void {
  workerFileOverride = override;
}

function attachWorker(created: Worker, native = false): Worker {
  worker = created;
  created.on("message", (reply: CallLogArtifactWorkerReply) => {
    if (!active || reply.id !== active.id) return;
    const completed = active;
    active = null;
    if (completed.kind === "deserialize") {
      if (reply.kind === "deserialize" && reply.result) completed.resolve(reply.result);
      else completed.reject(new CallLogArtifactDeserializationError());
    } else if (completed.kind === "serialize") {
      if (reply.kind === "serialize" && reply.bytes instanceof Uint8Array) {
        completed.resolve(reply.bytes);
      } else completed.reject(new CallLogArtifactSerializationError());
    } else {
      completed.resolve(reply.kind === "write" ? reply.result : null);
    }
    pump();
  });
  created.on("error", (err) => {
    if (!native || worker === created) failOpen(true, err);
  });
  created.on("messageerror", (err) => {
    if (!native || worker === created) failOpen(true, err);
  });
  created.on("exit", (code) => {
    if (worker !== created) return;
    worker = null;
    if (code !== 0 || active) failOpen(true, new Error(`worker exited with code ${code}`));
  });
  return created;
}

function ensureWorker(): Worker | Promise<Worker> {
  if (worker) return worker;
  if (parseDatabaseBackend() !== "postgres") {
    const { workerFile, execArgv } = workerFileOverride ?? resolveCallLogArtifactWorker();
    const created = Reflect.construct(Worker, [pathToFileURL(workerFile), { execArgv }]) as Worker;
    return attachWorker(created);
  }
  if (pendingSpawn) return pendingSpawn;
  const generation = workerGeneration;
  const override = workerFileOverride;
  const preparation = Promise.resolve().then(async () => {
    const { workerFile, execArgv } = override ?? (await resolveCallLogArtifactWorkerAsync());
    if (generation !== workerGeneration) throw new CallLogArtifactSerializationError();
    const created = Reflect.construct(Worker, [pathToFileURL(workerFile), { execArgv }]) as Worker;
    return attachWorker(created, true);
  });
  const pending = preparation.finally(() => {
    if (pendingSpawn === pending) pendingSpawn = null;
    notifyCloseWaiters();
  });
  pendingSpawn = pending;
  return pending;
}

function pump(): void {
  if (active) return;
  const next = queue.shift();
  if (!next) {
    notifyCloseWaiters();
    scheduleIdleTermination();
    return;
  }

  active = next;
  clearIdleTimer();
  const send = (created: Worker) => {
    if (active !== next) return;
    created.postMessage(
      next.kind === "deserialize"
        ? { id: next.id, kind: next.kind, bytes: next.bytes }
        : { id: next.id, kind: next.kind, artifact: next.artifact, environment: next.environment }
    );
  };
  try {
    const ready = ensureWorker();
    if (ready instanceof Promise) {
      void ready.then(send).catch((error: unknown) => {
        if (active === next) failOpen(true, error);
      });
    } else send(ready);
  } catch (err) {
    failOpen(true, err);
  }
}

function environmentSnapshot(): CallLogArtifactEnvironment {
  return {
    pipelineMaxSizeKb: process.env.CALL_LOG_PIPELINE_MAX_SIZE_KB,
    chatDebugFile: process.env.CHAT_DEBUG_FILE,
    appLogLevel: process.env.APP_LOG_LEVEL,
  };
}

function writeNativeArtifact(
  artifact: CallLogArtifact,
  options?: ArtifactPutOptions
): Promise<CallLogArtifactWriteResult> {
  if (closing || queue.length >= MAX_QUEUED_JOBS || nativeWrites.size >= MAX_QUEUED_JOBS) {
    return Promise.reject(new CallLogArtifactSerializationError());
  }
  // Verify native authority before admitting CPU work; never select filesystem storage on error.
  getPersistenceArtifactStore();
  if (!options?.onPrepared) {
    return Promise.reject(new Error("Native call-log artifact preparation is required"));
  }
  const serialized = new Promise<Uint8Array>((resolve, reject) => {
    queue.push({
      id: nextId++,
      kind: "serialize",
      artifact,
      environment: environmentSnapshot(),
      resolve,
      reject,
    });
    pump();
  });
  const operation = serialized.then(async (value) => {
    const { writeCallArtifactToNativeStore } = await import("./callLogArtifacts.ts");
    return writeCallArtifactToNativeStore(value, options);
  });
  nativeWrites.add(operation);
  void operation
    .then(
      () => nativeWrites.delete(operation),
      () => nativeWrites.delete(operation)
    )
    .then(notifyCloseWaiters);
  return operation;
}

/** Same bounded CPU actor as writes; failed workers never parse on the serving thread. */
export function deserializeCallArtifactAsync(
  bytes: Uint8Array
): Promise<CallLogArtifactDecodeResult> {
  try {
    getPersistenceArtifactStore();
  } catch (error) {
    return Promise.reject(error);
  }
  if (
    closing ||
    queue.length >= MAX_QUEUED_JOBS ||
    !(bytes instanceof Uint8Array) ||
    bytes.byteLength > MAX_CALL_LOG_DECODE_BYTES
  ) {
    return Promise.reject(new CallLogArtifactDeserializationError());
  }
  return new Promise((resolve, reject) => {
    queue.push({ id: nextId++, kind: "deserialize", bytes, resolve, reject });
    pump();
  });
}

export function writeCallArtifactAsync(
  artifact: CallLogArtifact,
  options?: ArtifactPutOptions
): Promise<CallLogArtifactWriteResult | null> {
  try {
    if (getSelectedControlPlane()) return writeNativeArtifact(artifact, options);
  } catch (error) {
    return Promise.reject(error);
  }
  if (closing || queue.length >= MAX_QUEUED_JOBS) {
    warnRateLimited("[callLogs] Call-log artifact queue unavailable; detail omitted.");
    return Promise.resolve(null);
  }
  return new Promise((resolve) => {
    queue.push({
      id: nextId++,
      kind: "write",
      artifact,
      environment: environmentSnapshot(),
      resolve,
    });
    pump();
  });
}

export async function closeCallLogArtifactWriter(timeoutMs = CLOSE_TIMEOUT_MS): Promise<void> {
  closing = true;
  if (!active && queue.length === 0 && nativeWrites.size === 0) {
    terminateWorker();
    return;
  }

  if (timeoutMs <= 0) {
    failOpen();
    terminateWorker();
    if (nativeWrites.size > 0) throw new Error("Native call-log writes are still draining");
    return;
  }

  let timeout: NodeJS.Timeout | undefined;
  await Promise.race([
    new Promise<void>((resolve) => closeWaiters.push(resolve)),
    new Promise<void>((resolve) => {
      timeout = setTimeout(resolve, timeoutMs);
      timeout.unref?.();
    }),
  ]);
  if (timeout) clearTimeout(timeout);
  if (active || queue.length > 0) failOpen();
  terminateWorker();
  if (nativeWrites.size > 0) throw new Error("Native call-log writes are still draining");
}
