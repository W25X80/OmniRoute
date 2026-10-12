import { stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import { getConfigDir } from "../config";
import type {
  ResponseStateWorkerOperation,
  ResponseStateWorkerReply,
  ResponseStateWorkerRequest,
} from "./stateWorkerProtocol";

const MAX_ACCEPTED_JOBS = 8;
const CLOSE_TIMEOUT_MS = 5_000;
const OWNER = Symbol.for("omniroute.codex.response-state-worker.v1");

type Pending = {
  resolve: (reply: ResponseStateWorkerReply) => void;
  reject: (error: Error) => void;
};
type CacheOwner = {
  home: string;
  worker?: Worker;
  starting?: Promise<Worker>;
  pending: Map<number, Pending>;
  nextId: number;
  closing?: Promise<void>;
  stopped?: boolean;
};
const holders = globalThis as typeof globalThis & { [OWNER]?: CacheOwner };

export class ResponseStateWorkerUnavailableError extends Error {
  constructor() {
    super("Continuation cache worker is unavailable");
    this.name = "ResponseStateWorkerUnavailableError";
  }
}

async function isFile(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}

export async function resolveResponseStateWorker(
  context: {
    moduleDir?: string;
    cwd?: string;
    entryFile?: string;
  } = {}
): Promise<{ workerFile: string; execArgv: string[] }> {
  const moduleDir = context.moduleDir ?? dirname(fileURLToPath(import.meta.url));
  const cwd = context.cwd ?? process.cwd();
  const entry = context.entryFile ?? process.argv[1];
  const relative = "open-sse/vendor/codex-chatgpt-web/responses/stateWorker.js";
  const candidates = [
    { workerFile: join(moduleDir, "stateWorker.js"), execArgv: [] },
    ...(entry ? [{ workerFile: join(dirname(resolve(entry)), relative), execArgv: [] }] : []),
    { workerFile: resolve(cwd, relative), execArgv: [] },
    {
      workerFile: join(moduleDir, "stateWorker.ts"),
      execArgv: ["--import", "tsx/esm"],
    },
    {
      workerFile: resolve(cwd, "open-sse/vendor/codex-chatgpt-web/responses/stateWorker.ts"),
      execArgv: ["--import", "tsx/esm"],
    },
  ];
  for (const candidate of candidates) if (await isFile(candidate.workerFile)) return candidate;
  throw new ResponseStateWorkerUnavailableError();
}

function currentOwner(): CacheOwner {
  const home = getConfigDir();
  const current = holders[OWNER];
  if (current) {
    if (current.home !== home || current.closing || current.stopped)
      throw new ResponseStateWorkerUnavailableError();
    return current;
  }
  return (holders[OWNER] = { home, pending: new Map(), nextId: 1 });
}

function fail(owner: CacheOwner, worker?: Worker): void {
  if (worker && owner.worker !== worker) return;
  owner.worker = undefined;
  for (const pending of owner.pending.values()) {
    pending.reject(new ResponseStateWorkerUnavailableError());
  }
  owner.pending.clear();
  if (worker) void worker.terminate().catch(() => {});
}

async function start(owner: CacheOwner): Promise<Worker> {
  if (owner.worker) return owner.worker;
  if (owner.starting) return owner.starting;
  const starting = (async () => {
    const { workerFile, execArgv } = await resolveResponseStateWorker();
    if (owner.stopped) throw new ResponseStateWorkerUnavailableError();
    const worker = Reflect.construct(Worker, [
      pathToFileURL(workerFile),
      { execArgv, workerData: { home: owner.home } },
    ]) as Worker;
    owner.worker = worker;
    worker.on("message", (reply: ResponseStateWorkerReply) => {
      const pending = owner.pending.get(reply?.id);
      if (!pending) return;
      owner.pending.delete(reply.id);
      if (reply.kind === "error") pending.reject(new ResponseStateWorkerUnavailableError());
      else pending.resolve(reply);
      if (!owner.pending.size) worker.unref();
    });
    worker.on("error", () => fail(owner, worker));
    worker.on("exit", () => fail(owner, worker));
    if (!owner.pending.size) worker.unref();
    return worker;
  })();
  owner.starting = starting;
  try {
    return await starting;
  } finally {
    if (owner.starting === starting) owner.starting = undefined;
  }
}

function enqueue(
  owner: CacheOwner,
  operation: ResponseStateWorkerOperation
): Promise<ResponseStateWorkerReply> {
  if (owner.stopped || owner.pending.size >= MAX_ACCEPTED_JOBS) {
    return Promise.reject(new ResponseStateWorkerUnavailableError());
  }
  const id = owner.nextId++;
  return new Promise((resolveJob, rejectJob) => {
    owner.pending.set(id, { resolve: resolveJob, reject: rejectJob });
    void start(owner).then(
      (worker) => {
        if (!owner.pending.has(id)) return;
        worker.ref();
        const request: ResponseStateWorkerRequest = { ...operation, id };
        try {
          worker.postMessage(request);
        } catch {
          owner.pending.delete(id);
          rejectJob(new ResponseStateWorkerUnavailableError());
          if (!owner.pending.size) worker.unref();
        }
      },
      () => fail(owner)
    );
  });
}

export function runResponseStateOperation(
  operation: ResponseStateWorkerOperation
): Promise<ResponseStateWorkerReply> {
  try {
    return enqueue(currentOwner(), operation);
  } catch {
    return Promise.reject(new ResponseStateWorkerUnavailableError());
  }
}

export function closeResponseStateWorker(): Promise<void> {
  const owner: CacheOwner =
    holders[OWNER] ??
    (holders[OWNER] = {
      home: getConfigDir(),
      pending: new Map(),
      nextId: 1,
      stopped: true,
    });
  if (owner.stopped && !owner.closing) return Promise.resolve();
  if (owner.closing) return owner.closing;
  // Capture every accepted reply before closing admission. All worker operations are FIFO.
  const settled = [...owner.pending.values()].map((pending) => {
    return new Promise<void>((resolveJob) => {
      const originalResolve = pending.resolve;
      const originalReject = pending.reject;
      pending.resolve = (reply) => {
        originalResolve(reply);
        resolveJob();
      };
      pending.reject = (error) => {
        originalReject(error);
        resolveJob();
      };
    });
  });
  owner.closing = (async () => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        (async () => {
          await Promise.all(settled);
          if (owner.worker) await enqueue(owner, { kind: "flush" });
        })(),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(
            () => reject(new ResponseStateWorkerUnavailableError()),
            CLOSE_TIMEOUT_MS
          );
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
      owner.stopped = true;
      const worker = owner.worker;
      fail(owner, worker);
      if (worker) await worker.terminate();
      // Keep the closed owner as an admission tombstone for late stream callbacks.
    }
  })();
  return owner.closing;
}
