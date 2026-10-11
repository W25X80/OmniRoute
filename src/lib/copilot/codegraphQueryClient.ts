import { fork, type ChildProcess } from "node:child_process";
import { access } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { getCodeGraphWorkerContext } from "./codegraphKnowledge";
import type { CodeGraphOperation, CodeGraphWorkerReply } from "./codegraphQueryProtocol";

const CLOSE_TIMEOUT_MS = 2000;
type Pending = { resolve: (output: string) => void; reject: (error: Error) => void };
type ClientState = {
  child: ChildProcess | null;
  starting: Promise<ChildProcess> | null;
  closing: boolean;
  nextId: number;
  pending: Map<number, Pending>;
};
declare global {
  var __omnirouteCodeGraphQueryClient: ClientState | undefined;
}
const state = (globalThis.__omnirouteCodeGraphQueryClient ??= {
  child: null,
  starting: null,
  closing: false,
  nextId: 1,
  pending: new Map(),
});
function unavailable(): Error {
  return new Error("CodeGraph worker is unavailable");
}
function failPending(): void {
  for (const item of state.pending.values()) item.reject(unavailable());
  state.pending.clear();
}

/** The CJS sidecar is explicitly bundled/co-located; source/dev does not need a TS child loader. */
export async function resolveCodeGraphQueryWorker(
  context: {
    moduleDir?: string;
    cwd?: string;
    entryFile?: string | null;
  } = {}
): Promise<{ workerFile: string; execArgv: string[] }> {
  const moduleDir = context.moduleDir ?? dirname(fileURLToPath(import.meta.url));
  const cwd = context.cwd ?? process.cwd();
  const entry = context.entryFile === undefined ? process.argv[1] : context.entryFile;
  const candidates = [
    join(moduleDir, "codegraphQueryWorker.cjs"),
    ...(entry ? [join(dirname(resolve(entry)), "src/lib/copilot/codegraphQueryWorker.cjs")] : []),
    resolve(cwd, "src/lib/copilot/codegraphQueryWorker.cjs"),
  ];
  for (const candidate of candidates) {
    try {
      await access(candidate);
      return { workerFile: candidate, execArgv: [] };
    } catch {
      /* next packaged location */
    }
  }
  for (const candidate of [
    join(moduleDir, "codegraphQueryWorker.ts"),
    resolve(cwd, "src/lib/copilot/codegraphQueryWorker.ts"),
  ]) {
    try {
      await access(candidate);
      return { workerFile: candidate, execArgv: ["--import", "tsx/esm"] };
    } catch {
      /* no source entry */
    }
  }
  throw unavailable();
}

async function getChild(): Promise<ChildProcess> {
  if (state.closing) throw unavailable();
  if (state.child) return state.child;
  if (!state.starting) {
    const starting = (async () => {
      const entry = await resolveCodeGraphQueryWorker();
      if (state.closing) throw unavailable();
      const child = fork(entry.workerFile, [], {
        execArgv: entry.execArgv,
        stdio: ["ignore", "ignore", "ignore", "ipc"],
      });
      state.child = child;
      child.on("message", (reply: CodeGraphWorkerReply) => {
        if (reply?.kind !== "result") return;
        const item = state.pending.get(reply.id);
        if (!item) return;
        state.pending.delete(reply.id);
        if (reply.failed || typeof reply.output !== "string") item.reject(unavailable());
        else item.resolve(reply.output);
      });
      const stop = () => {
        if (state.child === child) {
          state.child = null;
          failPending();
        }
      };
      child.once("error", stop);
      child.once("close", stop);
      return child;
    })();
    state.starting = starting;
    void starting
      .finally(() => {
        if (state.starting === starting) state.starting = null;
      })
      .catch(() => {});
  }
  return state.starting;
}

export async function runCodeGraphToolAsync(
  operation: CodeGraphOperation,
  args: unknown[]
): Promise<string> {
  const context = getCodeGraphWorkerContext();
  const child = await getChild();
  if (state.closing || !child.connected) throw unavailable();
  return new Promise((resolveOutput, reject) => {
    const id = state.nextId++;
    state.pending.set(id, { resolve: resolveOutput, reject });
    const rejectSend = () => {
      const item = state.pending.get(id);
      state.pending.delete(id);
      item?.reject(unavailable());
    };
    try {
      child.send({ kind: "query", id, operation, args, context }, (error) => {
        if (error) rejectSend();
      });
    } catch {
      rejectSend();
    }
  });
}

/** Close admission, request readonly-handle release and await the process exit. */
export async function closeNativeCodeGraphQueries(): Promise<void> {
  state.closing = true;
  if (state.starting) await state.starting.catch(() => {});
  const child = state.child;
  if (!child) {
    failPending();
    return;
  }
  await new Promise<void>((done) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
    }, CLOSE_TIMEOUT_MS);
    child.once("close", () => {
      clearTimeout(timer);
      done();
    });
    if (child.connected)
      child.send({ kind: "close" }, (error) => {
        if (error) child.kill("SIGKILL");
      });
    else child.kill("SIGKILL");
  });
}
