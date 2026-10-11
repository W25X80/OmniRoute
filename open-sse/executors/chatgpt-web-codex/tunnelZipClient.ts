import { stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";

const JOBS = Symbol.for("omniroute.codex.tunnel-zip-jobs.v1");
type ArchiveJobs = { active: Set<Promise<Uint8Array>>; closing: boolean };
const holders = globalThis as typeof globalThis & { [JOBS]?: ArchiveJobs };
function jobs(): ArchiveJobs {
  return (holders[JOBS] ??= { active: new Set(), closing: false });
}

export async function resolveTunnelZipWorker(
  context: { moduleDir?: string; cwd?: string; entryFile?: string } = {}
) {
  const moduleDir = context.moduleDir ?? dirname(fileURLToPath(import.meta.url));
  const relative = "open-sse/executors/chatgpt-web-codex/tunnelZipWorker.js";
  const entry = context.entryFile ?? process.argv[1];
  const candidates = [
    { workerFile: join(moduleDir, "tunnelZipWorker.js"), execArgv: [] },
    ...(entry ? [{ workerFile: join(dirname(resolve(entry)), relative), execArgv: [] }] : []),
    {
      workerFile: resolve(context.cwd ?? process.cwd(), relative),
      execArgv: [],
    },
    {
      workerFile: join(moduleDir, "tunnelZipWorker.ts"),
      execArgv: ["--import", "tsx/esm"],
    },
    {
      workerFile: resolve(
        context.cwd ?? process.cwd(),
        "open-sse/executors/chatgpt-web-codex/tunnelZipWorker.ts"
      ),
      execArgv: ["--import", "tsx/esm"],
    },
  ];
  for (const candidate of candidates) {
    try {
      if ((await Reflect.apply(stat, undefined, [candidate.workerFile]))?.isFile())
        return candidate;
    } catch {
      /* Try the next packaged location. */
    }
  }
  throw new Error("Tunnel archive worker is unavailable");
}

/** Takes ownership of archive bytes after their checksum has been verified by the installer. */
export function extractTunnelBinaryAsync(
  archive: Uint8Array,
  executableName: string
): Promise<Uint8Array> {
  const state = jobs();
  if (state.closing || state.active.size >= 1)
    return Promise.reject(new Error("Tunnel archive worker is unavailable"));
  const work = (async () => {
    const { workerFile, execArgv } = await resolveTunnelZipWorker();
    const worker = Reflect.construct(Worker, [pathToFileURL(workerFile), { execArgv }]) as Worker;
    try {
      return await new Promise<Uint8Array>((resolveBinary, rejectBinary) => {
        let settled = false;
        const finish = (binary?: Uint8Array, missing = false) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          if (binary) resolveBinary(binary);
          else
            rejectBinary(
              new Error(
                missing
                  ? "Tunnel archive does not contain the executable"
                  : "Tunnel archive extraction failed"
              )
            );
        };
        const timer = setTimeout(() => finish(), 15_000);
        worker.once("message", (reply: { kind?: string; binary?: unknown }) => {
          finish(
            reply?.kind === "binary" && reply.binary instanceof Uint8Array
              ? reply.binary
              : undefined,
            reply?.kind === "missing"
          );
        });
        worker.once("error", () => finish());
        worker.once("exit", () => finish());
        const transferable: Uint8Array<ArrayBuffer> =
          archive.buffer instanceof ArrayBuffer &&
          archive.byteOffset === 0 &&
          archive.byteLength === archive.buffer.byteLength
            ? new Uint8Array(archive.buffer)
            : new Uint8Array(archive);
        try {
          worker.postMessage({ archive: transferable, executableName }, [transferable.buffer]);
        } catch {
          finish();
        }
      });
    } finally {
      await worker.terminate();
    }
  })();
  state.active.add(work);
  void work.then(
    () => state.active.delete(work),
    () => state.active.delete(work)
  );
  return work;
}

export async function closeTunnelZipWorkers(): Promise<void> {
  const state = jobs();
  state.closing = true;
  const settled = await Promise.allSettled([...state.active]);
  if (holders[JOBS] === state) delete holders[JOBS];
  if (settled.some((result) => result.status === "rejected"))
    throw new Error("Tunnel archive work did not finish cleanly");
}
