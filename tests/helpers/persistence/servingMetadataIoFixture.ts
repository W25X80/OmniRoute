export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

export const metadataIo = {
  syncCalls: 0,
  asyncReads: 0,
  asyncWrites: 0,
  readPaths: [] as string[],
  writePaths: [] as string[],
  partialWrite: undefined as string | undefined,
  shims: new Map<string, string>(),
  directories: new Map<string, string[]>(),
  stats: new Map<string, { directory: boolean; mtimeMs: number }>(),
  files: new Map<string, string>(),
  gate: undefined as ReturnType<typeof barrier> | undefined,
  entered: undefined as ReturnType<typeof barrier> | undefined,
  written: undefined as ReturnType<typeof barrier> | undefined,
  failRead: false,
  failWrite: false,
  agentFailure: false,
  writeGate: undefined as ReturnType<typeof barrier> | undefined,
  writeEntered: undefined as ReturnType<typeof barrier> | undefined,
  reset() {
    this.gate?.resolve();
    this.writeGate?.resolve();
    this.syncCalls = 0;
    this.asyncReads = 0;
    this.asyncWrites = 0;
    this.readPaths = [];
    this.writePaths = [];
    this.partialWrite = undefined;
    this.shims.clear();
    this.directories.clear();
    this.stats.clear();
    this.files.clear();
    this.gate = undefined;
    this.entered = undefined;
    this.written = undefined;
    this.failRead = false;
    this.failWrite = false;
    this.agentFailure = false;
    this.writeGate = undefined;
    this.writeEntered = undefined;
  },
  armRead() {
    this.gate = barrier();
    this.entered = barrier();
    return { entered: this.entered.promise, release: this.gate.resolve };
  },
  armWrite() {
    this.writeGate = barrier();
    this.writeEntered = barrier();
    return {
      entered: this.writeEntered.promise,
      release: this.writeGate.resolve,
    };
  },
};
function synchronous(): never {
  metadataIo.syncCalls++;
  throw new Error("synchronous filesystem access reached native serving metadata");
}
export const existsSync = synchronous;
export const lstatSync = synchronous;
export const mkdirSync = synchronous;
export const readFileSync = synchronous;
export const readdirSync = synchronous;
export const realpathSync = synchronous;
export const writeFileSync = synchronous;

export async function realpath(path: string): Promise<string> {
  const result = metadataIo.shims.get(path);
  if (!result) throw new Error("missing fixture shim");
  return result;
}
export async function readdir(path: string): Promise<string[]> {
  const result = metadataIo.directories.get(path);
  if (!result) throw new Error("missing fixture directory");
  return result;
}
export async function lstat(path: string) {
  const result = metadataIo.stats.get(path);
  if (!result) throw new Error("vanished fixture entry");
  return { isDirectory: () => result.directory, mtimeMs: result.mtimeMs };
}
export async function readFile(path: string): Promise<string> {
  metadataIo.asyncReads++;
  metadataIo.readPaths.push(path);
  metadataIo.entered?.resolve();
  await metadataIo.gate?.promise;
  if (metadataIo.failRead) throw new Error("unavailable fixture file");
  const result = metadataIo.files.get(path);
  if (result === undefined) throw new Error("missing fixture file");
  return result;
}
export async function mkdir() {}
export async function writeFile(path: string, value: string): Promise<void> {
  metadataIo.asyncWrites++;
  metadataIo.writePaths.push(path);
  if (metadataIo.partialWrite !== undefined) metadataIo.files.set(path, metadataIo.partialWrite);
  metadataIo.writeEntered?.resolve();
  await metadataIo.writeGate?.promise;
  if (metadataIo.failWrite) throw new Error("unavailable fixture directory");
  metadataIo.files.set(path, value);
  metadataIo.written?.resolve();
}
export class CursorServerConfigError extends Error {
  constructor(
    message: string,
    readonly status: number
  ) {
    super(message);
  }
}
export async function resolveCursorAgentUrl(): Promise<string> {
  if (metadataIo.agentFailure) throw new CursorServerConfigError("fixture config unavailable", 503);
  return "https://api5.cursor.sh/agent.v1.AgentService/Run";
}
