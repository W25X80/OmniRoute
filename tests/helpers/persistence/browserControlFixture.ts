import { EventEmitter } from "node:events";
import * as realFs from "node:fs";
import * as realAsyncFs from "node:fs/promises";

export function deferred() {
  let release!: () => void;
  let enter!: () => void;
  const ready = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const waiting = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { ready, waiting, enter, release };
}

export type SocketMetadata = {
  dev: number;
  ino: number;
  uid: number;
  mode: number;
  isSocket(): boolean;
};

export const controls = {
  nativeOnly: true,
  syncCalls: [] as string[],
  asyncCalls: [] as string[],
  sockets: new Map<string, SocketMetadata>(),
  existingPaths: new Set<string>(),
  nextInode: 1,
  probe: "refused" as "refused" | "connected" | "timeout" | "denied",
  probeTimeouts: [] as number[],
  probeCount: 0,
  beforeProbe: undefined as (() => void) | undefined,
  servers: [] as FakeServer[],
  asyncGate: undefined as
    { operation: string; match: string; gate: ReturnType<typeof deferred> } | undefined,
  asyncFailure: undefined as { operation: string; match: string; error: Error } | undefined,
  acceptOnListen: false,
  serverCloseActiveCounts: [] as number[],
  contextCloses: 0,
  pageCloses: 0,
  browserCloses: 0,
  browserConnections: [] as Array<{ endpoint: string; options: unknown }>,
};

export function resetControls(): void {
  controls.nativeOnly = true;
  controls.syncCalls.length = 0;
  controls.asyncCalls.length = 0;
  controls.sockets.clear();
  controls.existingPaths.clear();
  controls.nextInode = 1;
  controls.probe = "refused";
  controls.probeTimeouts.length = 0;
  controls.probeCount = 0;
  controls.beforeProbe = undefined;
  controls.servers.length = 0;
  controls.asyncGate = undefined;
  controls.asyncFailure = undefined;
  controls.acceptOnListen = false;
  controls.serverCloseActiveCounts.length = 0;
  controls.contextCloses = 0;
  controls.pageCloses = 0;
  controls.browserCloses = 0;
  controls.browserConnections.length = 0;
}

function sync(name: string) {
  controls.syncCalls.push(name);
  if (controls.nativeOnly)
    throw new Error(`Unexpected native synchronous filesystem call: ${name}`);
}

async function before(operation: string, path: string): Promise<void> {
  controls.asyncCalls.push(`${operation}:${path}`);
  const pause = controls.asyncGate;
  if (pause?.operation === operation && path.startsWith(pause.match)) {
    controls.asyncGate = undefined;
    pause.gate.enter();
    await pause.gate.waiting;
  }
  const failure = controls.asyncFailure;
  if (failure?.operation === operation && path.startsWith(failure.match)) {
    controls.asyncFailure = undefined;
    throw failure.error;
  }
}

function missing(): NodeJS.ErrnoException {
  return Object.assign(new Error("isolated missing path"), { code: "ENOENT" });
}

export function socketMetadata(overrides: Partial<SocketMetadata> = {}): SocketMetadata {
  return {
    dev: 1,
    ino: controls.nextInode++,
    uid: process.getuid?.() ?? 0,
    mode: 0o600,
    isSocket: () => true,
    ...overrides,
  };
}

export function existsSync(path: string): boolean {
  sync("existsSync");
  return controls.sockets.has(path) || controls.existingPaths.has(path) || realFs.existsSync(path);
}
export function readFileSync(path: string, encoding: BufferEncoding): string {
  sync("readFileSync");
  return realFs.readFileSync(path, encoding);
}
export function mkdirSync(path: string, options?: realFs.MakeDirectoryOptions): string | undefined {
  sync("mkdirSync");
  return path.startsWith("/virtual") ? undefined : realFs.mkdirSync(path, options);
}
export function lstatSync(path: string): SocketMetadata {
  sync("lstatSync");
  const stat = controls.sockets.get(path);
  if (!stat) throw missing();
  return { ...stat };
}
export function chmodSync(path: string, mode: number): void {
  sync("chmodSync");
  const stat = controls.sockets.get(path);
  if (stat) stat.mode = mode;
  else realFs.chmodSync(path, mode);
}
export function unlinkSync(path: string): void {
  sync("unlinkSync");
  if (!controls.sockets.delete(path)) realFs.unlinkSync(path);
}
export function rmSync(path: string, options?: realFs.RmOptions): void {
  sync("rmSync");
  realFs.rmSync(path, options);
}
export function openSync(path: string, flags: string, mode?: number): number {
  sync("openSync");
  return realFs.openSync(path, flags, mode);
}
export function closeSync(fd: number): void {
  sync("closeSync");
  realFs.closeSync(fd);
}
export function renameSync(from: string, to: string): void {
  sync("renameSync");
  realFs.renameSync(from, to);
}
export function writeFileSync(fd: number, data: string | Uint8Array): void {
  sync("writeFileSync");
  realFs.writeFileSync(fd, data);
}

export async function access(path: string): Promise<void> {
  await before("access", path);
  if (!controls.existingPaths.has(path)) await realAsyncFs.access(path);
}
export async function readFile(path: string, encoding: BufferEncoding): Promise<string> {
  await before("readFile", path);
  return realAsyncFs.readFile(path, encoding);
}
export async function mkdir(path: string, options?: realFs.MakeDirectoryOptions) {
  await before("mkdir", path);
  return path.startsWith("/virtual") ? undefined : realAsyncFs.mkdir(path, options);
}
export async function lstat(path: string): Promise<SocketMetadata> {
  await before("lstat", path);
  const stat = controls.sockets.get(path);
  if (!stat) throw missing();
  return { ...stat };
}
export async function chmod(path: string, mode: number): Promise<void> {
  await before("chmod", path);
  const stat = controls.sockets.get(path);
  if (stat) stat.mode = mode;
  else await realAsyncFs.chmod(path, mode);
}
export async function unlink(path: string): Promise<void> {
  await before("unlink", path);
  if (!controls.sockets.delete(path)) await realAsyncFs.unlink(path);
}
export async function rm(path: string, options?: realFs.RmOptions): Promise<void> {
  await before("rm", path);
  await realAsyncFs.rm(path, options);
}
export async function open(path: string, flags: string, mode?: number) {
  await before("open", path);
  return realAsyncFs.open(path, flags, mode);
}
export async function rename(from: string, to: string): Promise<void> {
  await before("rename", from);
  await realAsyncFs.rename(from, to);
}

class FakeProbe extends EventEmitter {
  destroy(): void {}
  setTimeout(ms: number, callback: () => void): this {
    controls.probeTimeouts.push(ms);
    queueMicrotask(() => {
      controls.beforeProbe?.();
      if (controls.probe === "connected") this.emit("connect");
      else if (controls.probe === "timeout") callback();
      else
        this.emit(
          "error",
          Object.assign(new Error("isolated probe result"), {
            code: controls.probe === "denied" ? "EACCES" : "ECONNREFUSED",
          })
        );
    });
    return this;
  }
}
export class FakeAcceptedSocket extends EventEmitter {
  destroyed = false;
  setEncoding(): this {
    return this;
  }
  destroy(): void {
    this.destroyed = true;
    this.emit("close");
  }
  end(): void {
    this.destroy();
  }
}
export class FakeServer extends EventEmitter {
  listening = false;
  accepted: FakeAcceptedSocket[] = [];
  constructor(private readonly onConnection?: (socket: FakeAcceptedSocket) => void) {
    super();
  }
  listen(path: string, callback: () => void): this {
    const existing = controls.sockets.get(path);
    if (existing) queueMicrotask(() => this.emit("error", new Error("isolated path in use")));
    else {
      controls.sockets.set(path, socketMetadata({ mode: 0o755 }));
      this.listening = true;
      queueMicrotask(() => {
        if (controls.acceptOnListen) {
          const socket = new FakeAcceptedSocket();
          this.accepted.push(socket);
          this.onConnection?.(socket);
        }
        callback();
      });
    }
    return this;
  }
  close(callback: (error?: Error) => void): this {
    controls.serverCloseActiveCounts.push(
      this.accepted.filter((socket) => !socket.destroyed).length
    );
    this.listening = false;
    queueMicrotask(callback);
    return this;
  }
}
export function createServer(onConnection?: (socket: FakeAcceptedSocket) => void): FakeServer {
  const server = new FakeServer(onConnection);
  controls.servers.push(server);
  return server;
}
export function createConnection(): FakeProbe {
  controls.probeCount++;
  return new FakeProbe();
}

const fakePage = {
  async goto() {},
  locator() {
    return {
      first() {
        return { async waitFor() {} };
      },
    };
  },
  url() {
    return "https://chatgpt.com/?temporary-chat=true";
  },
  async close() {
    controls.pageCloses++;
  },
};
const fakeContext = {
  async newPage() {
    return fakePage;
  },
  async close() {
    controls.contextCloses++;
  },
};
const fakeBrowser = {
  async newContext() {
    return fakeContext;
  },
  contexts() {
    return [fakeContext];
  },
  async close() {
    controls.browserCloses++;
  },
};
export const chromium = {
  async connectOverCDP(endpoint: string, options: unknown) {
    controls.browserConnections.push({ endpoint, options });
    return fakeBrowser;
  },
  async launch() {
    return fakeBrowser;
  },
};
export const CHATGPT_COMPOSER_SELECTOR = "fixture-composer";
export const CHATGPT_TEMPORARY_CHAT_URL = "https://chatgpt.com/?temporary-chat=true";
export async function assertAuthenticatedChatGptPage(): Promise<void> {}
export async function assertTemporaryChatPage(): Promise<void> {}
export async function detectChatGptAccountCapabilities() {
  return { solAvailable: false, proAvailable: true };
}
