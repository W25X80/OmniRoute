import * as fs from "node:fs";
import * as files from "node:fs/promises";

export function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((done) => {
    release = done;
  });
  return { promise, release };
}
interface Gate {
  path: string;
  entered: ReturnType<typeof barrier>;
  release: ReturnType<typeof barrier>;
}
export const threadFileFixture = {
  syncCalls: 0,
  reads: 0,
  writes: 0,
  allowSync: true,
  readGate: undefined as Gate | undefined,
  writeGate: undefined as Gate | undefined,
  gate(kind: "read" | "write", path: string): Gate {
    const gate = { path, entered: barrier(), release: barrier() };
    if (kind === "read") this.readGate = gate;
    else this.writeGate = gate;
    return gate;
  },
  reset() {
    this.readGate?.release.release();
    this.writeGate?.release.release();
    this.readGate = undefined;
    this.writeGate = undefined;
    this.syncCalls = this.reads = this.writes = 0;
    this.allowSync = true;
  },
};
function syncCall(): void {
  threadFileFixture.syncCalls++;
  if (!threadFileFixture.allowSync) throw new Error("Synchronous native thread cache I/O");
}
export function existsSync(path: fs.PathLike): boolean {
  syncCall();
  return fs.existsSync(path);
}
export function mkdirSync(
  path: fs.PathLike,
  options?: fs.MakeDirectoryOptions
): string | undefined {
  syncCall();
  return fs.mkdirSync(path, options);
}
export function readFileSync(path: fs.PathOrFileDescriptor, encoding: "utf8"): string {
  syncCall();
  return fs.readFileSync(path, encoding);
}
export function writeFileSync(
  path: fs.PathOrFileDescriptor,
  value: string,
  encoding: "utf8"
): void {
  syncCall();
  fs.writeFileSync(path, value, encoding);
}
export async function readFile(path: string, encoding: "utf8"): Promise<string> {
  threadFileFixture.reads++;
  const gate = threadFileFixture.readGate;
  if (gate?.path === path) {
    threadFileFixture.readGate = undefined;
    gate.entered.release();
    await gate.release.promise;
  }
  return files.readFile(path, encoding);
}
export async function mkdir(
  path: string,
  options: { recursive: true }
): Promise<string | undefined> {
  return files.mkdir(path, options);
}
export async function writeFile(path: string, value: string, encoding: "utf8"): Promise<void> {
  threadFileFixture.writes++;
  const gate = threadFileFixture.writeGate;
  if (gate?.path === path) {
    threadFileFixture.writeGate = undefined;
    gate.entered.release();
    await gate.release.promise;
  }
  await files.writeFile(path, value, encoding);
}
