import { AsyncLocalStorage } from "node:async_hooks";
import { execFile } from "node:child_process";
import fs from "node:fs";
import { access, readFile, writeFile, mkdir, unlink } from "node:fs/promises";
import { parseDatabaseBackend } from "../lib/db/backendSelection";

const OWNER = Symbol.for("omniroute.nativeMitmHostIo");
type Owner = { signals: AsyncLocalStorage<AbortSignal | undefined>; commands: number };
const globals = globalThis as typeof globalThis & { [OWNER]?: Owner };
const owner = (globals[OWNER] ??= { signals: new AsyncLocalStorage(), commands: 0 });

export const nativeMitmEnabled = () => parseDatabaseBackend() === "postgres";
export const nativeMitmSignal = () => owner.signals.getStore();
export function assertNativeMitmNotAborted(): void {
  if (nativeMitmEnabled()) nativeMitmSignal()?.throwIfAborted();
}
export async function withNativeMitmCancellation<T>(
  signal: AbortSignal | undefined,
  body: () => Promise<T>
): Promise<T> {
  if (!nativeMitmEnabled()) return body();
  signal?.throwIfAborted();
  return owner.signals.run(signal, async () => {
    const result = await body();
    signal?.throwIfAborted();
    return result;
  });
}

export async function existsAsync(file: string): Promise<boolean> {
  assertNativeMitmNotAborted();
  try {
    await access(file);
    assertNativeMitmNotAborted();
    return true;
  } catch {
    assertNativeMitmNotAborted();
    return false;
  }
}
export function readTextAsync(file: string): Promise<string> {
  assertNativeMitmNotAborted();
  return readFile(file, { encoding: "utf8", signal: nativeMitmSignal() });
}

export async function mitmFileExists(file: string): Promise<boolean> {
  return nativeMitmEnabled() ? existsAsync(file) : fs.existsSync(file);
}
export function readMitmFile(file: string): Promise<Buffer>;
export function readMitmFile(file: string, encoding: "utf8"): Promise<string>;
export async function readMitmFile(file: string, encoding?: "utf8"): Promise<Buffer | string> {
  if (!nativeMitmEnabled())
    return encoding ? fs.readFileSync(file, encoding) : fs.readFileSync(file);
  assertNativeMitmNotAborted();
  return readFile(file, { ...(encoding ? { encoding } : {}), signal: nativeMitmSignal() });
}
export async function writeMitmFile(file: string, content: string): Promise<void> {
  if (!nativeMitmEnabled()) return fs.writeFileSync(file, content);
  assertNativeMitmNotAborted();
  await writeFile(file, content, { signal: nativeMitmSignal() });
}
export async function mkdirMitmDirectory(directory: string): Promise<void> {
  if (!nativeMitmEnabled()) {
    fs.mkdirSync(directory, { recursive: true });
    return;
  }
  assertNativeMitmNotAborted();
  await mkdir(directory, { recursive: true });
}
export async function removeMitmFile(file: string): Promise<void> {
  if (!nativeMitmEnabled()) return fs.unlinkSync(file);
  assertNativeMitmNotAborted();
  await unlink(file);
}

export interface NativeMitmCommandResult {
  code: number | null;
  stdout: string;
  stderr: string;
  error: Error | null;
}

/** Fixed executable/argv owners only. Sent host mutations are never automatically replayed. */
export function runNativeMitmCommand(
  command: string,
  args: readonly string[],
  options: { stdin?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number } = {}
): Promise<NativeMitmCommandResult> {
  assertNativeMitmNotAborted();
  if (owner.commands >= 32) return Promise.reject(new Error("MITM host command capacity is busy"));
  const timeout = Math.min(Math.max(options.timeoutMs ?? 30_000, 1), 30_000);
  owner.commands++;
  return new Promise<NativeMitmCommandResult>((resolve, reject) => {
    let closed = false;
    let result: NativeMitmCommandResult | undefined;
    const finish = () => {
      if (closed && result) resolve(result);
    };
    try {
      const child = execFile(
        command,
        [...args],
        {
          encoding: "utf8",
          windowsHide: true,
          timeout,
          killSignal: "SIGKILL",
          maxBuffer: 1024 * 1024,
          signal: nativeMitmSignal(),
          ...(options.env ? { env: options.env } : {}),
        },
        (error, stdout, stderr) => {
          const ordinaryExit = error && typeof error.code === "number" && !error.killed;
          result = {
            code: ordinaryExit ? Number(error.code) : error ? null : 0,
            stdout,
            stderr,
            error: ordinaryExit ? null : error,
          };
          finish();
        }
      );
      child.once("close", () => {
        closed = true;
        finish();
      });
      child.stdin?.on("error", () => {});
      child.stdin?.end(options.stdin ?? "");
    } catch (error) {
      reject(error);
    }
  }).finally(() => {
    owner.commands--;
  });
}
