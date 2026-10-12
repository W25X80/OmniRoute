import type { ChildProcess } from "node:child_process";
import { open, readFile, mkdir, writeFile, unlink } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { getSelectedPersistenceIdentity } from "../db/controlPlaneContext";
import {
  getPersistenceRuntimeStatus,
  isApplicationPersistenceIdentity,
} from "../db/persistenceRuntime";
import { DATA_DIR } from "../db/core";
import { findHeadroomBinaryAsync } from "./detect";
import { HeadroomError, isPidAlive, type StartResult, type StopResult } from "./process";

const childProcess = createRequire(import.meta.url)(
  "node:child_process"
) as typeof import("node:child_process");
const STARTUP_TIMEOUT_MS = 8000;
const STOP_GRACE_MS = 2000;
const MAX_OPERATIONS = 16;
const OWNER = Symbol.for("omniroute.nativeHeadroomProcess");

interface OwnedChild {
  child: ChildProcess;
  pid: number;
  closed: Promise<void>;
  exited: boolean;
  failure?: Error;
}
interface HeadroomOwner {
  identity: object;
  directory: string;
  active: Set<Promise<unknown>>;
  tail: Promise<void>;
  owned?: OwnedChild;
  closing: boolean;
  stopping?: Promise<void>;
  cleanupError?: unknown;
}
const globals = globalThis as typeof globalThis & { [OWNER]?: HeadroomOwner };

function captureOwner(): HeadroomOwner {
  const identity = getSelectedPersistenceIdentity();
  if (!identity || !isApplicationPersistenceIdentity(identity))
    throw new HeadroomError("Headroom requires application persistence", "NATIVE_UNAVAILABLE");
  const existing = globals[OWNER];
  if (existing && existing.identity !== identity)
    throw new HeadroomError("Headroom persistence identity changed", "IDENTITY_CHANGED");
  if (existing?.closing || getPersistenceRuntimeStatus().phase !== "ready")
    throw new HeadroomError("Headroom is stopping", "STOPPING");
  return (
    existing ??
    (globals[OWNER] = {
      identity,
      directory: path.join(DATA_DIR ?? ".", "headroom"),
      active: new Set(),
      tail: Promise.resolve(),
      closing: false,
    })
  );
}

function assertScope(owner: HeadroomOwner, signal?: AbortSignal): void {
  if (
    getSelectedPersistenceIdentity() !== owner.identity ||
    !isApplicationPersistenceIdentity(owner.identity)
  )
    throw new HeadroomError("Headroom persistence authority ended", "IDENTITY_CHANGED");
  signal?.throwIfAborted();
}

function serialize<T>(owner: HeadroomOwner, body: () => Promise<T>): Promise<T> {
  const work = owner.tail.then(body);
  owner.tail = work.then(
    () => {},
    () => {}
  );
  return work;
}

async function operation<T>(body: (owner: HeadroomOwner) => Promise<T>): Promise<T> {
  const owner = captureOwner();
  if (owner.active.size >= MAX_OPERATIONS)
    return Promise.reject(new HeadroomError("Headroom operation queue is full", "BUSY"));
  const work = serialize(owner, () => body(owner));
  owner.active.add(work);
  void work.then(
    () => owner.active.delete(work),
    () => owner.active.delete(work)
  );
  return work;
}

async function readPid(owner: HeadroomOwner): Promise<number | null> {
  try {
    const raw = await readFile(path.join(owner.directory, "proxy.pid"), "utf8");
    const pid = parseInt(raw.trim(), 10);
    return Number.isFinite(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

async function clearPidIfMatches(owner: HeadroomOwner, pid: number): Promise<void> {
  if ((await readPid(owner)) !== pid) return;
  try {
    await unlink(path.join(owner.directory, "proxy.pid"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

function observeExit(owner: HeadroomOwner, entry: OwnedChild): void {
  void serialize(owner, async () => {
    if (owner.owned !== entry) return;
    await clearPidIfMatches(owner, entry.pid);
    if (owner.owned === entry) owner.owned = undefined;
  }).catch((error: unknown) => {
    owner.cleanupError ??= error;
  });
}

async function stopOwned(entry: OwnedChild): Promise<void> {
  if (entry.exited) {
    await entry.closed;
    return;
  }
  entry.child.kill("SIGTERM");
  const timer = setTimeout(() => {
    if (!entry.exited) entry.child.kill("SIGKILL");
  }, STOP_GRACE_MS);
  try {
    await entry.closed;
  } finally {
    clearTimeout(timer);
  }
}

export function getManagedHeadroomPidAsync(signal?: AbortSignal): Promise<number | null> {
  return operation(async (owner) => {
    assertScope(owner, signal);
    const pid = await readPid(owner);
    assertScope(owner, signal);
    return pid && isPidAlive(pid) ? pid : null;
  });
}

export function startHeadroomProxyAsync(
  options: { port?: number; signal?: AbortSignal } = {}
): Promise<StartResult> {
  return operation(async (owner) => {
    assertScope(owner, options.signal);
    const binary = await findHeadroomBinaryAsync({ signal: options.signal });
    assertScope(owner, options.signal);
    if (!binary) throw new HeadroomError("Headroom CLI not installed", "NOT_INSTALLED");
    const existing = await readPid(owner);
    assertScope(owner, options.signal);
    if (existing && isPidAlive(existing)) return { pid: existing, alreadyRunning: true };
    const headroomDirectory = owner.directory;
    assertScope(owner, options.signal);
    await mkdir(headroomDirectory, { recursive: true });
    assertScope(owner, options.signal);
    const output = await open(path.join(headroomDirectory, "proxy.log"), "a");
    let entry: OwnedChild | undefined;
    let accepted = false;
    try {
      assertScope(owner, options.signal);
      const n = Number(options.port);
      const port = Number.isFinite(n) && n > 0 && n < 65536 ? n : 8787;
      const child = childProcess.spawn(binary, ["proxy", "--port", String(port)], {
        stdio: ["ignore", output.fd, output.fd],
        detached: true,
        windowsHide: true,
        env: { ...process.env },
      });
      let close!: () => void;
      const closed = new Promise<void>((resolve) => {
        close = resolve;
      });
      entry = { child, pid: child.pid ?? 0, closed, exited: false };
      const launched = entry;
      child.once("error", (error: Error) => {
        launched.failure = error;
      });
      child.once("close", () => {
        launched.exited = true;
        close();
        observeExit(owner, launched);
      });
      if (!child.pid) {
        await closed;
        throw new HeadroomError("Failed to spawn headroom proxy", "SPAWN_FAILED");
      }
      owner.owned = launched;
      assertScope(owner, options.signal);
      await writeFile(path.join(headroomDirectory, "proxy.pid"), String(child.pid));
      const waiting = new AbortController();
      const signal = options.signal
        ? AbortSignal.any([options.signal, waiting.signal])
        : waiting.signal;
      try {
        await Promise.race([
          delay(STARTUP_TIMEOUT_MS, undefined, { signal }),
          closed.then(() => {
            throw new HeadroomError(
              "headroom proxy exited during startup — see proxy.log",
              "EARLY_EXIT"
            );
          }),
        ]);
      } finally {
        waiting.abort();
      }
      assertScope(owner, options.signal);
      if (launched.exited || launched.failure || !isPidAlive(child.pid))
        throw new HeadroomError(
          "headroom proxy exited during startup — see proxy.log",
          "EARLY_EXIT"
        );
      child.unref();
      accepted = true;
      return { pid: child.pid, alreadyRunning: false };
    } finally {
      try {
        if (entry && !accepted) {
          if (!entry.exited) entry.child.kill("SIGKILL");
          await entry.closed;
          await clearPidIfMatches(owner, entry.pid);
          if (owner.owned === entry) owner.owned = undefined;
        }
      } finally {
        await output.close();
      }
    }
  });
}

export function stopHeadroomProxyAsync(signal?: AbortSignal): Promise<StopResult> {
  return operation(async (owner) => {
    assertScope(owner, signal);
    const pid = await readPid(owner);
    assertScope(owner, signal);
    if (!pid || !isPidAlive(pid)) return { stopped: false, reason: "not_running" };
    const owned = owner.owned?.pid === pid ? owner.owned : undefined;
    if (owned) await stopOwned(owned);
    else {
      process.kill(pid, "SIGTERM");
      const grace = Date.now() + STOP_GRACE_MS;
      while (isPidAlive(pid) && Date.now() < grace) await delay(25);
      if (isPidAlive(pid)) process.kill(pid, "SIGKILL");
      const killed = Date.now() + STOP_GRACE_MS;
      while (isPidAlive(pid) && Date.now() < killed) await delay(25);
      if (isPidAlive(pid))
        throw new HeadroomError("Headroom process has not exited", "STOP_FAILED");
    }
    await clearPidIfMatches(owner, pid);
    if (owner.owned === owned) owner.owned = undefined;
    return { stopped: true, pid };
  });
}

/** Close new admission without interrupting already accepted operations. */
export function beginNativeHeadroomClose(): void {
  if (globals[OWNER]) globals[OWNER]!.closing = true;
}

/** Adopted host processes are not application-owned; only explicit stop may signal them. */
export function stopNativeHeadroomProcesses(): Promise<void> {
  beginNativeHeadroomClose();
  const owner = globals[OWNER];
  if (!owner) return Promise.resolve();
  return (owner.stopping ??= (async () => {
    await Promise.allSettled([...owner.active]);
    const owned = owner.owned;
    if (owned) {
      await stopOwned(owned);
      await clearPidIfMatches(owner, owned.pid);
      if (owner.owned === owned) owner.owned = undefined;
    }
    await owner.tail;
    if (owner.cleanupError) throw owner.cleanupError;
  })());
}
