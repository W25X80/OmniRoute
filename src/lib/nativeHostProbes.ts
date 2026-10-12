import { createRequire } from "node:module";
import type { ChildProcess } from "node:child_process";
import { getSelectedPostgresDatabase } from "./db/controlPlaneContext";

const require = createRequire(import.meta.url);
const childProcess = require("node:child_process") as typeof import("node:child_process");
const OUTPUT_LIMIT_BYTES = 1024 * 1024;
const MAX_ACTIVE_PROBES = 64;
const MAX_PROBE_TIMEOUT_MS = 30_000;

type ProbeState = { closing: boolean; active: Map<ChildProcess, Promise<void>> };
declare global {
  var __omnirouteNativeHostProbes: ProbeState | undefined;
}
const state = (globalThis.__omnirouteNativeHostProbes ??= {
  closing: false,
  active: new Map(),
});

/** Capture before deferred host work; probes do not hold a database lease. */
export function usesNativeHostProfile(): boolean {
  return process.env.OMNIROUTE_DATABASE_BACKEND === "postgres" || !!getSelectedPostgresDatabase();
}

export interface NativeHostProbeResult {
  success: boolean;
  stdout: string;
  failure?: "closed" | "failed" | "timeout" | "output_limit" | "busy" | "aborted";
}

/** Internal capability probes only: callers own fixed argv and ACP version validation. */
export function probeNativeHostCapability(
  command: string,
  args: readonly string[],
  options: {
    shell?: boolean;
    ignoreOutput?: boolean;
    timeoutMs?: number;
    env?: NodeJS.ProcessEnv;
    signal?: AbortSignal;
    windowsHide?: boolean;
  } = {}
): Promise<NativeHostProbeResult> {
  if (state.closing) return Promise.resolve({ success: false, stdout: "", failure: "closed" });
  if (options.signal?.aborted)
    return Promise.resolve({ success: false, stdout: "", failure: "aborted" });
  if (state.active.size >= MAX_ACTIVE_PROBES)
    return Promise.resolve({ success: false, stdout: "", failure: "busy" });
  return new Promise((resolve) => {
    let proc: ChildProcess;
    try {
      proc = childProcess.spawn(command, [...args], {
        stdio: options.ignoreOutput ? "ignore" : ["pipe", "pipe", "pipe"],
        ...(options.shell ? { shell: true } : {}),
        ...(options.env ? { env: options.env } : {}),
        ...(options.windowsHide ? { windowsHide: true } : {}),
      });
    } catch {
      resolve({ success: false, stdout: "", failure: "failed" });
      return;
    }
    let closed!: () => void;
    const close = new Promise<void>((done) => {
      closed = done;
    });
    state.active.set(proc, close);
    let failure: NativeHostProbeResult["failure"];
    const stdout: Buffer[] = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    const stop = (reason: NonNullable<NativeHostProbeResult["failure"]>) => {
      failure ??= reason;
      proc.kill("SIGKILL");
      proc.stdin?.destroy();
      proc.stdout?.destroy();
      proc.stderr?.destroy();
    };
    const timeoutMs =
      Number.isFinite(options.timeoutMs) && options.timeoutMs! > 0
        ? Math.min(options.timeoutMs!, MAX_PROBE_TIMEOUT_MS)
        : 5000;
    const timer = setTimeout(() => stop("timeout"), timeoutMs);
    const abort = () => stop("aborted");
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.signal?.aborted) abort();
    proc.stdout?.on("data", (value: Buffer) => {
      stdoutBytes += value.length;
      if (stdoutBytes > OUTPUT_LIMIT_BYTES) stop("output_limit");
      else if (!failure) stdout.push(value);
    });
    proc.stderr?.on("data", (value: Buffer) => {
      stderrBytes += value.length;
      if (stderrBytes > OUTPUT_LIMIT_BYTES) stop("output_limit");
    });
    proc.once("error", () => {
      failure ??= "failed";
    });
    proc.once("close", (code) => {
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", abort);
      state.active.delete(proc);
      closed();
      resolve({
        success: !failure && code === 0,
        stdout: Buffer.concat(stdout).toString("utf8"),
        ...(failure ? { failure } : code !== 0 ? { failure: "failed" as const } : {}),
      });
    });
    // execFileSync with a piped stdin sends EOF; version commands must see the same input.
    proc.stdin?.on("error", () => {});
    proc.stdin?.end();
  });
}

/** The source CLI lookup is a fixed trusted shell expression, never caller command text. */
export async function findOmniRouteCliPathAsync(): Promise<string | null> {
  const result = await probeNativeHostCapability(
    "which omniroute 2>/dev/null || command -v omniroute 2>/dev/null",
    [],
    { shell: true, timeoutMs: 3000 }
  );
  return result.success ? result.stdout.trim() || null : null;
}

/** Close admission and join actual close events, rather than treating kill() as an ACK. */
export async function stopNativeHostProbes(): Promise<void> {
  state.closing = true;
  const active = [...state.active];
  for (const [proc] of active) {
    proc.kill("SIGKILL");
    proc.stdin?.destroy();
    proc.stdout?.destroy();
    proc.stderr?.destroy();
  }
  await Promise.all(active.map(([, close]) => close));
}
