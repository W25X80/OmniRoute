import { createRequire } from "node:module";
import type { ChildProcess } from "node:child_process";
import { getSelectedPostgresDatabase } from "./db/controlPlaneContext";

const require = createRequire(import.meta.url);
const childProcess = require("node:child_process") as typeof import("node:child_process");
const OUTPUT_LIMIT_BYTES = 1024 * 1024;

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
  failure?: "closed" | "failed" | "timeout" | "output_limit";
}

/** Internal capability probes only: callers own fixed argv and ACP version validation. */
export function probeNativeHostCapability(
  command: string,
  args: readonly string[],
  options: { shell?: boolean; ignoreOutput?: boolean; timeoutMs?: number } = {}
): Promise<NativeHostProbeResult> {
  if (state.closing) return Promise.resolve({ success: false, stdout: "", failure: "closed" });
  return new Promise((resolve) => {
    let proc: ChildProcess;
    try {
      proc = childProcess.spawn(command, [...args], {
        stdio: options.ignoreOutput ? "ignore" : ["pipe", "pipe", "pipe"],
        ...(options.shell ? { shell: true } : {}),
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
    const timer = setTimeout(() => stop("timeout"), options.timeoutMs ?? 5000);
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
