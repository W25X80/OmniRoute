/**
 * Runtime build identity (#10427).
 *
 * `scripts/build/write-build-sha.mjs` stamps the git SHA into `dist/BUILD_SHA` and
 * `.build/next/standalone/BUILD_SHA` at release-build time. Reading it back at runtime is
 * what lets an operator answer "what code is this box actually running?" over HTTP.
 *
 * Before this existed, answering that question during the 2026-08-14 gateway outage meant
 * SSH-ing into the host and grepping the compiled Next chunks — the deployed package
 * turned out to be built from a feature branch that predated the fix it was supposed to
 * carry.
 *
 * Resolution order: explicit env var (containers can inject it without the sentinel file),
 * then the sentinel files relative to the working directory. Unknown → `null`, never a
 * fabricated or guessed value.
 */

import fs from "node:fs";
import fsPromises from "node:fs/promises";
import { parseDatabaseBackend } from "../db/backendSelection";
import path from "node:path";

const SENTINEL_PATHS = [
  ["dist", "BUILD_SHA"],
  [".build", "next", "standalone", "BUILD_SHA"],
  ["BUILD_SHA"],
];

let cached: string | null | undefined;

export function readRunningBuildSha(cwd: string = process.cwd()): string | null {
  if (parseDatabaseBackend() === "postgres") {
    const state = buildShaGlobals[NATIVE_BUILD_SHA];
    if (state?.loaded) return state.value ?? null;
    throw new Error("Native build identity requires asynchronous lookup");
  }
  if (cached !== undefined) return cached;

  const fromEnv = process.env.OMNIROUTE_BUILD_SHA?.trim();
  if (fromEnv) {
    cached = fromEnv;
    return cached;
  }

  for (const segments of SENTINEL_PATHS) {
    try {
      const value = fs.readFileSync(path.join(cwd, ...segments), "utf8").trim();
      if (value) {
        cached = value;
        return cached;
      }
    } catch {
      // Sentinel absent at this location — try the next one. A dev run has none of them.
    }
  }

  cached = null;
  return cached;
}

const NATIVE_BUILD_SHA = Symbol.for("omniroute.nativeRunningBuildSha");
interface NativeBuildShaState {
  loaded: boolean;
  value?: string | null;
  preparation?: Promise<string | null>;
}
const buildShaGlobals = globalThis as typeof globalThis & {
  [NATIVE_BUILD_SHA]?: NativeBuildShaState;
};

export function readRunningBuildShaAsync(cwd: string = process.cwd()): Promise<string | null> {
  if (parseDatabaseBackend() !== "postgres") return Promise.resolve(readRunningBuildSha(cwd));
  const state = (buildShaGlobals[NATIVE_BUILD_SHA] ??= { loaded: false });
  if (state.loaded) return Promise.resolve(state.value ?? null);
  if (state.preparation) return state.preparation;
  const fromEnv = process.env.OMNIROUTE_BUILD_SHA?.trim();
  state.preparation = Promise.resolve().then(async () => {
    let value: string | null = fromEnv || null;
    if (!value) {
      for (const segments of SENTINEL_PATHS) {
        try {
          const candidate = (await fsPromises.readFile(path.join(cwd, ...segments), "utf8")).trim();
          if (candidate) {
            value = candidate;
            break;
          }
        } catch {
          // Exact source sentinel priority and absent/unreadable fallback.
        }
      }
    }
    state.value = value;
    state.loaded = true;
    return value;
  });
  return state.preparation;
}
