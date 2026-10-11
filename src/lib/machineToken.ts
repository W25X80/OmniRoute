import { createHash, createHmac, randomBytes } from "node:crypto";
import { exec } from "node:child_process";
import * as fs from "node:fs";
import fsPromises from "node:fs/promises";
import * as path from "node:path";
import * as nodeModule from "node:module";
import { resolveDataDir, resolveDataDirAsync, isTestContext } from "./dataPaths";
import { parseDatabaseBackend } from "./db/backendSelection";

let machineIdSync: (original?: boolean) => string;
let machineIdAsync: (original?: boolean) => Promise<string>;
try {
  // Anchor runtime resolution to the process entrypoint. Turbopack rewrites
  // createRequire(import.meta.url) into an in-bundle resolver, which cannot load
  // external CommonJS packages from the installed standalone node_modules tree.
  const runtimeRequire = nodeModule.createRequire(process.argv[1] || process.cwd());
  const mod = runtimeRequire("node-machine-id");
  machineIdSync = mod.machineIdSync || mod.default?.machineIdSync;
  machineIdAsync =
    typeof machineIdSync === "function" ? readDependencyMachineIdAsync : async () => "";
} catch {
  machineIdSync = () => "";
  machineIdAsync = async () => "";
}

// #13679 PR B: checked-in literal, used ONLY as a last-resort fallback (see
// getActiveSalt() below) — /etc/machine-id is commonly world-readable, so relying on
// this literal as the real default let any local user derive the same bearer token.
const BUILTIN_DEFAULT_SALT = "omniroute-cli-auth-v1";
const SALT_FILE_NAME = "cli-token-salt.json";
const PERSISTED_SALT_RE = /^[0-9a-f]{64}$/;

function saltFilePath(dataDir: string): string {
  return path.join(dataDir, SALT_FILE_NAME);
}

function readPersistedSalt(filePath: string): string | null {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const parsed: unknown = JSON.parse(raw);
    const salt = (parsed as { salt?: unknown } | null)?.salt;
    if (typeof salt === "string" && PERSISTED_SALT_RE.test(salt)) return salt;
  } catch {
    // Missing, unreadable, or corrupt — fall through to (re)generation.
  }
  return null;
}

/**
 * Generate a random per-install salt on first use and persist it under DATA_DIR so
 * every process on this install (the server and every `omniroute` CLI invocation,
 * see the mirrored logic in bin/cli/utils/cliToken.mjs) converges on the same value.
 * The write uses the `wx` flag (fails if the file already exists) so a race between
 * two processes both hitting "no file yet" at once cannot clobber one another — the
 * loser just reads back what the winner wrote instead of overwriting it.
 */
function establishPersistedSalt(dataDir: string): string | null {
  const filePath = saltFilePath(dataDir);
  const existing = readPersistedSalt(filePath);
  if (existing) return existing;

  const generated = randomBytes(32).toString("hex");
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify({ salt: generated }), { flag: "wx", mode: 0o600 });
    return generated;
  } catch (err) {
    if ((err as NodeJS.ErrnoException | null)?.code === "EEXIST") {
      return readPersistedSalt(filePath);
    }
    return null;
  }
}

let cachedActiveSalt: string | null = null;

function getActiveSalt(): string {
  const envSalt = process.env.OMNIROUTE_CLI_SALT;
  if (envSalt) return envSalt;

  if (cachedActiveSalt) return cachedActiveSalt;

  const hasExplicitDataDir = !!process.env.DATA_DIR?.trim();
  // A test process that never opted into an explicit DATA_DIR must not write a salt
  // file into the operator's real home directory — mirrors the same class of guard
  // resolveWritableDataDir() applies for the DB (dataPaths.ts::isTestContext). Falling
  // back to the literal here only affects tests that forgot to set DATA_DIR; every
  // production path (server boot, packaged CLI) always has one.
  if (!hasExplicitDataDir && isTestContext()) {
    cachedActiveSalt = BUILTIN_DEFAULT_SALT;
    return cachedActiveSalt;
  }

  const dataDir = resolveDataDir();
  const persisted = establishPersistedSalt(dataDir);
  if (!persisted) {
    console.warn(
      "[CLI_TOKEN] Could not persist a private CLI salt in DATA_DIR; using the compatibility fallback. Fix DATA_DIR permissions or set OMNIROUTE_CLI_SALT."
    );
  }
  cachedActiveSalt = persisted ?? BUILTIN_DEFAULT_SALT;
  return cachedActiveSalt;
}

export function deriveMachineToken(rawId: string, salt: string): string {
  if (!rawId) return "";
  return createHmac("sha256", rawId).update(salt).digest("hex");
}

export function deriveLegacyCliToken(machineId: string, salt: string): string {
  if (!machineId) return "";
  return createHash("sha256")
    .update(machineId + salt)
    .digest("hex")
    .substring(0, 32);
}

let cached: string | null = null;
let cachedSalt: string | null = null;

export function getMachineTokenSync(salt?: string): string {
  if (parseDatabaseBackend() === "postgres") {
    throw new Error("Native CLI authentication requires asynchronous identity lookup");
  }
  const activeSalt = salt ?? getActiveSalt();
  try {
    // machineIdSync(true) returns the original unhashed hardware ID.
    const rawId = machineIdSync(true);
    if (!rawId) return "";
    if (activeSalt === cachedSalt && cached !== null) return cached;
    const token = deriveMachineToken(rawId, activeSalt);
    if (!salt) {
      cached = token;
      cachedSalt = activeSalt;
    }
    return token;
  } catch {
    return "";
  }
}

export function getLegacyCliTokenSync(salt?: string): string {
  if (parseDatabaseBackend() === "postgres") {
    throw new Error("Native CLI authentication requires asynchronous identity lookup");
  }
  const activeSalt = salt ?? getActiveSalt();
  try {
    const machineId = machineIdSync();
    return deriveLegacyCliToken(machineId, activeSalt);
  } catch {
    return "";
  }
}

interface NativeSaltState {
  value?: string;
  preparation?: Promise<string>;
}
interface NativeTokenState {
  salts: Map<string, NativeSaltState>;
  token?: string;
  tokenSalt?: string;
}
const NATIVE_CLI_TOKEN = Symbol.for("omniroute.nativeCliTokenState");
const tokenGlobals = globalThis as typeof globalThis & {
  [NATIVE_CLI_TOKEN]?: NativeTokenState;
};
function nativeTokenState(): NativeTokenState {
  return (tokenGlobals[NATIVE_CLI_TOKEN] ??= { salts: new Map() });
}

async function readPersistedSaltAsync(filePath: string): Promise<string | null> {
  try {
    const parsed: unknown = JSON.parse(await fsPromises.readFile(filePath, "utf8"));
    const salt = (parsed as { salt?: unknown } | null)?.salt;
    if (typeof salt === "string" && PERSISTED_SALT_RE.test(salt)) return salt;
  } catch {
    // Match the synchronous absent/unreadable/corrupt fallback.
  }
  return null;
}

async function establishPersistedSaltAsync(dataDir: string): Promise<string | null> {
  const filePath = saltFilePath(dataDir);
  const existing = await readPersistedSaltAsync(filePath);
  if (existing) return existing;
  const generated = randomBytes(32).toString("hex");
  try {
    await fsPromises.mkdir(dataDir, { recursive: true });
    await fsPromises.writeFile(filePath, JSON.stringify({ salt: generated }), {
      flag: "wx",
      mode: 0o600,
    });
    return generated;
  } catch (error) {
    if ((error as NodeJS.ErrnoException | null)?.code === "EEXIST") {
      return readPersistedSaltAsync(filePath);
    }
    return null;
  }
}

async function getActiveSaltAsync(): Promise<string> {
  const envSalt = process.env.OMNIROUTE_CLI_SALT;
  if (envSalt) return envSalt;
  const configured = process.env.DATA_DIR?.trim();
  const testWithoutDataDir = !configured && isTestContext();
  const key = testWithoutDataDir
    ? "test-without-data-dir"
    : configured
      ? path.resolve(configured)
      : "default-data-dir";
  const states = nativeTokenState().salts;
  let state = states.get(key);
  if (!state) {
    state = {};
    states.set(key, state);
  }
  if (state.value) return state.value;
  if (state.preparation) return state.preparation;
  const selected = state;
  // Capture directory resolution before yielding; an environment change cannot retarget this write.
  const directory = testWithoutDataDir ? null : resolveDataDirAsync();
  selected.preparation = Promise.resolve().then(async () => {
    if (!directory) {
      selected.value = BUILTIN_DEFAULT_SALT;
      return selected.value;
    }
    const persisted = await establishPersistedSaltAsync(await directory);
    if (!persisted) {
      console.warn(
        "[CLI_TOKEN] Could not persist a private CLI salt in DATA_DIR; using the compatibility fallback. Fix DATA_DIR permissions or set OMNIROUTE_CLI_SALT."
      );
    }
    selected.value = persisted ?? BUILTIN_DEFAULT_SALT;
    return selected.value;
  });
  return selected.preparation;
}

/** Native lookup remains fresh even when a derived token is cached: errors fail closed. */
export async function getMachineTokenAsync(salt?: string): Promise<string> {
  if (parseDatabaseBackend() !== "postgres") return getMachineTokenSync(salt);
  const activeSalt = salt ?? (await getActiveSaltAsync());
  try {
    const rawId = await machineIdAsync(true);
    if (!rawId) return "";
    const state = nativeTokenState();
    if (activeSalt === state.tokenSalt && state.token !== undefined) return state.token;
    const token = deriveMachineToken(rawId, activeSalt);
    if (!salt) {
      state.token = token;
      state.tokenSalt = activeSalt;
    }
    return token;
  } catch {
    return "";
  }
}

export async function getLegacyCliTokenAsync(salt?: string): Promise<string> {
  if (parseDatabaseBackend() !== "postgres") return getLegacyCliTokenSync(salt);
  const activeSalt = salt ?? (await getActiveSaltAsync());
  try {
    return deriveLegacyCliToken(await machineIdAsync(), activeSalt);
  } catch {
    return "";
  }
}

/**
 * Async command/normalization adapter for node-machine-id 1.1.12. Its callback
 * parser can throw outside the package promise; catch that boundary here.
 * The trusted commands, normalization and default hash follow that dependency.
 *
 * The MIT License (MIT)
 * Copyright (c) 2016 Aleksandr Komlev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
function readDependencyMachineIdAsync(original?: boolean): Promise<string> {
  const platform = process.platform;
  const environment = { ...process.env };
  const mixedWindows =
    platform === "win32" &&
    process.arch === "ia32" &&
    Object.prototype.hasOwnProperty.call(environment, "PROCESSOR_ARCHITEW6432");
  const commands: Record<string, string> = {
    darwin: "ioreg -rd1 -c IOPlatformExpertDevice",
    win32: mixedWindows
      ? "%windir%\\sysnative\\cmd.exe /c %windir%\\System32\\REG.exe QUERY HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography /v MachineGuid"
      : "%windir%\\System32\\REG.exe QUERY HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography /v MachineGuid",
    linux:
      "( cat /var/lib/dbus/machine-id /etc/machine-id 2> /dev/null || hostname ) | head -n 1 || :",
    freebsd: "kenv -q smbios.system.uuid || sysctl -n kern.hostuuid",
  };
  return new Promise((resolve, reject) => {
    const command = commands[platform];
    if (!command) {
      reject(new Error(`Unsupported platform: ${platform}`));
      return;
    }
    exec(command, { env: environment }, (error, stdout) => {
      if (error) {
        reject(error);
        return;
      }
      try {
        let id: string;
        if (platform === "darwin") {
          id = stdout
            .split("IOPlatformUUID")[1]
            .split("\n")[0]
            .replace(/\=|\s+|\"/gi, "")
            .toLowerCase();
        } else if (platform === "win32") {
          id = stdout
            .toString()
            .split("REG_SZ")[1]
            .replace(/\r+|\n+|\s+/gi, "")
            .toLowerCase();
        } else {
          id = stdout
            .toString()
            .replace(/\r+|\n+|\s+/gi, "")
            .toLowerCase();
        }
        resolve(original ? id : createHash("sha256").update(id).digest("hex"));
      } catch (parserError) {
        reject(parserError);
      }
    });
  });
}
