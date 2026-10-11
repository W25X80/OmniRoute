import fsPromises from "node:fs/promises";
import { execFile } from "node:child_process";
import { parseDatabaseBackend } from "../../lib/db/backendSelection";
import { execFileSync, execSync } from "child_process";
import { existsSync, readFileSync } from "fs";
import os from "os";

/**
 * Module-level cache: on Windows, execSync("hostname") spawns
 * cmd.exe which is expensive.  Cache the result after first call
 * since the machine ID never changes at runtime.
 */
let cachedRawId: string | null = null;

/**
 * Resets the cached machine ID.  Used primarily for testing so each
 * test case starts with a clean cache.
 */
export function resetMachineIdCache(): void {
  cachedRawId = null;
  delete rawIdGlobals[NATIVE_RAW_MACHINE_ID];
}

/**
 * Get raw machine ID using OS-specific methods.
 *
 * We use try/catch waterfall: try each OS method and fall through
 * to the next on failure. Platform checks are INSIDE try blocks so they
 * run at RUNTIME (not build time), avoiding Next.js SWC dead-code elimination.
 *
 * On Linux: skips Windows (REG.exe) and macOS (ioreg) strategies entirely.
 */
function getMachineIdRaw(): string {
  // Return cached result immediately (machine identity is stable at runtime)
  if (cachedRawId !== null) return cachedRawId;

  // Strategy 1: Windows — REG.exe query for MachineGuid
  try {
    if (process.platform !== "win32") {
      throw new Error("Not Windows");
    }
    const sysRoot = process.env.SystemRoot || process.env.windir || "C:\\Windows";
    const regPath = `${sysRoot}\\System32\\REG.exe`;
    if (existsSync(/* turbopackIgnore: true */ regPath)) {
      const output = execFileSync(
        regPath,
        ["QUERY", "HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography", "/v", "MachineGuid"],
        { encoding: "utf8", timeout: 5000 }
      );
      const id = output
        .split("REG_SZ")[1]
        ?.replace(/\r+|\n+|\s+/gi, "")
        ?.toLowerCase();
      if (id && id.length > 8) {
        cachedRawId = id;
        return id;
      }
    }
  } catch {
    // Not Windows or REG.exe failed — continue
  }

  // Strategy 2: macOS — ioreg IOPlatformUUID
  // Skip when DISABLE_IOREG_STRATEGY=1 so tests can reach Strategy 4/5 on darwin.
  try {
    if (process.platform !== "darwin" || process.env.DISABLE_IOREG_STRATEGY === "1") {
      throw new Error("Not macOS or ioreg disabled");
    }
    const output = execSync("ioreg -rd1 -c IOPlatformExpertDevice", {
      encoding: "utf8",
      timeout: 5000,
    });
    if (output.includes("IOPlatformUUID")) {
      const id = output
        .split("IOPlatformUUID")[1]
        ?.split("\n")[0]
        ?.replace(/=|\s+|"/gi, "")
        ?.toLowerCase();
      if (id && id.length > 8) {
        cachedRawId = id;
        return id;
      }
    }
  } catch {
    // Not macOS or ioreg not available — continue
  }

  // Strategy 3: Linux — read machine-id files directly (no `head` or pipe)
  try {
    for (const filePath of ["/etc/machine-id", "/var/lib/dbus/machine-id"]) {
      try {
        const content = readFileSync(/* turbopackIgnore: true */ filePath, "utf8")
          .trim()
          .toLowerCase();
        if (content.length > 8) {
          cachedRawId = content;
          return content;
        }
      } catch {
        // Try the next candidate file
      }
    }
  } catch {
    // Files not readable — continue
  }

  // Strategy 4: Node.js os.hostname() — no child process, works everywhere
  try {
    const hostname = os.hostname().toLowerCase();
    if (hostname) {
      cachedRawId = hostname;
      return hostname;
    }
  } catch {
    // os.hostname() not available — continue
  }

  // Strategy 5: execSync("hostname") shell fallback (for constrained environments)
  try {
    const hostname = execSync("hostname", { encoding: "utf8", timeout: 5000 });
    const id = hostname.trim().toLowerCase();
    if (id) {
      cachedRawId = id;
      return id;
    }
  } catch {
    // hostname failed — continue
  }

  cachedRawId = "unknown-machine";
  return "unknown-machine";
}

async function getRandomMachineFallbackId() {
  try {
    const cryptoFallback = await import("crypto");
    return cryptoFallback.randomUUID();
  } catch {
    if (typeof globalThis !== "undefined" && globalThis.crypto && globalThis.crypto.randomUUID) {
      return globalThis.crypto.randomUUID();
    }
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
      let r = 0;
      if (
        typeof globalThis !== "undefined" &&
        globalThis.crypto &&
        globalThis.crypto.getRandomValues
      ) {
        const arr = new Uint8Array(1);
        globalThis.crypto.getRandomValues(arr);
        r = arr[0] % 16;
      } else {
        r = (Date.now() % 16) | 0;
      }
      const v = c === "x" ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }
}

/**
 * Get consistent machine ID using native registry/OS query with salt
 * This ensures the same physical machine gets the same ID across runs
 *
 * @param {string} salt - Optional salt to use (defaults to environment variable)
 * @returns {Promise<string>} Machine ID (16-character base32)
 */
export async function getConsistentMachineId(salt: string | null = null): Promise<string> {
  const saltValue = salt || process.env.MACHINE_ID_SALT || "endpoint-proxy-salt";
  try {
    const rawMachineId =
      parseDatabaseBackend() === "postgres" ? await getMachineIdRawAsync() : getMachineIdRaw();
    // Create consistent ID using salt
    const crypto = await import("crypto");
    const hashedMachineId = crypto
      .createHash("sha256")
      .update(rawMachineId + saltValue)
      .digest("hex");
    // Return only first 16 characters for brevity
    return hashedMachineId.substring(0, 16);
  } catch (error) {
    console.log("Error getting machine ID:", error);
    // Fallback to random ID if node-machine-id fails
    return getRandomMachineFallbackId();
  }
}

/**
 * Get raw machine ID without hashing (for debugging purposes)
 * @returns {Promise<string>} Raw machine ID
 */
export async function getRawMachineId() {
  try {
    return parseDatabaseBackend() === "postgres" ? await getMachineIdRawAsync() : getMachineIdRaw();
  } catch (error) {
    console.log("Error getting raw machine ID:", error);
    // Fallback to random ID if node-machine-id fails
    return getRandomMachineFallbackId();
  }
}

/**
 * Check if we're running in browser or server environment
 * @returns {boolean} True if in browser, false if in server
 */
export function isBrowser() {
  return typeof window !== "undefined";
}

const NATIVE_RAW_MACHINE_ID = Symbol.for("omniroute.nativeRawMachineId");
interface NativeRawMachineIdState {
  value?: string;
  preparation?: Promise<string>;
}
const rawIdGlobals = globalThis as typeof globalThis & {
  [NATIVE_RAW_MACHINE_ID]?: NativeRawMachineIdState;
};

function executeMachineIdCommand(file: string, args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile(file, args, { encoding: "utf8", timeout: 5000 }, (error, stdout) => {
      if (error) reject(error);
      else resolve(stdout);
    });
  });
}

async function resolveMachineIdRawAsync(): Promise<string> {
  const platform = process.platform;
  const sysRoot = process.env.SystemRoot || process.env.windir || "C:\\Windows";
  const disableIoreg = process.env.DISABLE_IOREG_STRATEGY === "1";
  try {
    if (platform === "win32") {
      const regPath = `${sysRoot}\\System32\\REG.exe`;
      await fsPromises.access(regPath);
      const output = await executeMachineIdCommand(regPath, [
        "QUERY",
        "HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography",
        "/v",
        "MachineGuid",
      ]);
      const id = output
        .split("REG_SZ")[1]
        ?.replace(/\r+|\n+|\s+/gi, "")
        ?.toLowerCase();
      if (id && id.length > 8) return id;
    }
  } catch {
    // Same Windows failure waterfall as the synchronous source.
  }
  try {
    if (platform === "darwin" && !disableIoreg) {
      const output = await executeMachineIdCommand("ioreg", [
        "-rd1",
        "-c",
        "IOPlatformExpertDevice",
      ]);
      if (output.includes("IOPlatformUUID")) {
        const id = output
          .split("IOPlatformUUID")[1]
          ?.split("\n")[0]
          ?.replace(/=|\s+|"/gi, "")
          ?.toLowerCase();
        if (id && id.length > 8) return id;
      }
    }
  } catch {
    // Same macOS failure waterfall as the synchronous source.
  }
  for (const filePath of ["/etc/machine-id", "/var/lib/dbus/machine-id"]) {
    try {
      const content = (await fsPromises.readFile(filePath, "utf8")).trim().toLowerCase();
      if (content.length > 8) return content;
    } catch {
      // Keep exact source candidate order, including on an OS-query fallback.
    }
  }
  try {
    const hostname = os.hostname().toLowerCase();
    if (hostname) return hostname;
  } catch {
    // Same direct hostname fallback.
  }
  try {
    const hostname = (await executeMachineIdCommand("hostname", [])).trim().toLowerCase();
    if (hostname) return hostname;
  } catch {
    // Same final constant fallback.
  }
  return "unknown-machine";
}

function getMachineIdRawAsync(): Promise<string> {
  const state = (rawIdGlobals[NATIVE_RAW_MACHINE_ID] ??= {});
  if (state.value !== undefined) return Promise.resolve(state.value);
  state.preparation ??= resolveMachineIdRawAsync().then((value) => {
    if (rawIdGlobals[NATIVE_RAW_MACHINE_ID] === state) state.value = value;
    return value;
  });
  return state.preparation;
}
